package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"

	uuid "github.com/satori/go.uuid"
)

// Pipeline is a sequence of stages
type Pipeline struct {
	Name             string   `json:"name"`
	Stages           []*Stage `json:"stages"`
	uuid             string
	config           *pipelineConfig
	expectedDuration time.Duration
	duration         time.Duration
	outsubscribed    bool
	outbufferlen     int
	tick             time.Duration
	cancelDrain      context.CancelFunc
	cancelProgress   context.CancelFunc
}

// New returns a new pipeline
// 	name of the pipeline
// 	outBufferLen is the size of the output buffered channel
func New(name string, opts ...Option) *Pipeline {

	config := &pipelineConfig{
		outBufferSize:   DefaultOutBufferSize,
		outDrainTimeout: DefaultOutDrainTimeout,
	}

	uuid := uuid.NewV1().String()

	if name == "" {
		name = uuid
	} else {
		name = spaceMap(name)
	}

	for _, o := range opts {
		o(config)
	}

	if buffersMap == nil {
		buffersMap = &buffers{bufferMap: make(map[string]*buffer)}
	}

	p := &Pipeline{
		Name:   name,
		config: config,
		uuid:   uuid,
	}

	buf := buffer{in: make(chan []byte, p.config.outBufferSize), out: []chan []byte{}, progress: []chan int64{}}
	buffersMap.set(p.uuid, &buf)

	return p

}

// Clone pipeline
func (p *Pipeline) Clone(name string, opts ...Option) *Pipeline {

	config := &pipelineConfig{
		outBufferSize:   p.config.outBufferSize,
		outDrainTimeout: p.config.outDrainTimeout,
	}

	uuid := uuid.NewV1().String()

	if name == "" {
		name = uuid
	} else {
		name = spaceMap(name)
	}

	for _, o := range opts {
		o(config)
	}

	if buffersMap == nil {
		buffersMap = &buffers{bufferMap: make(map[string]*buffer)}
	}

	cp := &Pipeline{
		Name:   name,
		config: config,
		Stages: p.Stages,
		uuid:   uuid,
	}

	buf := buffer{in: make(chan []byte, p.config.outBufferSize), out: []chan []byte{}, progress: []chan int64{}}
	buffersMap.set(p.uuid, &buf)

	return cp
}

// AddStage adds a new stage to the pipeline
func (p *Pipeline) AddStage(stage ...*Stage) {
	for i := range stage {
		for j := range stage[i].Steps {
			ctx := &stepContextVal{
				name:        p.Name + "." + stage[i].Name + "." + reflect.TypeOf(stage[i].Steps[j]).String(),
				pipelineKey: p.uuid,
				concurrent:  stage[i].config.concurrent,
				index:       j,
			}

			stage[i].Steps[j].setCtx(ctx)
		}
		stage[i].pipelineKey = p.uuid
	}

	p.Stages = append(p.Stages, stage...)
}

// Run the pipeline. The stages are executed in sequence while steps may be concurrent or sequential.
func (p *Pipeline) Run(data interface{}) *Result {

	if len(p.Stages) == 0 {
		return &Result{Error: fmt.Errorf("No stages to be executed")}
	}

	var ticker *time.Ticker
	if p.expectedDuration != 0 && p.tick != 0 {
		// start progress update ticker
		ticker = time.NewTicker(p.tick)
		ctx, cancelProgress := context.WithCancel(context.Background())
		p.cancelProgress = cancelProgress
		go p.updateProgress(ctx, ticker)
	}

	buf, ok := buffersMap.get(p.Name)
	if !ok {
		return &Result{Error: fmt.Errorf("error creating output %s", p.Name)}
	}

	ctx, cancelDrain := context.WithCancel(context.Background())
	p.cancelDrain = cancelDrain
	go buf.drainBuffer(ctx)

	defer buffersMap.remove(p.Name)
	defer p.waitForDrain()
	if p.expectedDuration != 0 && p.tick != 0 {
		defer ticker.Stop()
	}

	request := &Request{Data: data}
	result := &Result{}
	for i, stage := range p.Stages {
		stage.index = i
		result = stage.run(request)
		if result.Error != nil {

			return result
		}
		request.Data = result.Data
	}

	return result
}

// Out collects the status output from the stages and steps
func (p *Pipeline) Out() (<-chan []byte, error) {
	// add a new listener
	out := make(chan []byte, p.outbufferlen)
	err := buffersMap.appendOutBuffer(p.Name, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetDuration returns the current time spent by the pipleline
func (p *Pipeline) GetDuration() time.Duration {
	return p.duration
}

// GetProgressPercent of the pipeline
func (p *Pipeline) GetProgressPercent() (<-chan int64, error) {
	pg := make(chan int64, 1)
	err := buffersMap.appendProgressBuffer(p.Name, pg)
	if err != nil {
		return nil, err
	}
	return pg, nil
}

// started as a goroutine
func (p *Pipeline) updateProgress(ctx context.Context, ticker *time.Ticker) {
	start := time.Now()
	for range ticker.C {
		p.duration = time.Since(start)
		percentDone := int64((p.duration.Seconds() / p.expectedDuration.Seconds()) * 100)
		// if estimate is incorrect don't overflow progress end
		if percentDone > 100 {
			percentDone = 99
		}
		buf, ok := buffersMap.get(p.Name)
		if !ok {
			return
		}
	loop:
		for _, pg := range buf.progress {
			select {
			case <-ctx.Done():
				break loop
			case pg <- percentDone:
			default:
				<-pg
				pg <- percentDone
			}
		}

	}
}

func (p *Pipeline) waitForDrain() {
	buf, ok := buffersMap.get(p.Name)
	if !ok {
		return
	}

	var empty = func() chan bool {
		emptyChan := make(chan bool)
		go func() {
			if len(buf.out) == 0 {
				return
			}

			pending := 0
			for _, o := range buf.out {
				pending += len(o)
			}

			if pending == 0 && len(buf.in) == 0 {
				emptyChan <- true
				return
			}

			emptyChan <- false
		}()
		return emptyChan
	}

loop:
	for {
		select {
		case empty := <-empty():
			if empty {
				break loop
			}
		case <-time.After(p.config.outDrainTimeout):
			break loop
		}
	}

	p.cancelDrain()
	if p.cancelProgress != nil {
		p.cancelProgress()
	}
}

// status writes a line to the out channel
func (p *Pipeline) status(data []byte) {
	send(p.Name, data)
}

func spaceMap(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}
