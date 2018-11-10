package main

import (
	"fmt"
	"time"

	"github.com/myntra/pipeline"
)

type work struct {
	pipeline.StepContext
	id int
}

func (w *work) Exec(request *pipeline.Request) *pipeline.Result {

	w.Status(fmt.Sprintf("%+v", request))
	duration := time.Duration(1000 * w.id)
	time.Sleep(time.Millisecond * duration)
	msg := fmt.Sprintf("work %d", w.id)
	return &pipeline.Result{
		Error:  nil,
		Data:   map[string]string{"msg": msg},
		KeyVal: map[string]interface{}{"msg": msg},
	}
}

func (w *work) Cancel() error {
	w.Status("cancel step")
	return nil
}

func readPipeline(pipe *pipeline.Pipeline) {
	out, err := pipe.Out()
	if err != nil {
		return
	}

	progress, err := pipe.GetProgressPercent()
	if err != nil {
		return
	}

	for {
		select {
		case line := <-out:
			fmt.Println(line)
		case p := <-progress:
			fmt.Println("percent done: ", p)
		}
	}
}

func main() {

	workpipe := pipeline.NewProgress("myProgressworkpipe", 1000, time.Second*3)
	stage := pipeline.NewStage("mypworkstage", false, false)
	step1 := &work{id: 1}
	step2 := &work{id: 2}

	stage.AddStep(step1)
	stage.AddStep(step2)

	workpipe.AddStage(stage)

	go readPipeline(workpipe)

	result := workpipe.Run()
	if result.Error != nil {
		fmt.Println(result.Error)
	}

	fmt.Println("timeTaken:", workpipe.GetDuration())
}
