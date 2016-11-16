package pipeline

import (
	"context"
	"fmt"
	"sync"
)

type buffer struct {
	// single writer
	in chan string
	// multiple readers
	out      []chan string
	progress []chan int64
}

func (b *buffer) drainBuffer(ctx context.Context) {
loop:
	for l := range b.in {
		// send the line to each of the listeners
		for _, o := range b.out {
			select {
			case <-ctx.Done():
				break loop
			case o <- l:
			default:
				//throw the oldest line out
				<-o
				o <- l
			}
		}
	}
}

type buffers struct {
	bufferMap map[string]*buffer
	sync.RWMutex
}

// Get channel
func (bfs *buffers) get(key string) (*buffer, bool) {
	bfs.Lock()
	defer bfs.Unlock()
	val, ok := bfs.bufferMap[key]
	return val, ok
}

// set buffer
func (bfs *buffers) set(key string, value *buffer) {
	bfs.Lock()
	defer bfs.Unlock()
	bfs.bufferMap[key] = value
}

// remove buffer
func (bfs *buffers) remove(key string) {
	bfs.Lock()
	defer bfs.Unlock()
	delete(bfs.bufferMap, key)
}

func (bfs *buffers) appendOutBuffer(key string, o chan string) error {
	bfs.Lock()
	defer bfs.Unlock()
	val, ok := bfs.bufferMap[key]
	if !ok {
		return fmt.Errorf("buffer not found")
	}
	val.out = append(val.out, o)
	return nil
}

// append progress buffer
func (bfs *buffers) appendProgressBuffer(key string, p chan int64) error {
	bfs.Lock()
	defer bfs.Unlock()
	val, ok := bfs.bufferMap[key]
	if !ok {
		return fmt.Errorf("buffer not found")
	}
	val.progress = append(val.progress, p)
	return nil
}

// returns all buffer
func (bfs *buffers) all() map[string]*buffer {
	bfs.Lock()
	defer bfs.Unlock()

	copydata := make(map[string]*buffer)
	for key, value := range bfs.bufferMap {
		copydata[key] = value
	}

	return copydata
}

var buffersMap *buffers

func send(pipelineKey string, line string) {
	// line = fmt.Sprintf(time.Now().Format("2006-01-02 15:04:05")) + " " + line
	buf, ok := buffersMap.get(pipelineKey)
	if !ok {
		return
	}
	buf.in <- line

}
