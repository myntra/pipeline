package pipeline

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"
)

var tests = []pipeConfig{
	{"TestNoStage", 70, []sg{}},
	{"SingleStageEmptyStep", 100, []sg{{false, 0, 0}}},
	{"SingleStageSingleStep", 100, []sg{{false, 1, 0}}},
	{"TestSingleStageMultiStep", 100, []sg{{false, 2, 0}}},
	{"TestMultiStageMultiStep", 100, []sg{{false, 2, 0}, {false, 2, 0}}},
	{"TestMultiStageMultiStepConcurrent", 100, []sg{{true, 2, 0}, {true, 2, 0}}},
	{"TestMultiStageMultiStepConcurrentMixed", 100, []sg{{true, 2, 0}, {false, 2, 0}}},
	{"SingleStageSingleStepError", 100, []sg{{false, 0, 1}}},
	{"TestSingleStageMultiStepError", 100, []sg{{false, 0, 2}}},
	{"TestMultiStageMultiStepError", 100, []sg{{false, 0, 2}, {false, 0, 2}}},
	{"TestMultiStageMultiStepConcurrentError", 100, []sg{{true, 0, 2}, {true, 0, 2}}},
	{"TestMultiStageMultiStepConcurrentErrorMixed", -10, []sg{{true, 0, 2}, {false, 0, 2}}},
	{"TestMultiStageMultiStepConcurrentErrorMixed2", 100, []sg{{true, 1, 2}, {false, 0, 2}}},
	{"TestMultiStageMultiStepConcurrentErrorMixed3", -1, []sg{{true, 0, 2}, {true, 1, 2}}},
}

func TestPipeline(t *testing.T) {
	for _, test := range tests {

		testpipe, errorPipe := createPipeline(test)
		go readPipeline(testpipe)
		if errorPipe {
			result := testpipe.Run()
			if result.Error == nil {
				log.Fatalf("Test failed: %v", test.pipelineName)
			}

		} else {
			result := testpipe.Run()
			if result.Error != nil {
				log.Fatalf("Test failed: %v", test.pipelineName)
			}
		}
	}

}

func BenchmarkPipeline(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := rand.Intn(len(tests))
			testpipe, errorPipe := createPipeline(tests[i])
			if errorPipe {
				err := testpipe.Run()
				if err == nil {
					log.Fatalf("Test failed: %v", testpipe.Name)
				}

			} else {
				err := testpipe.Run()
				if err != nil {
					log.Fatalf("Test failed: %v", testpipe.Name)
				}
			}
		}
	})

}

type TestStep struct {
	StepContext
	key string
}

func (t TestStep) Exec(request *Request) *Result {
	t.Status("start step")
	t.Status("executing test ")
	time.Sleep(time.Millisecond * 200)
	t.Status("end step")
	return nil
}

func (t TestStep) Cancel() error {
	t.Status("cancel step")
	return nil
}

type TestStep2 struct {
	StepContext
	key string
}

func (t TestStep2) Exec(request *Request) *Result {
	t.Status("start step")
	t.Status("executing test 2")
	time.Sleep(time.Millisecond * 200)
	t.Status("end step")
	return nil
}

func (t TestStep2) Cancel() error {
	t.Status("cancel step")
	return nil
}

type TestStepErr struct {
	StepContext
	key string
}

func (t TestStepErr) Exec(request *Request) *Result {

	t.Status("start step")
	t.Status("executing test err")
	time.Sleep(time.Millisecond * 500)
	t.Status("end step")
	return &Result{Error: errors.New("test error 1")}
}

func (t TestStepErr) Cancel() error {
	t.Status("cancel step")
	return nil
}

type TestStepErr2 struct {
	StepContext
	key string
}

func (t TestStepErr2) Exec(request *Request) *Result {
	t.Status("start step")
	t.Status("executing test err 2")
	time.Sleep(time.Millisecond * 200)
	t.Status("end step")
	return &Result{Error: errors.New("test error 2")}
}

func (t TestStepErr2) Cancel() error {
	t.Status("cancel step")
	return nil
}

type sg struct {
	concurrent bool
	steps      int
	errorSteps int
}

type pipeConfig struct {
	pipelineName         string
	pipeLineOutBufferLen int
	sgArr                []sg
}

func createPipeline(testPipeConfig pipeConfig) (*Pipeline, bool) {
	testpipe := New(testPipeConfig.pipelineName, testPipeConfig.pipeLineOutBufferLen)
	errorPipeline := false

	var testStages []*Stage

	for i, sg := range testPipeConfig.sgArr {
		// create stage
		stage := NewStage(fmt.Sprintf("testStage%d", i), sg.concurrent, false)
		// create normal steps

		for j := 0; j < sg.steps; j++ {
			if j == 0 {
				stage.AddStep(&TestStep{key: testpipe.Name})
			} else if j == 1 {
				stage.AddStep(&TestStep2{key: testpipe.Name})
			} else {
				stage.AddStep(&TestStep2{key: testpipe.Name})
			}
		}

		// create error steps

		for k := 0; k < sg.errorSteps; k++ {
			if k == 0 {
				stage.AddStep(&TestStepErr{key: testpipe.Name})
			} else if k == 1 {
				stage.AddStep(&TestStepErr2{key: testpipe.Name})
			} else {
				stage.AddStep(&TestStepErr2{key: testpipe.Name})
			}
			errorPipeline = true
		}

		testStages = append(testStages, stage)

		totalSteps := sg.errorSteps + sg.steps

		if totalSteps == 0 {
			errorPipeline = true
		}

	}

	if len(testStages) == 0 {
		errorPipeline = true
	}

	testpipe.AddStage(testStages...)

	return testpipe, errorPipeline
}

func readPipeline(testpipe *Pipeline) {

	out, err := testpipe.Out()
	if err != nil {
		return
	}

loop:
	for {
		select {
		case line := <-out:
			fmt.Println(line)
		case <-time.After(time.Second * 10):
			break loop
		}
	}
}
