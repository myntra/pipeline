package pipeline

import (
	"context"
	"fmt"

	"github.com/fatih/color"
)

// Stage is a collection of steps executed concurrently or sequentially
//    concurrent: run the steps concurrently
//
//    disableStrictMode: In strict mode if a single step fails, all the other concurrent steps are cancelled.
//    Step.Cancel will be invoked for cancellation of the step. Set disableStrictMode to true to disable strict mode
type Stage struct {
	Name              string `json:"name"`
	Steps             []Step `json:"steps"`
	Concurrent        bool   `json:"concurrent"`
	DisableStrictMode bool   `json:"disableStrictMode"`
	index             int
	pipelineKey       string
}

// NewStage returns a new stage
// 	name of the stage
// 	concurrent flag sets whether the steps will be executed concurrently
func NewStage(name string, concurrent bool, disableStrictMode bool) *Stage {
	st := &Stage{Name: name, Concurrent: concurrent}
	return st
}

// AddStep adds a new step to the stage
func (st *Stage) AddStep(step ...Step) {
	st.Steps = append(st.Steps, step...)
}

// Run the stage execution sequentially
func (st *Stage) run(request *Request) *Result {
	if len(st.Steps) == 0 {
		return &Result{Error: fmt.Errorf("No steps to be executed")}
	}
	st.status("begin")
	defer st.status("end")

	if st.Concurrent {
		st.status("is concurrent")
		g, ctx := withContext(context.Background())
		for _, step := range st.Steps {
			step := step
			step.Status("begin")
			g.run(func() *Result {

				defer step.Status("end")
				//disables strict mode. g.run will wait for all steps to finish
				if st.DisableStrictMode {
					return step.Exec(request)
				}

				resultChan := make(chan *Result, 1)

				go func() {
					result := step.Exec(request)
					if result == nil {
						result = &Result{}
					}
					resultChan <- result
				}()

				select {
				case <-ctx.Done():

					if err := step.Cancel(); err != nil {
						st.status("Error Cancelling Step " + step.getCtx().name)
					}

					<-resultChan
					return &Result{Error: ctx.Err()}

				case result := <-resultChan:
					if result == nil {
						result = &Result{}
					}
					return result
				}

			})
		}

		if result := g.wait(); result != nil && result.Error != nil {
			st.status(" >>>failed !!! ")
			return result
		}

	} else {
		st.status("is not concurrent")
		for _, step := range st.Steps {
			step.Status("begin")
			res := step.Exec(request)
			if res != nil && res.Error != nil {
				step.Status(">>>failed !!!")
				return res
			}

			if res == nil {
				step.Status("end")
				continue
			}

			request.Data = res.Data
			request.KeyVal = res.KeyVal
			step.Status("end")
		}
	}

	return &Result{}
}

// status writes a line to the out channel
func (st *Stage) status(line string) {
	stageText := fmt.Sprintf("[stage-%d]", st.index)
	yellow := color.New(color.FgYellow).SprintFunc()
	line = yellow(stageText) + "[" + st.Name + "]: " + line
	send(st.pipelineKey, line)
}
