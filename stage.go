package pipeline

import (
	"context"
	"fmt"
)

// Stage is a collection of steps executed concurrently or sequentially
//    concurrent: run the steps concurrently
//
//    disableStrictMode: In strict mode if a single step fails, all the other concurrent steps are cancelled.
//    Step.Cancel will be invoked for cancellation of the step. Set disableStrictMode to true to disable strict mode
type Stage struct {
	Name        string `json:"name"`
	Steps       []Step `json:"steps"`
	config      *stageConfig
	index       int
	pipelineKey string
}

// NewStage returns a new stage
// 	name of the stage
// 	concurrent flag sets whether the steps will be executed concurrently
func NewStage(name string, opts ...StageOption) *Stage {

	config := &stageConfig{}

	for _, o := range opts {
		o(config)
	}

	st := &Stage{Name: name, config: config}
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
	if st.config.concurrent {
		g, ctx := withContext(context.Background())
		for _, step := range st.Steps {
			step := step
			step.Status([]byte("begin"))
			g.run(func() *Result {

				defer step.Status([]byte("end"))
				//disables strict mode. g.run will wait for all steps to finish
				if st.config.disableStrictMode {
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

					step.Cancel()

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
			return result
		}

	} else {
		res := &Result{}
		for _, step := range st.Steps {
			res = step.Exec(request)
			if res != nil && res.Error != nil {
				return res
			}

			if res == nil {
				res = &Result{}
				continue
			}

			request.Data = res.Data
			request.KeyVal = res.KeyVal
		}
		return res
	}

	return &Result{}
}
