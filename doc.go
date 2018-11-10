/*
Package pipeline is used to build multi-staged concurrent workflows with a centralized logging output:
	Pipeline
		|
		| Stages
			|
			| Steps

The package has three building blocks to create workflows : Pipeline, Stage and Step . A pipeline is a collection of stages and a stage is a
collection of steps. A stage can have either concurrent or sequential steps, while stages are always sequential.
Example Usage:

    package main

    import (
        "github.com/myntra/pipeline"
        "fmt"
        "time"
    )

    type work struct {
        pipeline.StepContext
        id int
    }

    func (w work) Exec(request *pipeline.Request) *pipeline.Result {
        w.Status("work")
        time.Sleep(time.Millisecond * 2000)
        return &pipeline.Result{}
    }

    func (w work) Cancel() error {
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

        workpipe := pipeline.NewProgress("myProgressworkpipe", 1000, time.Second*2)
        stage := pipeline.NewStage("mypworkstage", false, false)
        stage.AddStep(&work{id: 1})
        workpipe.AddStage(stage)
        go readPipeline(workpipe)
        workpipe.Run()
    }

For a detailed guide check Readme.md
*/
package pipeline
