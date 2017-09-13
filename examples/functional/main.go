package main

import (
	"context"
	"fmt"

	"github.com/myntra/pipeline"
)

// TransformStep ...
func TransformStep(context context.Context, request *pipeline.Request) *pipeline.Result {
	request.Status("Starting transformstep")
	fmt.Println(request.Data)
	// handle cancel step: https://blog.golang.org/context
	//<-context.Done() is unblocked when step is cancelled on error returned by sibling concurrent step
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

	logParserPipe := pipeline.New("")
	stageOne := pipeline.NewStage("")

	transfromStep := pipeline.NewStep(TransformStep)

	stageOne.AddStep(transfromStep)
	logParserPipe.AddStage(stageOne)

	go readPipeline(logParserPipe)

	data := map[string]string{
		"a": "b",
	}

	result := logParserPipe.Run(data)
	if result.Error != nil {
		fmt.Println(result.Error)
	}

	fmt.Println("timeTaken:", logParserPipe.GetDuration())

	clonedlogParserPipe := logParserPipe.Clone("")

	go readPipeline(clonedlogParserPipe)

	data2 := map[string]string{
		"c": "d",
	}

	result = clonedlogParserPipe.Run(data2)
	if result.Error != nil {
		fmt.Println(result.Error)
	}

	fmt.Println("timeTaken:", clonedlogParserPipe.GetDuration())

}
