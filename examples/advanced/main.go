package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"io/ioutil"

	"github.com/myntra/pipeline"
)

type downloadStep struct {
	pipeline.StepContext
	fileName string
	bytes    int64
	fail     bool
	ctx      context.Context
	cancel   context.CancelFunc
}

func newDownloadStep(fileName string, bytes int64, fail bool) *downloadStep {
	ctx, cancel := context.WithCancel(context.Background())
	d := &downloadStep{fileName: fileName, bytes: bytes, fail: fail}
	d.ctx = ctx
	d.cancel = cancel
	return d
}

func (d *downloadStep) Exec(request *pipeline.Request) *pipeline.Result {

	d.Status([]byte(fmt.Sprintf("%+v", request)))

	d.Status([]byte(fmt.Sprintf("Started downloading file %s", d.fileName)))

	client := &http.Client{}

	req, err := http.NewRequest("GET", fmt.Sprintf("http://httpbin.org/bytes/%d", d.bytes), nil)
	if err != nil {
		return &pipeline.Result{Error: err}
	}

	req = req.WithContext(d.ctx)

	resp, err := client.Do(req)
	if err != nil {
		return &pipeline.Result{Error: err}
	}

	defer resp.Body.Close()

	n, err := io.Copy(ioutil.Discard, resp.Body)
	if err != nil {
		return &pipeline.Result{Error: err}
	}

	if d.fail {
		return &pipeline.Result{Error: fmt.Errorf("File download failed %s", d.fileName)}
	}

	d.Status([]byte(fmt.Sprintf("Successfully downloaded file %s", d.fileName)))

	return &pipeline.Result{
		Error:  nil,
		Data:   struct{ bytesDownloaded int64 }{bytesDownloaded: n},
		KeyVal: map[string]interface{}{"bytesDownloaded": n},
	}
}

func (d *downloadStep) Cancel() {
	d.Status([]byte(fmt.Sprintf("Cancel downloading file %s", d.fileName)))
	d.cancel()
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

	workflow := pipeline.New("getfiles",
		pipeline.OutBufferSize(1000),
		pipeline.ExpectedDuration(7*time.Second))
	//stages
	stage := pipeline.NewStage("stage", pipeline.Concurrent(true))
	// in this stage, steps will be executed concurrently
	concurrentStage := pipeline.NewStage("con_stage", pipeline.Concurrent(true))
	// another concurrent stage
	concurrentErrStage := pipeline.NewStage("con_err_stage", pipeline.Concurrent(true))

	//steps
	fileStep1mb := newDownloadStep("1mbfile", 1e6, false)
	fileStep1mbFail := newDownloadStep("1mbfileFail", 1e6, true)
	fileStep5mb := newDownloadStep("5mbfile", 5e6, false)
	fileStep10mb := newDownloadStep("10mbfile", 10e6, false)

	//stage with sequential steps
	stage.AddStep(fileStep1mb, fileStep5mb, fileStep10mb)

	//stage with concurrent steps
	concurrentStage.AddStep(fileStep1mb, fileStep5mb, fileStep10mb)

	//stage with concurrent steps one of which fails early, prompting a cancellation
	//of the other running steps.
	concurrentErrStage.AddStep(fileStep1mbFail, fileStep5mb, fileStep10mb)

	// add all stages
	workflow.AddStage(stage, concurrentStage, concurrentErrStage)

	// start a routine to read out and progress
	go readPipeline(workflow)

	// execute pipeline
	result := workflow.Run(nil)
	if result.Error != nil {
		fmt.Println(result.Error)
	}

	// one would persist the time taken duration to use as progress scale for the next workflow build
	fmt.Println("timeTaken:", workflow.GetDuration())

}
