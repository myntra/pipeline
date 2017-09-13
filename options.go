package pipeline

import "time"

// DefaultOutDrainTimeout time to wait for all readers to finish consuming output
const DefaultOutDrainTimeout = time.Second * 5

// DefaultOutBufferSize size of the pipeline.Out channel
const DefaultOutBufferSize = 1000

type pipelineConfig struct {
	outBufferSize    int
	outDrainTimeout  time.Duration
	expectedDuration time.Duration
}

// Option represents an option for the pipeline. It must be used as an arg
// to New(...) or Clone(...)
type Option func(*pipelineConfig)

// OutBufferSize is size of the pipeline.Out channel
func OutBufferSize(size int) Option {
	return Option(func(c *pipelineConfig) {
		c.outBufferSize = size
	})
}

// OutDrainTimeout is the time to wait for all readers to finish consuming output
func OutDrainTimeout(timeout time.Duration) Option {
	return Option(func(c *pipelineConfig) {
		c.outDrainTimeout = timeout
	})
}

// ExpectedDuration is the expected time for the pipeline to finish
func ExpectedDuration(timeout time.Duration) Option {
	return Option(func(c *pipelineConfig) {
		c.expectedDuration = timeout
	})
}

type stageConfig struct {
	concurrent        bool
	disableStrictMode bool
}

// StageOption represents an option for a stage. It must be used as an arg
// to NewStage(...)
type StageOption func(*stageConfig)

// Concurrent enables concurrent execution of steps
func Concurrent(enable bool) StageOption {
	return StageOption(func(c *stageConfig) {
		c.concurrent = enable
	})
}

// DisableStrictMode disables cancellation of concurrently executing steps if
// there is an error
func DisableStrictMode(disable bool) StageOption {
	return StageOption(func(c *stageConfig) {
		c.disableStrictMode = disable
	})
}
