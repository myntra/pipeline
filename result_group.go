package pipeline

// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package errgroup provides synchronization, error propagation, and Context
// cancelation for groups of goroutines working on subtasks of a common task.

// Modified the errgroup package to return type Result
import (
	"sync"

	"context"
)

// A Group is a collection of goroutines working on subtasks that are part of
// the same overall task.
//
// A zero Group is valid and does not cancel on error.
type group struct {
	cancel func()

	wg sync.WaitGroup

	errOnce sync.Once
	results []*Result
}

// WithContext returns a new Group and an associated Context derived from ctx.
//
// The derived Context is canceled the first time a function passed to Go
// returns a non-nil error or the first time Wait returns, whichever occurs
// first.
func withContext(ctx context.Context) (*group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &group{cancel: cancel}, ctx
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the first non-nil error (if any) from them.
func (g *group) wait() []*Result {
	g.wg.Wait()
	if g.cancel != nil {
		g.cancel()
	}
	return g.results
}

// Go calls the given function in a new goroutine.
//
// The first call to return a non-nil error cancels the group; its error will be
// returned by Wait.
func (g *group) run(f func() *Result) {
	g.wg.Add(1)

	go func() {
		defer g.wg.Done()

		if result := f(); result.Error != nil {
			g.errOnce.Do(func() {
				g.results = append(g.results, result)
				if g.cancel != nil {
					g.cancel()
				}
			})
		}
	}()
}
