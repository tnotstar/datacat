// Copyright 2023, Antonio Alvarado Hernández <tnotstar@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tasks

import (
	"log"
	"sync"

	"github.com/tnotstar/sqltoapi/adapters"
	"github.com/tnotstar/sqltoapi/core"
	"github.com/tnotstar/sqltoapi/sources"
	"github.com/tnotstar/sqltoapi/targets"
)

// ExecuteFetch executes the fetch task with given name.
//
// The `taskName` is the name of the task to be executed.
func RunTask(taskName string) {
	log.Printf("Running task %s...", taskName)
	var wg sync.WaitGroup
	var prev, next <-chan core.RowMap

	cfg := core.GetConfig()

	log.Printf("Starting source for task %s...", taskName)
	source := sources.BuildSource(cfg, taskName)
	prev = source.Run(&wg)

	next = prev
	for idx := range cfg.GetAdaptersConfig(taskName) {
		log.Printf("Starting adapter #%d for task %s...", idx, taskName)
		adapter := adapters.BuildAdapter(cfg, taskName, idx)
		next = adapter.Run(&wg, prev)
	}

	log.Printf("Starting target for task %s...", taskName)
	target := targets.BuildTarget(cfg, taskName)
	target.Run(&wg, next)

	wg.Wait()
	log.Printf("Task \"%s\" finished!", taskName)
}
