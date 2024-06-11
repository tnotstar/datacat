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
	"time"

	"github.com/tnotstar/datacat/adapters"
	"github.com/tnotstar/datacat/core"
	"github.com/tnotstar/datacat/sources"
	"github.com/tnotstar/datacat/targets"
)

// ExecuteFetch executes the fetch task with given name.
//
// The `taskName` is the name of the task to be executed.
func RunTask(taskName string) {
	log.Printf("Running taskz '%s'...", taskName)
	log.Print("RunTask: ************** HOOOOOLLLLLLLAAAAAA*********", taskName)
	start := time.Now()
	cfg := core.GetConfig()
	var wg sync.WaitGroup
	var pipe <-chan core.RowMap

	log.Printf("> Starting source for task '%s'...", taskName)
	source := sources.BuildSource(0, cfg, taskName)
	pipe = source.Run(&wg)

	for _, adapterName := range cfg.GetAdapterNames(taskName) {
		log.Printf("> Starting adapter '%s' for task '%s'...", adapterName, taskName)
		adapter := adapters.BuildAdapter(0, cfg, taskName, adapterName)
		pipe = adapter.Run(&wg, pipe)
	}

	for i := 0; i < 2; i++ {
		log.Printf("> Starting instance #%d of target for task '%s'...", i, taskName)
		target := targets.BuildTarget(i, cfg, taskName)
		target.Run(&wg, pipe)
	}

	wg.Wait()
	elapsed := time.Since(start)
	log.Printf("Task '%s' finished! (%s elapsed)", taskName, elapsed)
}
