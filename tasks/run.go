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
	var prev, next <-chan core.RowMap

	cfg := core.GetConfig()
	scfg := cfg.GetSourceConfig(taskName)
	tcfg := cfg.GetTargetConfig(taskName)

	log.Printf("> Starting source of type \"%s\" on task \"%s\"...",
		scfg.Type, taskName)
	source := sources.BuildSource(taskName, scfg)
	prev = source.Run()

	next = prev
	for _, acfg := range cfg.GetAdaptersConfig(taskName) {
		log.Printf("> Starting adapter of type \"%s\" on task \"%s\"...",
			acfg.Type, taskName)
		adapter := adapters.BuildAdapter(taskName, acfg)
		next = adapter.Run(prev)
	}

	var wg sync.WaitGroup
	log.Printf("> Starting target of type \"%s\" on task \"%s\"...",
		tcfg.Type, taskName)
	wg.Add(1)
	target := targets.BuildTarget(&wg, taskName, tcfg)
	target.Run(next)
	wg.Wait()

	log.Printf("Task \"%s\" finished!", taskName)
}
