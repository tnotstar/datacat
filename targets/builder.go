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

package targets

import (
	"log"
	"sync"

	"github.com/tnotstar/sqltoapi/core"
)

// `BuildTarget` creates a new instance of the target endpoint specified
// by the configuration object passed as argument.
//
// The `wg` is a pointer to the wait group to be used to sync all targets.
// The `task` is the name of the task is being executed.
// The `tgcfg` is the configuration for the object to be created.
func BuildTarget(wg *sync.WaitGroup, task string, tgcfg core.TargetConfig) core.Target {
	switch tgcfg.Type {
	case "jsonl-file":
		return NewJSONLinesTarget(wg, task, tgcfg.Output)
	default:
		log.Fatal("Invalid target type: ", tgcfg.Type)
	}

	return nil
}
