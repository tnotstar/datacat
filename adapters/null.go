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

package adapters

import (
	"fmt"
	"log"
	"sync"

	"github.com/tnotstar/datacat/core"
)

// `NullHandlingAdapter` an adapter to handle null values.
type NullHandlingAdapter struct {
	// The `id` of the adapter.
	id int
	// The `task` of the task which is running into.
	task string
	// The name of the `adapter`.
	adapter string
	// The `handling` the way to handle null values.
	handling string
}

// `IsaNullHandlingAdapter` returns true if given adapter type
// is NullHandlingAdapter.
func IsaNullHandlingAdapter(adapterType string) bool {
	return adapterType == "null-handling-adapter"
}

// `NewNullHandlingAdapter` creates a new instance of the NullHandling adapter.
//
// The `id` is the instance of the adapter to be created.
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
// The `adapterName` is the name of the adapter to be created.
func NewNullHandlingAdapter(id int, cfg core.Configurator, taskName string, adapterName string) core.Adapter {
	adapterConfig, _ := cfg.GetAdapterConfig(taskName, adapterName)

	handling := fmt.Sprint(adapterConfig.Arguments["handling"])

	return &NullHandlingAdapter{
		id:       id,
		task:     taskName,
		adapter:  adapterName,
		handling: handling,
	}
}

// Returns the output channel of the null handled rows.
//
// The `wg` is the wait group for the goroutine.
// The `in` is the input channel of the rows to be casted.
func (adp *NullHandlingAdapter) Run(wg *sync.WaitGroup, in <-chan core.RowMap) <-chan core.RowMap {
	log.Printf("* Creating #%d instance of null handling adapter for task %s...", adp.id, adp.task)
	out := make(chan core.RowMap)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for row := range in {
			for field, value := range row {
				if value == nil {
					switch adp.handling {
					case "remove":
						delete(row, field)
					default:
						log.Fatalf("Invalid null handling type: %s", adp.handling)
					}
				}
			}
			out <- row
		}

		close(out)
	}()

	return out
}
