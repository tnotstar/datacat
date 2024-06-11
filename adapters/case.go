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
	"strings"
	"sync"
	"unicode"

	"github.com/tnotstar/datacat/core"
)

// `CaseConversionAdapter` an adapter to handle null values.
type CaseConversionAdapter struct {
	// The `id` of the adapter.
	id int
	// The `task` of the task which is running into.
	task string
	// The name of the `adapter`.
	adapter string
	// The `fields` to be casted.
	fields []string
	// The `handling` the way to handle null values.
	handling string
}

// `IsaCaseConversionAdapter` returns true if given adapter type
// is CaseConversionAdapter.
func IsaCaseConversionAdapter(adapterType string) bool {
	return adapterType == "case-conversion-adapter"
}

// `NewCaseConversionAdapter` creates a new instance of the CaseConversion adapter.
//
// The `id` is the instance of the adapter to be created.
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
// The `adapterName` is the name of the adapter to be created.
func NewCaseConversionAdapter(id int, cfg core.Configurator, taskName string, adapterName string) core.Adapter {
	adapterConfig, _ := cfg.GetAdapterConfig(taskName, adapterName)

	raws := adapterConfig.Arguments["fields"].([]any)
	fields := make([]string, len(raws))
	for i, field := range raws {
		fields[i] = fmt.Sprint(field)
	}
	handling := fmt.Sprint(adapterConfig.Arguments["handling"])

	return &CaseConversionAdapter{
		id:       id,
		task:     taskName,
		adapter:  adapterName,
		fields:   fields,
		handling: handling,
	}
}

// Returns the output channel of the case converted rows.
//
// The `wg` is the wait group for the goroutine.
// The `in` is the input channel of the rows to be casted.
func (adp *CaseConversionAdapter) Run(wg *sync.WaitGroup, in <-chan core.RowMap) <-chan core.RowMap {
	log.Printf("* Creating #%d instance of case conversion adapter for task %s...", adp.id, adp.task)
	out := make(chan core.RowMap)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for row := range in {
			for _, field := range adp.fields {
				raw, ok := row[field]
				if raw == nil || !ok {
					continue
				}

				var err error = nil
				var value = fmt.Sprint(raw)
				switch adp.handling {
				case "upper":
					row[field] = strings.ToUpper(value)
				case "lower":
					row[field] = strings.ToLower(value)
				case "title":
					row[field] = titleCase(value)
				default:
					log.Fatalf("Invalid case conversion type: %s", adp.handling)
				}

				if err != nil {
					log.Fatalf("Can't convert value %v for field '%s': %s", value, field, err)
				}
			}

			out <- row
		}

		close(out)
	}()

	return out
}

// `titleCase` returns the title case of the given string.
func titleCase(s string) string {
	tmp := []rune(strings.ToLower(s))
	tmp[0] = unicode.ToUpper(tmp[0])
	return string(tmp)
}
