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
	"strings"
	"sync"

	"github.com/tnotstar/sqltoapi/core"
)

// `CastToDatatypeAdapter` casts the given fields to boolean values.
type CastToDatatypeAdapter struct {
	// The `task` of the task which is running into.
	task string
	// The name of the `adapter`.
	adapter string
	// The `fields` to be casted.
	fields []string
}

// `IsaCastToDatatypeAdapter` returns true if given adapter type
// is CastToDatatype.
func IsaCastToDatatypeAdapter(adapterType string) bool {
	return adapterType == "cast-to-datatype-adapter"
}

// `NewCastToDatatypeAdapter` creates a new instance of the CastToDatatype adapter.
//
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
// The `adapterName` is the name of the adapter to be created.
func NewCastToDatatypeAdapter(cfg core.Configurator, taskName string, adapterName string) core.Adapter {
	adapterConfig, _ := cfg.GetAdapterConfig(taskName, adapterName)

	fields := adapterConfig.Arguments["fields"].([]any)
	fieldstrs := make([]string, len(fields))
	for i, field := range fields {
		fieldstrs[i] = fmt.Sprint(field)
	}

	return &CastToDatatypeAdapter{
		task:    taskName,
		adapter: adapterName,
		fields:  fieldstrs,
	}
}

// CastToDatatype casts given row fields to boolean values.
//
// The `fields` is a list of fields to be casted.
//
// Returns the output channel of the casted rows.
func (adp *CastToDatatypeAdapter) Run(wg *sync.WaitGroup, in <-chan core.RowMap) <-chan core.RowMap {
	out := make(chan core.RowMap)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for row := range in {
			for _, field := range adp.fields {
				rawValue, ok := row[field]
				if !ok {
					continue
				}
				strValue := strings.ToLower(fmt.Sprint(rawValue))
				if strValue == "1" || strValue == "yes" || strValue == "true" {
					row[field] = true
				} else {
					row[field] = false
				}
			}
			out <- row
		}

		close(out)
	}()

	return out
}
