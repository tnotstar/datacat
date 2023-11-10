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
//

package adapters

import (
	"fmt"
	"strings"

	"github.com/tnotstar/sqltoapi/core"
)

// `CastToBooleanAdapter` casts the given fields to boolean values.
type CastToBooleanAdapter struct {
	// The `task` of the task which is running into.
	task string
	// The `fields` to be casted.
	fields []string
}

// `NewCastToBooleanAdapter` creates a new instance of the CastToBoolean adapter.
//
// The `task` is the name of the task to be executed.
// The `fields` is an array with the names of the fields to be casted.
func NewCastToBooleanAdapter(task string, fields []string) core.Adapter {

	return &CastToBooleanAdapter{
		task:   task,
		fields: fields,
	}
}

// CastToBoolean casts given row fields to boolean values.
//
// The `fields` is a list of fields to be casted.
//
// Returns the output channel of the casted rows.
func (adp *CastToBooleanAdapter) Run(in <-chan core.RowMap) <-chan core.RowMap {
	out := make(chan core.RowMap)

	go func() {
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
