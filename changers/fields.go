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

package changers

import (
	"fmt"
	"strings"

	"github.com/tnotstar/sqltoapi/core"
)

// CastToBoolean casts the given fields to boolean values.
//
// The `fields` is a list of fields to be casted.
//
// Returns the output channel of the casted rows.
func CastToBoolean(in <-chan core.RowMap, fields []string) <-chan core.RowMap {
	out := make(chan core.RowMap)

	go func() {
		for row := range in {
			out <- castGivenFieldsToBoolean(row, fields)
		}
		close(out)
	}()

	return out
}

// castGivenFieldsToBoolean casts given fields to boolean values.
//
// The `row` is the row to be casted.
// The `fields` is a list of fields to be casted.
//
// Returns the modified row instance with fields casted.
func castGivenFieldsToBoolean(row core.RowMap, fields []string) core.RowMap {
	for _, field := range fields {
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

	return row
}
