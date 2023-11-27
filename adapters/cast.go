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
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

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
	// The `dataType` to be used to cast.
	dataType string
	// The `inLayout` to be used to parse datetime values.
	inLayout string
	// The `outLayout` to be used to format datetime values.
	outLayout string
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

	raws := adapterConfig.Arguments["fields"].([]any)
	fields := make([]string, len(raws))
	for i, field := range raws {
		fields[i] = fmt.Sprint(field)
	}
	datatype := fmt.Sprint(adapterConfig.Arguments["datatype"])
	inLayout := fmt.Sprint(adapterConfig.Arguments["inlayout"])
	outLayout := fmt.Sprint(adapterConfig.Arguments["outlayout"])

	return &CastToDatatypeAdapter{
		task:      taskName,
		adapter:   adapterName,
		fields:    fields,
		dataType:  datatype,
		inLayout:  inLayout,
		outLayout: outLayout,
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
				raw, ok := row[field]
				if raw == nil || !ok {
					continue
				}

				var err error = nil
				var value = fmt.Sprint(raw)
				switch adp.dataType {
				case "boolean":
					row[field], err = strconv.ParseBool(strings.ToLower(value))
				case "int64":
					row[field], err = strconv.ParseInt(value, 10, 64)
				case "float64":
					row[field], err = strconv.ParseFloat(value, 64)
				case "datetime":
					dtValue, _ := time.Parse(adp.inLayout, value)
					row[field] = dtValue.Format(adp.outLayout)
				default:
					err = errors.New("Invalid datatype " + adp.dataType)
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
