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
	"os"
	"path/filepath"
	"sync"

	"github.com/tnotstar/datacat/core"
	"gopkg.in/yaml.v3"
)

// `ConstantMappingAdapter` an adapter to randomize names.
type ConstantMappingAdapter struct {
	// The `id` of the adapter.
	id int
	// The `task` of the task which is running into.
	task string
	// The name of the `adapter`.
	adapter string
	// The `fields` to be casted.
	fields []string
	// The `mapData` is a hash table of mapping constants.
	mapData map[string]string
	// The `default` is the default value for non-mapped constants.
	otherwise string
}

// `IsaConstantMappingAdapter` returns true if given adapter type
// is ConstantMappingAdapter.
func IsaConstantMappingAdapter(adapterType string) bool {
	return adapterType == "constant-mapping-adapter"
}

// `NewConstantMappingAdapter` creates a new instance of the constant mapping adapter.
//
// The `id` is the instance of the adapter to be created.
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
// The `adapterName` is the name of the adapter to be created.
func NewConstantMappingAdapter(id int, cfg core.Configurator, taskName string, adapterName string) core.Adapter {
	adapterConfig, _ := cfg.GetAdapterConfig(taskName, adapterName)

	raws := adapterConfig.Arguments["fields"].([]any)
	fields := make([]string, len(raws))
	for i, field := range raws {
		fields[i] = fmt.Sprint(field)
	}

	relative := fmt.Sprint(adapterConfig.Arguments["filename"])
	basePath := filepath.Dir(cfg.GetConfigFilename())
	filename := core.ResolveFilename(basePath, relative)
	mapName := fmt.Sprint(adapterConfig.Arguments["mapname"])
	mapData := getMapData(filename, mapName)
	otherwise := fmt.Sprint(adapterConfig.Arguments["otherwise"])

	return &ConstantMappingAdapter{
		id:        id,
		task:      taskName,
		adapter:   adapterName,
		fields:    fields,
		mapData:   mapData,
		otherwise: otherwise,
	}
}

// Returns the output channel of the constant mapping rows.
//
// The `wg` is the wait group for the goroutine.
// The `in` is the input channel of the rows to be casted.
func (adp *ConstantMappingAdapter) Run(wg *sync.WaitGroup, in <-chan core.RowMap) <-chan core.RowMap {
	log.Printf("* Creating #%d instance of constant mapping adapter for task %s...", adp.id, adp.task)
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

				var value = fmt.Sprint(raw)
				if mapped, ok := adp.mapData[value]; ok {
					row[field] = mapped
				} else {
					row[field] = adp.otherwise
				}
			}

			out <- row
		}

		close(out)
	}()

	return out
}

// `getMapData` returns the mapped terms from the file with given filename.
func getMapData(filename string, mapName string) map[string]string {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Error reading mapping file '%s': %v", filename, err)
	}

	var raw map[string]any = make(map[string]any)

	if err = yaml.Unmarshal(data, &raw); err != nil {
		log.Fatalf("Error parsing mapping file '%s': %v", filename, err)
	}

	mappings, ok := raw["mappings"]
	if !ok {
		log.Fatalf("Invalid top mapping container at file '%s'", filename)
	}

	mapRaw, ok := mappings.(map[string]any)[mapName]
	if !ok {
		log.Fatalf("Invalid mapping object with name '%s'", mapName)
	}

	mapData := make(map[string]string)
	for k, v := range mapRaw.(map[string]any) {
		mapData[k] = fmt.Sprint(v)
	}

	return mapData
}
