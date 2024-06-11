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

package sources

import (
	"log"

	"github.com/tnotstar/datacat/core"
)

// `BuildSource` creates a new instance of the source endpoint specified
// by the configuration object passed as argument.
//
// The `id` is the index of the adapter to be created.
// The `cfg` is the global configuration object.
// The `taskName` is the name of the task to be executed.
func BuildSource(id int, cfg core.Configurator, taskName string) core.Source {
	sourceConfig, err := cfg.GetSourceConfig(taskName)
	if err != nil {
		log.Fatalf("Error getting source configuration for task %s: %s", taskName, err)
	}

	if IsaDatabaseQuerySource(sourceConfig.Type) {
		return NewDatabaseQuerySource(id, cfg, taskName)
	}

	if IsaJSONLFileSource(sourceConfig.Type) {
		return NewJSONLFileSource(id, cfg, taskName)
	}

	log.Fatalf("Invalid source endpoint type %s", sourceConfig.Type)
	return nil
}
