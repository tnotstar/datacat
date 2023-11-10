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

	"github.com/tnotstar/sqltoapi/core"
)

// `BuildSource` creates a new instance of the source endpoint specified
// by the configuration object passed as argument.
//
// The `task` is the name of the task is being executed.
// The `srcfg` is the configuration for the object to be created.
func BuildSource(task string, srcfg core.SourceConfig) core.Source {
	cfg := core.GetConfig()
	dbcfg := cfg.GetDatabaseConfig(srcfg.Database)

	switch srcfg.Type {
	case "oracle-query":
		return NewOracleSource(task, dbcfg.URI, srcfg.Query)
	default:
		log.Fatal("Invalid source type: ", srcfg.Type)
	}

	return nil
}
