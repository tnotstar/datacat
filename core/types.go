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

package core

import "sync"

// A `Configurator` is an interface for objects that can provide the
// configuration for the whole application.
type Configurator interface {
	GetDatabaseConfig(databaseName string) DatabaseConfig
	GetSourceConfig(taskName string) SourceConfig
	GetAdaptersConfig(taskName string) []AdapterConfig
	GetTargetConfig(taskName string) TargetConfig
}

// A `RowMap` represents a row of data moving through a task.
type RowMap map[string]any

// A `Source` endpoint is a subtask which retrieves data from a specialized
// type of data source.
type Source interface {
	// Run creates a `goroutine` to execute the retrieval procedure.
	Run(*sync.WaitGroup) <-chan RowMap
}

// An Adapter middlepoint is a subtask which applies a transformation
// to a each row of data retrieved from the previous stage in a task.
type Adapter interface {
	// Run creates a `goroutine` to execute the adapter procedure.
	Run(*sync.WaitGroup, <-chan RowMap) <-chan RowMap
}

// A Target endpoint is a subtask which sends data to a specialized
// type of data target.
type Target interface {
	// Run creates a `goroutine` to execute the sending procedure.
	Run(*sync.WaitGroup, <-chan RowMap)
}
