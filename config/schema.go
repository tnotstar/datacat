//
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

package config

// Config represents the configuration of the application.
type Config struct {
	// A section for databases configurations.
	Databases map[string]DatabaseConfig `mapstructure:"databases"`

	// A section for tasks configurations.
	Tasks struct {
		// A subsection for fetch tasks configurations.
		Fetch map[string]FetchConfig `mapstructure:"fetch"`
	} `mapstructure:"tasks"`
}

// DatabaseConfig represents the configuration of a database.
type DatabaseConfig struct {
	// The database driver.
	Driver string `mapstructure:"driver"`

	// The database URI.
	URI string `mapstructure:"uri"`
}

// FetchConfig represents the configuration of a fetch task.
type FetchConfig struct {
	// Specifies the source of the task.
	Source struct {
		// The name of the database to fetch data from.
		Database string `mapstructure:"database"`

		// The query to fetch data from the database.
		Query string `mapstructure:"query"`
	} `mapstructure:"source"`

	// Specifies the post-changes applied to the fetched data.
	Changes []struct {
		// The type of conversion to be applied.
		Type string `mapstructure:"type"`

		// A list of the fields to be converted.
		Fields []string `mapstructure:"fields"`
	} `mapstructure:"changes"`
}
