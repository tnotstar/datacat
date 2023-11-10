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

package core

import (
	"log"

	"github.com/spf13/viper"
)

type Configurer interface {
	GetDatabaseConfig(databaseName string) DatabaseConfig
	GetSourceConfig(taskName string) SourceConfig
	GetAdaptersConfig(taskName string) []AdapterConfig
	GetTargetConfig(taskName string) TargetConfig
}

// `Config` represents the configuration data for the whole application.
type Config struct {
	// A map with all database conections configuration.
	Databases map[string]DatabaseConfig `mapstructure:"databases"`

	// An array with all task configurations
	Tasks map[string]struct {
		// Specifies the configuration for the source endpoint.
		Source SourceConfig `mapstructure:"source"`
		// Specifies the configuration of the adapters array.
		Adapters []AdapterConfig `mapstructure:"adapters"`
		// Specifies the configuration of the target endpoint.
		Target TargetConfig `mapstructure:"target"`
	}
}

// `DatabaseConfig` specifies the configuration for a database conection.
type DatabaseConfig struct {
	// The database driver identifier.
	Driver string `mapstructure:"driver"`
	// The database conection URI.
	URI string `mapstructure:"uri"`
}

// `SourceConfig` specifies the configuration of a source endpoint.
type SourceConfig struct {
	// The type of source endpoint.
	Type string `mapstructure:"type"`
	// The name of the database to fetch data from.
	Database string `mapstructure:"database"`
	// The query to fetch data from the database.
	Query string `mapstructure:"query"`
}

// `AdapterConfig` specifies the configuration of each adapter middlepoint.
type AdapterConfig struct {
	// The type of conversion adapter.
	Type string `mapstructure:"type"`
	// A list of the fields to be converted.
	Fields []string `mapstructure:"fields"`
}

// `TargetConfig` specifies the configuration of the target endpoint.
type TargetConfig struct {
	// The type of source endpoint.
	Type string `mapstructure:"type"`
	// The name or pattern for the output writer.
	Output string `mapstructure:"output"`
}

// `cfg` is the global configuration instance.
var cfg Config

// `LoadConfig` initializes the global configuration instance.
func InitConfig(cfgfile string) {
	viper.AddConfigPath(".")
	viper.SetConfigType("yaml")
	viper.SetConfigFile(cfgfile)

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}

	if err := viper.Unmarshal(&cfg); err != nil {
		log.Fatalf("Error unmarshalling config file: %s", err)
	}

	viper.AutomaticEnv()
}

// `GetConfig` returns a pointer to the global configuration instance.
func GetConfig() *Config {
	return &cfg
}

// `GetDatabaseConfig` returns the configuration of the database
// with a given database name in the global configuration instance.
//
// The `databaseName` is the name of the database to be returned, if it exists.
//
// NB. If the database does not exist, the function will panic.
func (cfg *Config) GetDatabaseConfig(databaseName string) DatabaseConfig {
	database, ok := cfg.Databases[databaseName]
	if !ok {
		log.Fatalf("Missing configuration for database named: %s", databaseName)
	}
	return database
}

// `GetSourceConfig` returns the configuration of the source endpoint of
// the task with a given name in the global configuration instance.
//
// The `taskName` is the name of the task which database configuration will be
// returned, if it exists.
//
// NB. If the task does not exist, the function will panic.
func (cfg *Config) GetSourceConfig(taskName string) SourceConfig {
	task, ok := cfg.Tasks[taskName]
	if !ok {
		log.Fatalf("Missing configuration for task named: %s", taskName)
	}
	return task.Source
}

// `GetAdaptersConfig` returns the array with the configuration of all
// adapter middlepoints of the task with a given name in the global
// configuration instance.
//
// The `taskName` is the name of the task which database configuration will be
// returned, if it exists.
//
// NB. If the task or the adapter does not exist, the function will panic.
func (cfg *Config) GetAdaptersConfig(taskName string) []AdapterConfig {
	task, ok := cfg.Tasks[taskName]
	if !ok {
		log.Fatalf("Missing configuration for task named: %s", taskName)
	}
	return task.Adapters
}

// `GetTargetConfig` returns the configuration of the target endpoint of
// the task with a given name in the global configuration instance.
//
// The `taskName` is the name of the task which database configuration will be
// returned, if it exists.
//
// NB. If the task does not exist, the function will panic.
func (cfg *Config) GetTargetConfig(taskName string) TargetConfig {
	task, ok := cfg.Tasks[taskName]
	if !ok {
		log.Fatalf("Missing configuration for task named: %s", taskName)
	}
	return task.Target
}
