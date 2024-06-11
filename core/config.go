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
	"errors"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

// Represents the configuration for the whole application.
type Config struct {
	// A map with all database configurations.
	Databases map[string]DatabaseConfig `mapstructure:"databases"`

	// A map with all http-endpoint configurations.
	Services map[string]ServiceConfig `mapstructure:"services"`

	// A map with all task configurations.
	Tasks map[string]struct {
		// Specifies the configuration for the source endpoint.
		Source SourceConfig `mapstructure:"source"`
		// Specifies the configuration of the adapters array.
		Adapters map[string]AdapterConfig `mapstructure:"adapters"`
		// Specifies the configuration of the target endpoint.
		Target TargetConfig `mapstructure:"target"`
	}

	// The name of the configuration file loaded from.
	configFilename string
}

// `DatabaseConfig` specifies the configuration for a database connection.
type DatabaseConfig struct {
	// The database `driver` identifier.
	Driver string `mapstructure:"driver"`
	// The database `schema` name.
	Scheme string `mapstructure:"scheme"`
	// The server `host` name.
	Host string `mapstructure:"host"`
	// The server `port` number.
	Port int `mapstructure:"port"`
	// The instance or the `service` name.
	Service string `mapstructure:"service"`
	// The connection `username`.
	Username string `mapstructure:"username"`
	// The connection user `password`.
	Password string `mapstructure:"password"`
	// The connection `parameters`.
	Parameters map[string]string `mapstructure:"parameters"`
}

// `ServiceConfig` specifies the configuration for an HTTP endpoint.
type ServiceConfig struct {
	// The base URL for the endpoint.
	BaseURL string `mapstructure:"baseurl"`
	// The `method` for the HTTP request.
	Method string `mapstructure:"method"`
	// The query `parameters` for the HTTP request.
	Parameters map[string]string `mapstructure:"parameters"`
	// The name of the authorization service to use.
	WithAuthz string `mapstructure:"withauthz"`
	// A flag to indicate if the certificate must be trusted.
	TrustCert bool `mapstructure:"trustcert"`
}

// `SourceConfig` specifies the configuration of a source endpoint.
type SourceConfig struct {
	// The type of source endpoint.
	Type string `mapstructure:"type"`
	// The arguments for the source driver.
	Arguments map[string]any `mapstructure:"arguments"`
}

// `AdaptersConfig` specifies the configuration of an adapter middlepoint.
type AdapterConfig struct {
	// The type of conversion adapter.
	Type string `mapstructure:"type"`
	// The execution `order` of the adapter in the chain.
	Order int `mapstructure:"order"`
	// The arguments for the adapter driver.
	Arguments map[string]any `mapstructure:"arguments"`
}

// `TargetConfig` specifies the configuration of the target endpoint.
type TargetConfig struct {
	// The type of source endpoint.
	Type string `mapstructure:"type"`
	// The name or pattern for the output to target.
	Arguments map[string]any `mapstructure:"arguments"`
}

// `cfg` is the global configuration instance.
var cfg Config

// `GetConfig` returns a pointer to the global configuration instance.
func GetConfig() *Config {
	return &cfg
}

// `GetDatabaseConfig` method implementation.
func (cfg *Config) GetDatabaseConfig(name string) (*DatabaseConfig, error) {
	database, ok := cfg.Databases[name]
	if !ok {
		return nil, errors.New("Missing configuration for database: " + name)
	}

	return &database, nil
}

// `GetServiceConfig` method implementation.
func (cfg *Config) GetServiceConfig(name string) (*ServiceConfig, error) {
	service, ok := cfg.Services[name]
	if !ok {
		return nil, errors.New("Missing configuration for service: " + name)
	}

	return &service, nil
}

// `GetSourceConfig` method implementation.
func (cfg *Config) GetSourceConfig(name string) (*SourceConfig, error) {
	task, ok := cfg.Tasks[name]
	if !ok {
		return nil, errors.New("Missing configuration for task: " + name)
	}

	return &task.Source, nil
}

// `GetAdapterNames` method implementation.
func (cfg *Config) GetAdapterNames(name string) []string {
	task, ok := cfg.Tasks[name]
	if !ok {
		return []string{}
	}

	names := make([]string, 0, len(task.Adapters))
	for name := range task.Adapters {
		names = append(names, name)
	}

	sort.Slice(names, func(i, j int) bool {
		return task.Adapters[names[i]].Order < task.Adapters[names[j]].Order
	})

	return names
}

// `GetAdapterConfig` method implementation.
func (cfg *Config) GetAdapterConfig(name string, key string) (*AdapterConfig, error) {
	task, ok := cfg.Tasks[name]
	if !ok {
		return nil, errors.New("Missing configuration for task: " + name)
	}

	adapter, ok := task.Adapters[key]
	if !ok {
		return nil, errors.New("Missing configuration for adapter: " + key)
	}
	return &adapter, nil
}

// `GetTargetConfig` method implementation.
func (cfg *Config) GetTargetConfig(name string) (*TargetConfig, error) {
	task, ok := cfg.Tasks[name]
	if !ok {
		return nil, errors.New("Missing configuration for task: " + name)
	}

	return &task.Target, nil
}

// `GetConfigFilename` method implementation.
func (cfg *Config) GetConfigFilename() string {
	return cfg.configFilename
}

// The default environment variable prefix.
const defaultEnvPrefix = "SQL2API"

// `LoadConfig` initializes the global configuration instance.
func LoadConfig(cfgfile string) {
	env := os.Getenv(defaultEnvPrefix + "_ENV")
	if env == "" {
		env = "development"
	}

	godotenv.Load(".env." + env + ".local")
	if env != "test" {
		godotenv.Load(".env.local")
	}
	godotenv.Load(".env." + env)
	godotenv.Load()

	viper.AddConfigPath(".")
	viper.SetConfigType("yaml")
	viper.SetConfigFile(cfgfile)

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}

	viper.AutomaticEnv()
	viper.SetEnvPrefix(defaultEnvPrefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := viper.Unmarshal(&cfg); err != nil {
		log.Fatalf("Error unmarshalling config file: %s", err)
	}

	cfg.configFilename = viper.ConfigFileUsed()
}
