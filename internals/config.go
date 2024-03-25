package internals

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

type Config struct {
	DefaultIn  string                    `yaml:"default_in"`
	In         map[string]map[string]any `yaml:"in"`
	DefaultOut []string                  `yaml:"default_out"`
	Out        map[string]map[string]any `yaml:"out"`
	Mappings   []map[string]any          `yaml:"mappings"`
}

func (config *Config) LoadFromYaml(path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("cannot find config file %s - UID %d", path, os.Getuid())
	}
	err = yaml.Unmarshal(content, &config)
	if err != nil {
		return fmt.Errorf("cannot parse config file %s", path)
	}
	return nil
}
