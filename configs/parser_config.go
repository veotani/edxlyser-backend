package configs

import (
	"os"

	"gopkg.in/yaml.v2"
)

// ParserConfig determines structure to store parser services configs
type ParserConfig struct {
	Kafka struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	} `yaml:"kafka"`
	Elastic struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	} `yaml:"elastic"`
}

// GetParserConfig returns config object. It takes an configFileName to
// be able to have several configs. In the program entry point the
// filenames should be determined.
func GetParserConfig(configFileName string) (ParserConfig, error) {
	f, err := os.Open(configFileName)
	if err != nil {
		return ParserConfig{}, err
	}
	defer f.Close()

	var cfg ParserConfig
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		return ParserConfig{}, err
	}

	return cfg, nil
}
