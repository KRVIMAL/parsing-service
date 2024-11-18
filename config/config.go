package config

import (
	"io"
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Kafka struct {
		URL   string `yaml:"url"`
		Topic string `yaml:"topic"`
	} `yaml:"kafka"`
}

func LoadConfig(configPath string) (*Config, error) {
	config := &Config{}

	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}
