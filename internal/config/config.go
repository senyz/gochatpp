package config

import (
	"fmt"
	"os"

	"github.com/stretchr/testify/assert/yaml"
)

type Config struct {
	RabbitMQURL  string
	ExchangeName string
	AuthFile     string
	LogLevel     string
}

func Load(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &c, nil
}
func LoadConfig(filename string) (cfg *Config) {
	cfg, err := Load(filename)
	if err != nil {
		cfg = &Config{
			RabbitMQURL:  getEnv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/"),
			ExchangeName: getEnv("CHAT_EXCHANGE", "chat_direct"),
			AuthFile:     getEnv("AUTH_FILE", "users.json"),
			LogLevel:     getEnv("LOG_LEVEL", "info"),
		}

	}
	return cfg
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return fallback
}
