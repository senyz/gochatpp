package config

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

/*
rabbitmq:

	url: "amqp://admin:password123@rabbitmq:5672/"
	user: "admin"
	pass: "password123"
	host: "rabbitmq"
	port: 5672
*/
type RabbitMQConfig struct {
	URL  string `yaml:"url" json:"url"`
	User string `yaml:"user" json:"user"`
	Pass string `yaml:"pass" json:"pass"`
	Host string `yaml:"host" json:"host"`
	Port int    `yaml:"port" json:"port"`
}

type ChatConfig struct {
	Exchange string `yaml:"exchange" json:"exchange"`
	AuthFile string `yaml:"auth_file" json:"auth_file"`
}

type AppConfig struct {
	LogLevel   string `yaml:"log_level" json:"log_level"`
	ServerPort int    `yaml:"server_port" json:"server_port"`
}

// Основная функция загрузки конфигурации
func Load(filename string) (*FileConfig, error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return loadFromEnv(), nil
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var c FileConfig
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	return &c, nil
}

// Обертка с fallback к переменным окружения
func LoadConfig(filename string) (*FileConfig, error) {
	cfg, err := Load(filename)
	if err != nil {
		log.Printf("Using environment variables due to config error: %v", err)
		return loadFromEnv(), nil
	}
	return cfg, nil
}

// Загрузка значений по умолчанию из переменных окружения
// Загрузка из environment variables
func loadFromEnv() *FileConfig {
	port, _ := strconv.Atoi(getEnv("SERVER_PORT", "8080"))
	rabbitPort, _ := strconv.Atoi(getEnv("RABBITMQ_PORT", "5672"))

	return &FileConfig{
		RabbitMQ: RabbitMQConfig{
			URL:  getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
			User: getEnv("RABBITMQ_USER", "guest"),
			Pass: getEnv("RABBITMQ_PASS", "guest"),
			Host: getEnv("RABBITMQ_HOST", "localhost"),
			Port: rabbitPort,
		},
		Chat: ChatConfig{
			Exchange: getEnv("CHAT_EXCHANGE", "chat_direct"),
			AuthFile: getEnv("AUTH_FILE", "users.json"),
		},
		App: AppConfig{
			LogLevel:   getEnv("LOG_LEVEL", "info"),
			ServerPort: port,
		},
	}
}

// Универсальная функция получения переменной окружения
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return fallback
}
