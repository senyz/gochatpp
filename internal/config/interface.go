package config

type Config interface {
	GetRabbitMQURL() string
	GetExchangeName() string
	GetAuthFile() string
	GetLogLevel() string
	GetServerPort() int
}

// Реализация для production
type FileConfig struct {
	RabbitMQ RabbitMQConfig `yaml:"rabbitmq"`
	Chat     ChatConfig     `yaml:"chat"`
	App      AppConfig      `yaml:"app"`
}

func (f *FileConfig) GetRabbitMQURL() string {
	return f.RabbitMQ.URL
}

func (f *FileConfig) GetExchangeName() string {
	return f.Chat.Exchange
}

func (f *FileConfig) GetAuthFile() string {
	return f.Chat.AuthFile
}

func (f *FileConfig) GetLogLevel() string {
	return f.App.LogLevel
}

func (f *FileConfig) GetServerPort() int {
	return f.App.ServerPort
}

// Mock для тестов
type MockConfig struct {
	RabbitMQURL  string
	ExchangeName string
	AuthFile     string
	LogLevel     string
	ServerPort   int
}

func (m *MockConfig) GetRabbitMQURL() string {
	return m.RabbitMQURL
}

func (m *MockConfig) GetExchangeName() string {
	return m.ExchangeName
}

func (m *MockConfig) GetAuthFile() string {
	return m.AuthFile
}

func (m *MockConfig) GetLogLevel() string {
	return m.LogLevel
}

func (m *MockConfig) GetServerPort() int {
	return m.ServerPort
}
