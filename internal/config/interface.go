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
