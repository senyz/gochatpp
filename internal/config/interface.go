package config

/*
rabbitmq:
  url: "amqp://admin:password123@rabbitmq:5672/"
  user: "admin"
  pass: "password123"
  host: "rabbitmq"
  port: 5672
*/
type Config interface {
	GetRabbitUser() string
	GetRabbitPass() string
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

func (f *FileConfig) GetRabbitUser() string {
	return f.RabbitMQ.User
}
func (f *FileConfig) GetRabbitPass() string {
	return f.RabbitMQ.Pass
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
