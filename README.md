# goChat++ - Распределенная чат-система на Go и RabbitMQ

**goChatpp** — это распределенная система обмена сообщениями с IRC-подобным интерфейсом, построенная на языке Go и брокере сообщений RabbitMQ. Система поддерживает прямую отправку сообщений между пользователями, бродкаст и автоматическое управление очередями.

---

## 🚀 Основные функции

- **Прямая доставка сообщений** (1:1)
- **Бродкаст** (сообщение всем онлайн-пользователям)
- **Распределенная архитектура** через RabbitMQ
- **Пользовательская аутентификация**
- **Автоматическое удаление неактивных очередей**
- **Поддержка тысяч одновременных пользователей**

---

## 🧱 Структура проекта

```
gochatpp
|
├── go.mod
├── go.sum
├── models.go
├── .env.example
│
├───client/
│   └── main.go     # Клиентский код
│
└───server/
    └── main.go     # Серверный код
```

---

## 🛠 Установка и запуск

### 1. Зависимости

```
# Клонирование репозитория
git clone <repository-url>
cd gochatpp

# Инициализация модуля
go mod init gochatpp

# Установка зависимостей
go mod tidy
```

---

### 2. Запуск RabbitMQ

#### Базовый запуск:
```
docker run -d \
  --name rabbitmq-chat \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

#### С сохранением данных:
```
mkdir -p rabbitmq/data rabbitmq/logs

docker run -d \
  --name rabbitmq-chat \
  -p 5672:5672 \
  -p 15672:15672 \
  -v $(pwd)/rabbitmq/data:/var/lib/rabbitmq \
  -v $(pwd)/rabbitmq/logs:/var/log/rabbitmq \
  rabbitmq:3-management
```

---

### 3. Переменные окружения

Создайте файл `.env` в корне проекта:

```
RABBITMQ_URL=amqp://admin:password123@localhost:5672/
CHAT_EXCHANGE=chat_direct
AUTH_FILE=users.json
LOG_LEVEL=info
```

---

## 🚀 Компиляция и запуск

### Сборка бинарников:

```
# Сборка сервера
go build -o bin/server ./server

# Сборка клиента
go build -o bin/client ./client
```

### Запуск системы:

```
# Terminal 1: Запуск RabbitMQ (если еще не запущен)
docker start rabbitmq-chat

# Terminal 2: Запуск сервера
./bin/server

# Terminal 3+: Запуск клиентов
./bin/client
```

---

## 💬 Команды клиента

| Команда              | Описание                                  |
|----------------------|-------------------------------------------|
| `!/help`             | Показать справку                          |
| `!/chat <user> <msg>`| Отправить сообщение пользователю          |
| `!/broadcast <msg>`  | Отправить сообщение всем                 |
| `!/online`           | Показать список онлайн-пользователей     |
| `!/exit`             | Выйти из чата                            |

---

## 🔄 Пример использования

```
Enter username (user@domain): alice@example.com
Enter password: ********

Welcome to Chat++! Type !/help for commands

!/chat bob@example.com Hello Bob!
[bob@example.com]: Hi Alice! How are you?
!/broadcast This is a test message
!/exit
```

---

## 🧪 Тестирование

```
# Запуск всех тестов
go test ./...

# Тест подключения к RabbitMQ
go run ./test/connection_test.go
```

---

## 🔐 Безопасность

- **Хеширование паролей** (используется `bcrypt`)
- **Избегайте дефолтных учетных записей** в продакшене
- **Рекомендуется использовать TLS** для соединений

---

## 📈 Мониторинг RabbitMQ

- **Веб-интерфейс**: [http://localhost:15672](http://localhost:15672)
- **Логины**:
  - `admin/password123` (из `.env`)
  - `guest/guest` (по умолчанию)

---

## 🧂 Структура очередей

| Тип         | Название        | Binding Key      |
|-------------|------------------|-------------------|
| Exchange    | `chat_direct`   | `direct`          |
| Queue       | `user_<username>`| `user.<username>` |

---

## 🧹 Чистка неактивных очередей

Очереди удаляются автоматически при отключении клиента (свойство `auto_delete = true`).

---

## 📦 Лицензия

MIT License — см. файл [LICENSE](LICENSE) для деталей.

---

## 🤝 Вклад в проект

1. **Клонируйте репозиторий**
2. **Создайте ветку** для своей задачи
3. **Добавьте новые функции** в соответствующие модули (`models.go`, `server/main.go`, `client/main.go`)
4. **Тестируйте** изменения
5. **Отправьте Pull Request**

---

## 🛠 Тroubleshooting

### RabbitMQ недоступен
```
docker ps -a | grep rabbitmq
docker logs rabbitmq-chat
```

### Порт занят
```
sudo lsof -i :5672
sudo lsof -i :15672
```

### Логирование
```
export LOG_LEVEL=debug
./bin/server
```
