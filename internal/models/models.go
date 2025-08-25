package models

import (
	"time"

	"golang.org/x/crypto/bcrypt"
)

type Queue struct {
	Name      string `json:"name"`
	VHost     string `json:"vhost"`
	Messages  int    `json:"messages"`
	Consumers int    `json:"consumers"`
}

// ChatMessage - структура сообщения
type ChatMessage struct {
	ID        string    `json:"id"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"` // "direct", "broadcast"
}

// User - структура пользователя
type User struct {
	Username string    `json:"username"`
	Password string    `json:"password"` // Хранится как хэш
	Created  time.Time `json:"created"`
}

// HashPassword - хеширует пароль перед сохранением
func (u *User) HashPassword(password string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	u.Password = string(hash)
	return nil
}

// CheckPassword - проверяет введенный пароль
func (u *User) CheckPassword(password string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(password))
	return err == nil
}
