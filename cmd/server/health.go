package main

import (
	"log"
	"net/http"
	"strconv"
	"time"
)

type RealHealthServer struct {
	server *http.Server
}

func (s *RealHealthServer) StartHealthServer(port int) error {
	// Создаем канал для передачи ошибок из горутины
	errChan := make(chan error, 1)

	// Создаем и настраиваем HTTP-сервер
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:         ":" + strconv.Itoa(port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	// Запускаем сервер в отдельной горутине
	go func() {
		log.Printf("Health check server starting on port %d", port)

		// Перехватываем ошибки сервера
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Health server error: %v", err)
			errChan <- err
		}
		close(errChan) // Закрываем канал после завершения
	}()
	// Ждем завершения или отмены контекста
	select {
	case err := <-errChan:
		if err == nil {
			return nil
		}
		return err
	}

}

func (s *RealHealthServer) StopHealthServer() error {
	err := s.server.Close()
	return err
}
