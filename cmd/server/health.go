package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

type RealHealthServer struct {
	server *http.Server
}

func (s *RealHealthServer) StartHealthServer(ctx context.Context, port int) error {
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
			errChan <- fmt.Errorf("server ListenAndServe failed: %w", err)
		}
		close(errChan) // Закрываем канал после завершения
	}()
	// Ждем завершения или отмены контекста
	select {
	case <-ctx.Done():
		// завершаем сервер при отмене контекста
		if err := server.Shutdown(context.Background()); err != nil {
			return fmt.Errorf("server shutdown error: %w", err)
		}
		return ctx.Err()
	case err := <-errChan:

		return err
	}

}

func (s *RealHealthServer) StopHealthServer() error {
	if s.server == nil {
		return fmt.Errorf("server is not initialized")
	}
	if err := s.server.Shutdown(context.Background()); err != nil {
		return fmt.Errorf("server shutdown failed: %w", err)
	}
	return nil
}
