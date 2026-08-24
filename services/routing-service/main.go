package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"
)

func main() {
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		fmt.Println("KAFKA_BROKER is not set")
		return
	}

	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		fmt.Println("KAFKA_TOPIC is not set")
		return
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fmt.Println("DATABASE_URL is not set")
		return
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}
	defer db.Close()

	graph := buildGraph()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   kafkaTopic,
		GroupID: "routing-service",
	})

	defer reader.Close()

	publisher := NewKafkaEventPublisher(
		kafkaBroker,
		kafkaTopic,
	)

	defer publisher.Close()

	deadLetterPublisher := NewKafkaDeadLetterPublisher(
		kafkaBroker,
		"routing-service-dlq",
	)

	defer deadLetterPublisher.Close()

	store := NewPostgresRoutingStore(db)

	go func() {
		for {
			if err := publishPendingEvents(store, publisher); err != nil {
				fmt.Println("Error publishing pending outbox events:", err)
			}

			time.Sleep(5 * time.Second)
		}
	}()

	for {
		message, err := reader.FetchMessage(context.Background())
		if err != nil {
			fmt.Println("Error reading message:", err)
			continue
		}

		if err := handleKafkaMessage(
			context.Background(),
			message,
			graph,
			store,
			reader,
			deadLetterPublisher,
			ExponentialBackoff{},
		); err != nil {
			fmt.Println("Error handling Kafka message:", err)
			continue
		}

		fmt.Println("Kafka message handled successfully")
	}
}
