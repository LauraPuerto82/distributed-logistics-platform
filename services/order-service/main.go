package main

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")

	if databaseURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		log.Fatal("KAFKA_BROKER is not set")
	}

	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		log.Fatal("KAFKA_TOPIC is not set")
	}

	conn, err := pgx.Connect(ctx, databaseURL)

	if err != nil {
		log.Fatal("Failed to connect to PostgreSQL:", err)
	}

	defer conn.Close(ctx)

	if err := conn.Ping(ctx); err != nil {
		log.Fatal("PostgreSQL ping failed:", err)
	}

	log.Println("Connected to PostgreSQL")

	store := NewPostgresShipmentStore(conn)
	publisher := NewKafkaEventPublisher(kafkaBroker, kafkaTopic)
	defer publisher.Close()

	router := setupRouter(store, publisher)
	router.Run(":8080")
}
