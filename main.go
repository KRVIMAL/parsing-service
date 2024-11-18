package main

import (
	"context"
	"fmt"
	"log"

	"github.com/KRVIMAL/parsing-service/config"
	"github.com/KRVIMAL/parsing-service/parser"
	"github.com/segmentio/kafka-go"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Setup Kafka consumer
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{cfg.Kafka.URL},
		Topic:    cfg.Kafka.Topic,
		GroupID:  "", // No GroupID for dynamic topic
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Println("Parsing Service is running...")

	for {
		// Consume Kafka messages
		msg, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Error fetching message: %v", err)
			continue
		}

		// Parse the message
		parsedData, err := parser.ReadAndProcessDataPacket(msg.Value)
		if err != nil {
			log.Printf("Error parsing data: %v", err)
			continue
		}
		fmt.Println(parsedData)
		// Send parsed data to Notification Service
		// err = parser.PublishToQueue(cfg, parsedData)
		if err != nil {
			log.Printf("Error publishing parsed data: %v", err)
			continue
		}

		// Commit the Kafka message
		err = reader.CommitMessages(context.Background(), msg)
		if err != nil {
			log.Printf("Failed to commit message: %v", err)
		}
	}
}
