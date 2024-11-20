// main.go

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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

	// Setup signal handling for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Setup Kafka consumer with a unique GroupID
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{cfg.Kafka.URL},
		Topic:    cfg.Kafka.Topic,
		GroupID:  "socket310pproducer", // Updated GroupID to avoid conflicts
		MaxBytes: 10e6,
	})
	defer reader.Close()

	// Kafka brokers list
	kafkaBrokers := []string{cfg.Kafka.URL}

	// Writer cache to reuse Kafka writers
	var writerCache = make(map[string]*kafka.Writer)
	var writerCacheMutex sync.Mutex
	defer closeAllWriters(writerCache) // Close writers on exit

	log.Println("Parsing Service is running...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down gracefully...")
			return
		default:
			// Consume Kafka messages with a timeout context
			msgCtx, cancel := context.WithTimeout(ctx, 10*time.Second)

			msg, err := reader.FetchMessage(msgCtx)
			cancel() // Cancel the context to avoid resource leaks

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					log.Println("Fetch message timeout")
					continue
				}
				log.Printf("Error fetching message: %v", err)
				continue
			}

			log.Printf("Processing message at offset %d, partition %d", msg.Offset, msg.Partition)

			// Process the message
			if err := processMessage(msg, kafkaBrokers, &writerCache, &writerCacheMutex); err != nil {
				log.Printf("Error processing message at offset %d: %v", msg.Offset, err)
				continue
			}

			// Commit the Kafka message
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Failed to commit message at offset %d: %v", msg.Offset, err)
			} else {
				log.Printf("Successfully committed message at offset %d", msg.Offset)
			}
		}
	}
}

// processMessage handles fetching, processing, and sending the message
func processMessage(msg kafka.Message, kafkaBrokers []string, writerCache *map[string]*kafka.Writer, writerCacheMutex *sync.Mutex) error {
	// Decode the hex data packet
	hexData := string(msg.Value)
	decodedData, err := hex.DecodeString(hexData)
	if err != nil {
		return fmt.Errorf("error decoding hex data: %v", err)
	}

	// Parse the message
	parsedData, err := parser.ReadAndProcessDataPacket(decodedData)
	if err != nil {
		return fmt.Errorf("error parsing data: %v", err)
	}

	// Process parsedData depending on its type
	switch data := parsedData.(type) {
	case map[string]interface{}:
		// Single object
		return processParsedData(kafkaBrokers, data, writerCache, writerCacheMutex)
	case []map[string]interface{}:
		// Slice of objects
		for _, item := range data {
			if err := processParsedData(kafkaBrokers, item, writerCache, writerCacheMutex); err != nil {
				return fmt.Errorf("error processing parsed data item: %v", err)
			}
		}
	default:
		return fmt.Errorf("unknown parsed data type")
	}

	return nil
}

// getKafkaWriter returns a Kafka writer for the specified topic, creating one if it doesn't exist
func getKafkaWriter(brokers []string, topic string, writerCache *map[string]*kafka.Writer, mutex *sync.Mutex) *kafka.Writer {
	mutex.Lock()
	defer mutex.Unlock()

	if writer, ok := (*writerCache)[topic]; ok {
		return writer
	}

	// Create a new Kafka writer for the topic
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // Ensure messages with the same key go to the same partition
		RequiredAcks: -1,
	}

	// Add the writer to the cache
	(*writerCache)[topic] = writer

	return writer
}

// processParsedData handles the parsed data, extracts IMEI, and sends it to Kafka
func processParsedData(kafkaBrokers []string, parsedData map[string]interface{}, writerCache *map[string]*kafka.Writer, writerCacheMutex *sync.Mutex) error {
	// Convert parsedData to JSON
	jsonData, err := json.Marshal(parsedData)
	if err != nil {
		return fmt.Errorf("error converting parsed data to JSON: %v", err)
	}
	fmt.Println("Parsed Data in JSON:", string(jsonData))

	// Extract 'imei' from parsedData
	imeiValue, ok := parsedData["imei"]
	if !ok {
		return fmt.Errorf("IMEI not found in parsed data")
	}
	imei, ok := imeiValue.(string)
	if !ok {
		return fmt.Errorf("IMEI is not a string")
	}

	// Use a single topic name
	topicName := "socket_310p_jsonData"

	// Get or create a Kafka writer for the topic
	writer := getKafkaWriter(kafkaBrokers, topicName, writerCache, writerCacheMutex)
	if writer == nil {
		return fmt.Errorf("failed to get Kafka writer for topic %s", topicName)
	}

	// Retry logic for sending messages
	maxRetries := 5
	backoffDuration := 500 * time.Millisecond
	var retryErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Send the JSON data to the topic with IMEI as the key
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = writer.WriteMessages(ctx,
			kafka.Message{
				Key:   []byte(imei), // Set the IMEI as the message key
				Value: jsonData,
			},
		)
		if err == nil {
			// Success
			fmt.Printf("Sent parsed data to topic %s with IMEI %s\n", topicName, imei)
			return nil
		}

		retryErr = err
		log.Printf("Error sending data to topic %s: %v. Retrying... (%d/%d)", topicName, err, attempt+1, maxRetries)
		time.Sleep(backoffDuration)
	}

	return fmt.Errorf("failed to send data to topic %s after %d retries: %v", topicName, maxRetries, retryErr)
}

// closeAllWriters closes all Kafka writers in the cache
func closeAllWriters(writerCache map[string]*kafka.Writer) {
	for topic, writer := range writerCache {
		if err := writer.Close(); err != nil {
			log.Printf("Error closing writer for topic %s: %v", topic, err)
		}
	}
}
