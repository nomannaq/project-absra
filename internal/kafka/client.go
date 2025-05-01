package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/nomannaq/absra/internal/config"
)

// Client wraps Kafka producer and consumer functionality
type Client struct {
	config      *sarama.Config
	brokers     []string
	producer    sarama.SyncProducer
	topicACL    map[string][]string
	mu          sync.RWMutex
	adminClient sarama.ClusterAdmin
}

// NewClient creates a new Kafka client
func NewClient(cfg config.KafkaConfig) (*Client, error) {
	// Create Kafka configuration
	kafkaCfg := sarama.NewConfig()

	// Set modern defaults
	kafkaCfg.Version = sarama.V3_0_0_0 // Use a recent version
	kafkaCfg.Producer.Return.Successes = true
	kafkaCfg.Producer.Return.Errors = true
	kafkaCfg.Producer.RequiredAcks = sarama.WaitForAll
	kafkaCfg.Producer.Retry.Max = 5
	kafkaCfg.Producer.Retry.Backoff = 500 * time.Millisecond
	kafkaCfg.ClientID = "absra-event-bus"
	kafkaCfg.Consumer.Return.Errors = true

	// Configure security if enabled
	if cfg.EnableTLS {
		kafkaCfg.Net.TLS.Enable = true
		// In production you would add proper TLS configuration here:
		// kafkaCfg.Net.TLS.Config = &tls.Config{...}
	}

	if cfg.SASL.Enabled {
		kafkaCfg.Net.SASL.Enable = true
		kafkaCfg.Net.SASL.User = cfg.SASL.User
		kafkaCfg.Net.SASL.Password = cfg.SASL.Password

		switch cfg.SASL.Mechanism {
		case "PLAIN":
			kafkaCfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			kafkaCfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512":
			kafkaCfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", cfg.SASL.Mechanism)
		}
	}

	// Create producer
	producer, err := sarama.NewSyncProducer(cfg.Brokers, kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Create admin client
	admin, err := sarama.NewClusterAdmin(cfg.Brokers, kafkaCfg)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("failed to create Kafka admin client: %w", err)
	}

	// Create client instance
	client := &Client{
		config:      kafkaCfg,
		brokers:     cfg.Brokers,
		producer:    producer,
		topicACL:    cfg.TopicACL,
		adminClient: admin,
	}

	slog.Info("Kafka client initialized", "brokers", cfg.Brokers)

	return client, nil
}

// Close closes the Kafka client connections
func (c *Client) Close() error {
	var errs []error

	if err := c.producer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("producer close error: %w", err))
	}

	if err := c.adminClient.Close(); err != nil {
		errs = append(errs, fmt.Errorf("admin client close error: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing Kafka client: %v", errs)
	}

	return nil
}

// PublishMessage publishes an event to a Kafka topic
func (c *Client) PublishMessage(ctx context.Context, topic, key string, value []byte) (int32, int64, error) {
	// Ensure topic exists
	if err := c.ensureTopicExists(topic); err != nil {
		return 0, 0, fmt.Errorf("topic creation error: %w", err)
	}

	// If key is empty, use a timestamp as key
	if key == "" {
		key = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	// Create message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("producer"),
				Value: []byte("absra-event-bus"),
			},
			{
				Key:   []byte("timestamp"),
				Value: []byte(fmt.Sprintf("%d", time.Now().UnixNano())),
			},
		},
	}

	// Send the message
	partition, offset, err := c.producer.SendMessage(msg)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to publish message: %w", err)
	}

	slog.Info("Message published",
		"topic", topic,
		"partition", partition,
		"offset", offset)

	return partition, offset, nil
}

// CanAccessTopic checks if a client has access to a topic
func (c *Client) CanAccessTopic(clientID, topic string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// If no ACL is defined, allow access
	if len(c.topicACL) == 0 {
		return true
	}

	// Check if topic has specific ACL
	if clients, exists := c.topicACL[topic]; exists {
		for _, id := range clients {
			if id == clientID || id == "*" {
				return true
			}
		}
		return false
	}

	// If topic isn't specifically restricted, allow access
	return true
}

// ensureTopicExists creates a topic if it doesn't exist
func (c *Client) ensureTopicExists(topic string) error {
	topics, err := c.adminClient.ListTopics()
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	// Check if topic exists
	if _, exists := topics[topic]; exists {
		return nil
	}

	// Create topic with sensible defaults
	err = c.adminClient.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     3,
		ReplicationFactor: 1, // Use higher value in production
		ConfigEntries: map[string]*string{
			"retention.ms": strPtr("604800000"), // 7 days
		},
	}, false)

	if err != nil {
		// Check if it's an error because topic already exists
		if errors.Is(err, sarama.ErrTopicAlreadyExists) {
			return nil
		}
		return fmt.Errorf("failed to create topic: %w", err)
	}

	slog.Info("Created new topic", "topic", topic)
	return nil
}

// Helper to convert string to pointer
func strPtr(s string) *string {
	return &s
}
