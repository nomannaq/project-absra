package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration
type Config struct {
	Server         ServerConfig
	Kafka          KafkaConfig
	Auth           AuthConfig
	SchemaRegistry SchemaRegistryConfig
	Streaming      StreamingConfig
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Address string
	Port    int
	Mode    string // "debug", "release", "test"
}

// KafkaConfig holds Kafka connection and behavior settings
type KafkaConfig struct {
	Brokers       []string
	ConsumerGroup string
	TopicACL      map[string][]string // Maps topics to allowed clients
	EnableTLS     bool
	SASL          struct {
		Enabled   bool
		User      string
		Password  string
		Mechanism string // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	}
}

// AuthConfig holds authentication settings
type AuthConfig struct {
	Secret           string
	TokenExpiration  time.Duration
	EnablePermission bool
}

// SchemaRegistryConfig holds schema registry settings
type SchemaRegistryConfig struct {
	Enabled     bool
	Type        string // "local" or "confluent"
	URL         string // For confluent schema registry
	StoragePath string // For local schema registry
}

// StreamingConfig holds streaming settings
type StreamingConfig struct {
	BufferSize        int
	KeepaliveInterval time.Duration
	MaxConnections    int
}

// GetServerAddress returns the full server address string
func (c *Config) GetServerAddress() string {
	return fmt.Sprintf("%s:%d", c.Server.Address, c.Server.Port)
}

// Load loads configuration from environment variables
func Load() (*Config, error) {
	// Server configuration
	port, err := strconv.Atoi(getEnv("SERVER_PORT", "8080"))
	if err != nil {
		return nil, fmt.Errorf("invalid SERVER_PORT: %w", err)
	}

	serverMode := getEnv("SERVER_MODE", "debug")
	if serverMode != "debug" && serverMode != "release" && serverMode != "test" {
		serverMode = "debug" // Default to debug if invalid
	}

	// Auth configuration
	tokenExpHours, err := strconv.Atoi(getEnv("AUTH_TOKEN_EXPIRATION_HOURS", "24"))
	if err != nil {
		return nil, fmt.Errorf("invalid AUTH_TOKEN_EXPIRATION_HOURS: %w", err)
	}

	authSecret := getEnv("AUTH_SECRET", "")
	if authSecret == "" {
		return nil, errors.New("AUTH_SECRET is required")
	}

	// Streaming configuration
	bufferSize, err := strconv.Atoi(getEnv("STREAMING_BUFFER_SIZE", "100"))
	if err != nil {
		return nil, fmt.Errorf("invalid STREAMING_BUFFER_SIZE: %w", err)
	}

	keepaliveInterval, err := time.ParseDuration(getEnv("STREAMING_KEEPALIVE_INTERVAL", "30s"))
	if err != nil {
		return nil, fmt.Errorf("invalid STREAMING_KEEPALIVE_INTERVAL: %w", err)
	}

	maxConnections, err := strconv.Atoi(getEnv("STREAMING_MAX_CONNECTIONS", "1000"))
	if err != nil {
		return nil, fmt.Errorf("invalid STREAMING_MAX_CONNECTIONS: %w", err)
	}

	// Kafka configuration
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	if len(brokers) == 0 {
		return nil, errors.New("no Kafka brokers specified")
	}

	// Create the config
	cfg := &Config{
		Server: ServerConfig{
			Address: getEnv("SERVER_ADDRESS", "localhost"),
			Port:    port,
			Mode:    serverMode,
		},
		Kafka: KafkaConfig{
			Brokers:       brokers,
			ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "event-bus-api"),
			TopicACL:      parseTopicACL(getEnv("KAFKA_TOPIC_ACL", "")),
			EnableTLS:     getBoolEnv("KAFKA_ENABLE_TLS", false),
			SASL: struct {
				Enabled   bool
				User      string
				Password  string
				Mechanism string
			}{
				Enabled:   getBoolEnv("KAFKA_SASL_ENABLED", false),
				User:      getEnv("KAFKA_SASL_USER", ""),
				Password:  getEnv("KAFKA_SASL_PASSWORD", ""),
				Mechanism: getEnv("KAFKA_SASL_MECHANISM", "PLAIN"),
			},
		},
		Auth: AuthConfig{
			Secret:           authSecret,
			TokenExpiration:  time.Duration(tokenExpHours) * time.Hour,
			EnablePermission: getBoolEnv("ENABLE_PERMISSION", true),
		},
		SchemaRegistry: SchemaRegistryConfig{
			Enabled:     getBoolEnv("SCHEMA_REGISTRY_ENABLED", false),
			Type:        getEnv("SCHEMA_REGISTRY_TYPE", "local"),
			URL:         getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
			StoragePath: getEnv("SCHEMA_STORAGE_PATH", "./schemas"),
		},
		Streaming: StreamingConfig{
			BufferSize:        bufferSize,
			KeepaliveInterval: keepaliveInterval,
			MaxConnections:    maxConnections,
		},
	}

	return cfg, nil
}

// Helper functions
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return fallback
}

func getBoolEnv(key string, fallback bool) bool {
	strValue := getEnv(key, "")
	if strValue == "" {
		return fallback
	}

	value, err := strconv.ParseBool(strValue)
	if err != nil {
		return fallback
	}
	return value
}

// parseTopicACL parses the ACL string into a map.
// Format: "topic1:client1,client2;topic2:client3,client4"
func parseTopicACL(aclStr string) map[string][]string {
	result := make(map[string][]string)
	if aclStr == "" {
		return result
	}

	entries := strings.Split(aclStr, ";")
	for _, entry := range entries {
		parts := strings.Split(entry, ":")
		if len(parts) == 2 {
			topic := parts[0]
			clients := strings.Split(parts[1], ",")
			result[topic] = clients
		}
	}
	return result
}
