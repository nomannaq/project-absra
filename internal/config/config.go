package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Server ServerConfig
	Kafka  KafkaConfig
	Auth   AuthConfig
}

type ServerConfig struct {
	Address string
}

type KafkaConfig struct {
	Brokers        []string
	ConsumerGroup  string
	TopicWhitelist map[string][]string // topic -> allowed clients
}

type AuthConfig struct {
	Secret           string
	TokenExpiration  time.Duration
	EnablePermission bool
}

func Load() *Config {
	return &Config{
		Server: ServerConfig{
			Address: getEnv("SERVER_ADDRESS", ":8080"),
		},
		Kafka: KafkaConfig{
			Brokers:        strings.Split(getEnv("KAFKA_BROKERS", "localhost:29092"), ","),
			ConsumerGroup:  getEnv("KAFKA_CONSUMER_GROUP", "event-bus-api"),
			TopicWhitelist: parseTopicWhitelist(getEnv("KAFKA_TOPIC_WHITELIST", "")),
		},
		Auth: AuthConfig{
			Secret:           getEnv("AUTH_SECRET", "your-secret-key"),
			TokenExpiration:  time.Duration(getIntEnv("AUTH_TOKEN_EXPIRATION", 24)) * time.Hour,
			EnablePermission: getBoolEnv("ENABLE_PERMISSION", true),
		},
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getIntEnv(key string, fallback int) int {
	strValue := getEnv(key, "")
	if value, err := strconv.Atoi(strValue); err == nil {
		return value
	}
	return fallback
}

func getBoolEnv(key string, fallback bool) bool {
	strValue := getEnv(key, "")
	if value, err := strconv.ParseBool(strValue); err == nil {
		return value
	}
	return fallback
}

func parseTopicWhitelist(whitelist string) map[string][]string {
	result := make(map[string][]string)
	if whitelist == "" {
		return result
	}

	entries := strings.Split(whitelist, ";")
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
