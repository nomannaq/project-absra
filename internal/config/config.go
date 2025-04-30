package config

import (
	"time"
)

type Config struct {
	Server ServerConfig
	Kafka  KafkaConfig
	Auth   AuthConfig
}

type ServerConfig struct {
	Address string
	Port    int
	Mode    string
}

type KafkaConfig struct {
	Brokers       []string
	ConsumerGroup string
	TopicACL      map[string][]string //maps topic to ACLs
}

type AuthConfig struct {
	Secret           string
	TokenExpiration  time.Duration
	EnablePermission bool
}
