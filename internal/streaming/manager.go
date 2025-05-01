package streaming

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/nomannaq/absra/internal/config"
	"github.com/nomannaq/absra/internal/kafka"
)

// Consumer represents a streaming consumer connection
type Consumer struct {
	ID          string
	ClientID    string
	Topics      []string
	MessageChan chan []byte
	DoneChan    chan struct{}
	CreatedAt   time.Time
	LastActive  time.Time
}

// Manager manages streaming connections
type Manager struct {
	kafkaClient       *kafka.Client
	consumers         map[string]*Consumer
	mu                sync.RWMutex
	bufferSize        int
	keepaliveInterval time.Duration
	maxConnections    int
	activeConnections int
}

// NewManager creates a new streaming manager
func NewManager(kafkaClient *kafka.Client, cfg config.StreamingConfig) *Manager {
	m := &Manager{
		kafkaClient:       kafkaClient,
		consumers:         make(map[string]*Consumer),
		bufferSize:        cfg.BufferSize,
		keepaliveInterval: cfg.KeepaliveInterval,
		maxConnections:    cfg.MaxConnections,
	}

	// Start keepalive checker
	go m.keepaliveChecker()

	return m
}

// CreateConsumer initializes a new consumer streaming connection
func (m *Manager) CreateConsumer(clientID string, topics []string) (*Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check connection limits
	if m.activeConnections >= m.maxConnections {
		return nil, fmt.Errorf("maximum number of connections reached (%d)", m.maxConnections)
	}

	// Validate access to topics
	for _, topic := range topics {
		if !m.kafkaClient.CanAccessTopic(clientID, topic) {
			return nil, fmt.Errorf("access denied to topic: %s", topic)
		}
	}

	consumer := &Consumer{
		ID:          uuid.NewString(),
		ClientID:    clientID,
		Topics:      topics,
		MessageChan: make(chan []byte, m.bufferSize),
		DoneChan:    make(chan struct{}),
		CreatedAt:   time.Now(),
		LastActive:  time.Now(),
	}

	m.consumers[consumer.ID] = consumer
	m.activeConnections++

	slog.Info("Consumer created",
		"consumer_id", consumer.ID,
		"client_id", clientID,
		"topics", topics,
		"active_connections", m.activeConnections)

	return consumer, nil
}

// HandleStreamRequest handles an HTTP streaming request
func (m *Manager) HandleStreamRequest(c *gin.Context) {
	clientID := c.GetString("clientID")
	if clientID == "" {
		c.JSON(401, gin.H{"error": "Authentication required"})
		return
	}

	topics := c.QueryArray("topic")
	if len(topics) == 0 {
		c.JSON(400, gin.H{"error": "At least one topic must be specified"})
		return
	}

	consumer, err := m.CreateConsumer(clientID, topics)
	if err != nil {
		c.JSON(403, gin.H{"error": err.Error()})
		return
	}

	// Set headers for SSE (Server-Sent Events)
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("Transfer-Encoding", "chunked")
	c.Writer.Flush()

	// Send a connection established message
	c.SSEvent("info", gin.H{
		"message":   "Connection established",
		"stream_id": consumer.ID,
		"topics":    consumer.Topics,
	})
	c.Writer.Flush()

	// Handle client disconnect
	go func() {
		<-c.Request.Context().Done()
		m.removeConsumer(consumer.ID)
	}()

	// Keep alive ticker
	ticker := time.NewTicker(m.keepaliveInterval / 2)
	defer ticker.Stop()

	// Stream events
	for {
		select {
		case msg, ok := <-consumer.MessageChan:
			if !ok {
				return
			}
			c.SSEvent("event", string(msg))
			c.Writer.Flush()

			m.mu.Lock()
			if consumer, exists := m.consumers[consumer.ID]; exists {
				consumer.LastActive = time.Now()
			}
			m.mu.Unlock()

		case <-ticker.C:
			// Send keepalive
			c.SSEvent("keepalive", gin.H{"time": time.Now().Format(time.RFC3339)})
			c.Writer.Flush()

		case <-consumer.DoneChan:
			return
		}
	}
}

// SendEventToConsumers sends an event to all consumers subscribed to the topic
func (m *Manager) SendEventToConsumers(topic string, eventData []byte) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, consumer := range m.consumers {
		for _, t := range consumer.Topics {
			if t == topic {
				select {
				case consumer.MessageChan <- eventData:
					// Message sent
				default:
					// Channel full, log warning but don't block
					slog.Warn("Consumer message buffer full, dropping message",
						"consumer_id", consumer.ID,
						"topic", topic)
				}
				break
			}
		}
	}
}

// CloseAll closes all consumer connections
func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	slog.Info("Closing all consumer connections", "count", len(m.consumers))

	for _, consumer := range m.consumers {
		close(consumer.DoneChan)
		close(consumer.MessageChan)
	}

	m.consumers = make(map[string]*Consumer)
	m.activeConnections = 0
}

// removeConsumer removes a consumer
func (m *Manager) removeConsumer(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if consumer, exists := m.consumers[id]; exists {
		close(consumer.DoneChan)
		close(consumer.MessageChan)
		delete(m.consumers, id)
		m.activeConnections--

		slog.Info("Consumer removed",
			"consumer_id", id,
			"client_id", consumer.ClientID,
			"active_connections", m.activeConnections)
	}
}

// keepaliveChecker periodically checks for inactive consumers
func (m *Manager) keepaliveChecker() {
	ticker := time.NewTicker(m.keepaliveInterval)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.checkInactiveConsumers()
	}
}

// checkInactiveConsumers removes consumers that haven't been active
func (m *Manager) checkInactiveConsumers() {
	timeout := time.Now().Add(-2 * m.keepaliveInterval)
	toRemove := []string{}

	m.mu.RLock()
	for id, consumer := range m.consumers {
		if consumer.LastActive.Before(timeout) {
			toRemove = append(toRemove, id)
		}
	}
	m.mu.RUnlock()

	for _, id := range toRemove {
		slog.Info("Consumer timed out", "consumer_id", id)
		m.removeConsumer(id)
	}
}
