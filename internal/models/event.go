package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event represents a message in the event bus
type Event struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Source   string          `json:"source,omitempty"`
	Time     time.Time       `json:"time"`
	Data     json.RawMessage `json:"data"`
	Metadata map[string]any  `json:"metadata,omitempty"`
}

// NewEvent creates a new event with defaults
func NewEvent(eventType string, data json.RawMessage) Event {
	return Event{
		ID:   uuid.NewString(),
		Type: eventType,
		Time: time.Now().UTC(),
		Data: data,
	}
}

// EventEnvelope wraps an event with delivery information
type EventEnvelope struct {
	Event     Event  `json:"event"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}
