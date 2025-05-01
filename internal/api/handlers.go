package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/nomannaq/absra/internal/auth"
	"github.com/nomannaq/absra/internal/kafka"
	"github.com/nomannaq/absra/internal/models"
	"github.com/nomannaq/absra/internal/registry"
	"github.com/nomannaq/absra/internal/streaming"
)

// Handler handles all API endpoints
type Handler struct {
	kafkaClient      *kafka.Client
	schemaRegistry   registry.SchemaRegistry
	streamingManager *streaming.Manager
}

// NewHandler creates a new API handler
func NewHandler(
	kafkaClient *kafka.Client,
	schemaRegistry registry.SchemaRegistry,
	streamingManager *streaming.Manager,
) *Handler {
	return &Handler{
		kafkaClient:      kafkaClient,
		schemaRegistry:   schemaRegistry,
		streamingManager: streamingManager,
	}
}

// RegisterEventType registers a new event type with schema
func (h *Handler) RegisterEventType(c *gin.Context) {
	var request struct {
		Type   string          `json:"type" binding:"required"`
		Schema json.RawMessage `json:"schema" binding:"required"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Register schema
	if err := h.schemaRegistry.RegisterSchema(request.Type, string(request.Schema)); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid schema: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"status":  "success",
		"message": "Event type registered successfully",
		"type":    request.Type,
	})
}

// GetEventTypeSchema gets the schema for an event type
func (h *Handler) GetEventTypeSchema(c *gin.Context) {
	eventType := c.Param("type")

	schema, err := h.schemaRegistry.GetSchema(eventType)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Event type not found: " + err.Error()})
		return
	}

	var schemaJSON json.RawMessage
	if err := json.Unmarshal([]byte(schema), &schemaJSON); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Invalid schema format"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"type":   eventType,
		"schema": schemaJSON,
	})
}

// ListEventTypes lists all available event types
func (h *Handler) ListEventTypes(c *gin.Context) {
	eventTypes, err := h.schemaRegistry.ListEventTypes()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list event types: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"event_types": eventTypes,
	})
}

// PublishEvent publishes an event to a specific event type
func (h *Handler) PublishEvent(c *gin.Context) {
	eventType := c.Param("type")
	clientID := c.GetString(string(auth.ClientIDKey))

	// Check access permission
	if !h.kafkaClient.CanAccessTopic(clientID, eventType) {
		c.JSON(http.StatusForbidden, gin.H{"error": "Access denied to event type: " + eventType})
		return
	}

	// Read event data
	eventData, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read request body"})
		return
	}

	// Validate event against schema if registry is available
	if err := h.schemaRegistry.ValidateEvent(eventType, eventData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Event validation failed: " + err.Error(),
		})
		return
	}

	// Get event key from header or generate one
	eventKey := c.GetHeader("X-Event-Key")
	if eventKey == "" {
		eventKey = uuid.NewString()
	}

	// Create full event
	event := models.Event{
		ID:     eventKey,
		Type:   eventType,
		Source: clientID,
		Time:   time.Now().UTC(),
		Data:   eventData,
	}

	// Convert event to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to serialize event"})
		return
	}

	// Publish to Kafka
	partition, offset, err := h.kafkaClient.PublishMessage(c.Request.Context(), eventType, eventKey, eventJSON)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to publish event: " + err.Error()})
		return
	}

	// Forward to streaming consumers
	h.streamingManager.SendEventToConsumers(eventType, eventJSON)

	slog.Info("Event published successfully",
		"event_type", eventType,
		"event_key", eventKey,
		"client_id", clientID,
		"partition", partition,
		"offset", offset)

	// Return success response
	c.JSON(http.StatusAccepted, gin.H{
		"status":    "success",
		"message":   "Event published successfully",
		"type":      eventType,
		"id":        eventKey,
		"partition": partition,
		"offset":    offset,
	})
}
