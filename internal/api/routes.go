package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/nomannaq/absra/internal/auth"
	"github.com/nomannaq/absra/internal/kafka"
	"github.com/nomannaq/absra/internal/registry"
	"github.com/nomannaq/absra/internal/streaming"
)

// SetupRoutes configures all API routes
func SetupRoutes(
	router *gin.Engine,
	kafkaClient *kafka.Client,
	authService *auth.Service,
	schemaRegistry registry.SchemaRegistry,
	streamingManager *streaming.Manager,
) {
	// Create handler
	handler := NewHandler(kafkaClient, schemaRegistry, streamingManager)

	// API v1 group
	v1 := router.Group("/api/v1")
	{
		// Public documentation
		v1.GET("/", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{
				"name":          "ABSRA Event Bus API",
				"version":       "1.0.0",
				"documentation": "/docs",
			})
		})

		// Auth endpoints (public)
		v1.POST("/auth/token", authService.IssueToken)

		// Protected routes
		protected := v1.Group("/")
		protected.Use(authService.Middleware())
		{
			// Event Types & Schema Management
			protected.GET("/event-types", handler.ListEventTypes)
			protected.POST("/event-types", handler.RegisterEventType)
			protected.GET("/event-types/:type", handler.GetEventTypeSchema)

			// Event Publishing
			protected.POST("/events/:type", handler.PublishEvent)

			// Event Consumption via SSE (Server-Sent Events)
			protected.GET("/streams", streamingManager.HandleStreamRequest)
		}
	}
}
