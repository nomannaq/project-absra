package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"

	"github.com/nomannaq/absra/internal/api"
	"github.com/nomannaq/absra/internal/auth"
	"github.com/nomannaq/absra/internal/config"
	"github.com/nomannaq/absra/internal/kafka"
	"github.com/nomannaq/absra/internal/registry"
	"github.com/nomannaq/absra/internal/streaming"
)

func main() {
	// Configure structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)
	// Load environment variables
	if err := godotenv.Load("../../.env"); err != nil {
		slog.Info("No .env file found or error loading, using environment variables")
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	// Set Gin mode
	gin.SetMode(cfg.Server.Mode)

	// Initialize Kafka client
	kafkaClient, err := kafka.NewClient(cfg.Kafka)
	if err != nil {
		slog.Error("Failed to initialize Kafka client", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	// Initialize schema registry if enabled
	var schemaRegistry registry.SchemaRegistry
	if cfg.SchemaRegistry.Enabled {
		schemaRegistry, err = registry.NewSchemaRegistry(cfg.SchemaRegistry)
		if err != nil {
			slog.Error("Failed to initialize schema registry", "error", err)
			os.Exit(1)
		}
	} else {
		slog.Info("Schema registry is disabled")
		schemaRegistry = registry.NewNoOpRegistry()
	}

	// Initialize authentication service
	authService := auth.NewService(cfg.Auth)

	// Initialize streaming manager for consumer connections
	streamingManager := streaming.NewManager(kafkaClient, cfg.Streaming)

	// Initialize router with middleware
	router := gin.New()
	router.Use(gin.Recovery())

	// Add logging middleware
	router.Use(func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		slog.Info("Request processed",
			"path", path,
			"method", c.Request.Method,
			"status", status,
			"latency", latency.String(),
			"client_ip", c.ClientIP(),
		)
	})

	// Configure CORS
	router.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Content-Length, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
			"time":   time.Now().Format(time.RFC3339),
		})
	})

	// Setup API routes
	api.SetupRoutes(
		router,
		kafkaClient,
		authService,
		schemaRegistry,
		streamingManager,
	)

	// Create HTTP server with secure defaults
	srv := &http.Server{
		Addr:              cfg.GetServerAddress(),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second, // Longer timeout for streaming
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MB
	}

	// Start server in a goroutine
	go func() {
		slog.Info("Starting server", "address", cfg.GetServerAddress())
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Server error", "error", err)
			os.Exit(1)
		}
	}()

	// Setup graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("Shutting down server...")

	// Close streaming connections first
	streamingManager.CloseAll()

	// Then shutdown the HTTP server
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("Server forced to shutdown", "error", err)
		os.Exit(1)
	}

	slog.Info("Server exited gracefully")
}
