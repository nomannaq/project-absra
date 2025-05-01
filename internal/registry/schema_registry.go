package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/nomannaq/absra/internal/config"
	"github.com/xeipuuv/gojsonschema"
)

// SchemaRegistry defines the interface for schema validation
type SchemaRegistry interface {
	RegisterSchema(eventType string, schema string) error
	ValidateEvent(eventType string, eventData []byte) error
	GetSchema(eventType string) (string, error)
	ListEventTypes() ([]string, error)
}

// LocalSchemaRegistry implements SchemaRegistry interface with local storage
type LocalSchemaRegistry struct {
	storagePath string
	schemas     map[string]*gojsonschema.Schema
	mu          sync.RWMutex
}

// NewSchemaRegistry creates a schema registry based on configuration
func NewSchemaRegistry(cfg config.SchemaRegistryConfig) (SchemaRegistry, error) {
	if !cfg.Enabled {
		return NewNoOpRegistry(), nil
	}

	switch cfg.Type {
	case "local":
		return NewLocalSchemaRegistry(cfg.StoragePath)
	case "confluent":
		return nil, errors.New("confluent schema registry not implemented")
	default:
		return nil, fmt.Errorf("unknown schema registry type: %s", cfg.Type)
	}
}

// NewLocalSchemaRegistry creates a new local schema registry
func NewLocalSchemaRegistry(storagePath string) (SchemaRegistry, error) {
	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create schema storage directory: %w", err)
	}

	registry := &LocalSchemaRegistry{
		storagePath: storagePath,
		schemas:     make(map[string]*gojsonschema.Schema),
	}

	// Load existing schemas
	if err := registry.loadSchemas(); err != nil {
		return nil, fmt.Errorf("failed to load existing schemas: %w", err)
	}

	slog.Info("Local schema registry initialized",
		"storage_path", storagePath,
		"schemas_loaded", len(registry.schemas))

	return registry, nil
}

// RegisterSchema registers a new schema for an event type
func (r *LocalSchemaRegistry) RegisterSchema(eventType string, schemaJSON string) error {
	// Validate the schema itself
	schemaLoader := gojsonschema.NewStringLoader(schemaJSON)
	schema, err := gojsonschema.NewSchema(schemaLoader)
	if err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	// Store the schema
	r.mu.Lock()
	defer r.mu.Unlock()

	r.schemas[eventType] = schema

	// Write to disk
	schemaPath := filepath.Join(r.storagePath, eventType+".json")
	if err := os.WriteFile(schemaPath, []byte(schemaJSON), 0644); err != nil {
		return fmt.Errorf("failed to write schema file: %w", err)
	}

	slog.Info("Schema registered", "event_type", eventType)
	return nil
}

// ValidateEvent validates event data against the registered schema
func (r *LocalSchemaRegistry) ValidateEvent(eventType string, eventData []byte) error {
	r.mu.RLock()
	schema, exists := r.schemas[eventType]
	r.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no schema registered for event type: %s", eventType)
	}

	// Parse the event data to ensure it's valid JSON before validation
	var jsonData interface{}
	if err := json.Unmarshal(eventData, &jsonData); err != nil {
		return fmt.Errorf("invalid JSON in event data: %w", err)
	}

	documentLoader := gojsonschema.NewGoLoader(jsonData)
	result, err := schema.Validate(documentLoader)
	if err != nil {
		return fmt.Errorf("validation error: %w", err)
	}

	if !result.Valid() {
		var errMsg string
		for i, desc := range result.Errors() {
			if i > 0 {
				errMsg += ", "
			}
			errMsg += desc.String()
		}
		return fmt.Errorf("schema validation failed: %s", errMsg)
	}

	return nil
}

// GetSchema retrieves the schema for an event type
func (r *LocalSchemaRegistry) GetSchema(eventType string) (string, error) {
	schemaPath := filepath.Join(r.storagePath, eventType+".json")
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return "", fmt.Errorf("failed to read schema file: %w", err)
	}
	return string(data), nil
}

// ListEventTypes returns all registered event types
func (r *LocalSchemaRegistry) ListEventTypes() ([]string, error) {
	files, err := os.ReadDir(r.storagePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema directory: %w", err)
	}

	var eventTypes []string
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".json" {
			eventTypes = append(eventTypes, file.Name()[:len(file.Name())-5]) // Remove .json extension
		}
	}

	return eventTypes, nil
}

// loadSchemas loads schemas from disk
func (r *LocalSchemaRegistry) loadSchemas() error {
	files, err := os.ReadDir(r.storagePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist yet, which is fine
		}
		return err
	}

	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".json" {
			eventType := file.Name()[:len(file.Name())-5] // Remove .json extension
			schemaPath := filepath.Join(r.storagePath, file.Name())

			schemaData, err := os.ReadFile(schemaPath)
			if err != nil {
				return fmt.Errorf("failed to read schema file %s: %w", file.Name(), err)
			}

			schemaLoader := gojsonschema.NewStringLoader(string(schemaData))
			schema, err := gojsonschema.NewSchema(schemaLoader)
			if err != nil {
				return fmt.Errorf("invalid schema in file %s: %w", file.Name(), err)
			}

			r.schemas[eventType] = schema
			slog.Info("Loaded schema", "event_type", eventType)
		}
	}

	return nil
}

// NoOpRegistry provides a no-operation implementation of SchemaRegistry
type NoOpRegistry struct{}

// NewNoOpRegistry creates a new no-operation schema registry
func NewNoOpRegistry() SchemaRegistry {
	return &NoOpRegistry{}
}

// RegisterSchema is a no-op implementation
func (r *NoOpRegistry) RegisterSchema(eventType string, schema string) error {
	slog.Debug("NoOp registry: RegisterSchema called", "event_type", eventType)
	return nil
}

// ValidateEvent is a no-op implementation that always passes validation
func (r *NoOpRegistry) ValidateEvent(eventType string, eventData []byte) error {
	slog.Debug("NoOp registry: ValidateEvent called", "event_type", eventType)
	return nil
}

// GetSchema is a no-op implementation
func (r *NoOpRegistry) GetSchema(eventType string) (string, error) {
	return "", errors.New("schema registry is disabled")
}

// ListEventTypes is a no-op implementation
func (r *NoOpRegistry) ListEventTypes() ([]string, error) {
	return []string{}, nil
}
