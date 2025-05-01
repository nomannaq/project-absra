ABSRA - Event Bus Abstraction Layer
<img alt="License" src="https://img.shields.io/badge/license-MIT-blue.svg">
<img alt="Go Version" src="https://img.shields.io/badge/go-1.23.2-00ADD8.svg">
<img alt="Status" src="https://img.shields.io/badge/status-beta-yellow">
ABSRA (Abstract Event Bus RESTful API) is a powerful service that provides a clean REST API abstraction over Kafka, allowing teams to maintain proper service boundaries without direct dependencies on message broker technologies.

Problem Statement
In microservices architectures, services often need to communicate asynchronously via events. However, directly coupling services to Kafka or other message brokers creates several challenges:

Tight coupling to specific message broker technology
Knowledge requirements - Teams need deep Kafka expertise
Security concerns - Direct access to Kafka requires careful ACL management
Schema evolution - Ensuring data quality and compatibility across services
Development complexity - Implementing proper consumer patterns is complex
ABSRA solves these challenges by providing a simple HTTP API that handles all the complexities of event-driven architecture.

Features
🔒 Secured API with OAuth authentication and fine-grained access control
📊 Schema validation for events with registry management
🔄 Real-time event streaming via Server-Sent Events (SSE)
📦 Technology abstraction - No Kafka client code needed in microservices
🔍 Event discoverability - Self-documenting API for event types
⚡ Low latency event delivery and processing
🛡️ Data quality enforcement through schema validation
Architecture
Installation
Prerequisites
Go 1.23.2 or higher
Kafka cluster
Docker & Docker Compose (for local development)
Getting Started
Clone the repository:
Configure your environment:
Start the dependencies using Docker Compose:
Run the service:
Usage
Authentication
Obtain an authentication token:

Define Event Types with Schemas
Register a new event type with its JSON schema:

'
Publish Events
Publish an event (ABSRA validates it against the schema):

Consume Events via Streaming
Subscribe to events using Server-Sent Events (SSE):

API Reference
Endpoint	Method	Description
/api/v1/auth/token	POST	Get authentication token
/api/v1/event-types	GET	List all registered event types
/api/v1/event-types	POST	Register a new event type with schema
/api/v1/event-types/:type	GET	Get schema for specific event type
/api/v1/events/:type	POST	Publish an event of specified type
/api/v1/streams	GET	Subscribe to events via SSE
/health	GET	Service health check
Configuration
ABSRA is configured via environment variables, which can be provided in a .env file:

Variable	Description	Default
SERVER_ADDRESS	Server host address	localhost
SERVER_PORT	Server port	8080
SERVER_MODE	Gin mode (debug, release, test)	debug
KAFKA_BROKERS	Comma-separated Kafka brokers	localhost:9092
KAFKA_CONSUMER_GROUP	Consumer group ID	event-bus-api
KAFKA_TOPIC_ACL	Topic access controls	``
AUTH_SECRET	JWT signing secret	required
AUTH_TOKEN_EXPIRATION_HOURS	Token expiration time in hours	24
SCHEMA_REGISTRY_ENABLED	Enable schema validation	true
SCHEMA_REGISTRY_TYPE	Registry type (local, confluent)	local
SCHEMA_STORAGE_PATH	Local schema storage path	./schemas
STREAMING_BUFFER_SIZE	Event buffer size per consumer	100
STREAMING_KEEPALIVE_INTERVAL	Keepalive interval	30s
Benefits for Microservices Teams
Focus on Domain Logic: Teams can focus on their business logic rather than messaging infrastructure
Simplified Development: No Kafka client libraries needed in every service
Consistency: Enforced data structures and formats via schema validation
Security: Fine-grained access control without direct Kafka access
Technology Independence: Underlying message broker can be changed without affecting services
Cross-Language Support: Any service that can make HTTP requests can use the event bus
Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

Fork the repository
Create your feature branch (git checkout -b feature/amazing-feature)
Commit your changes (git commit -m 'Add some amazing feature')
Push to the branch (git push origin feature/amazing-feature)
Open a Pull Request
License
This project is licensed under the MIT License - see the LICENSE file for details.

Built with ❤️ by Nouman Qureshi

