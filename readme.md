# ABSRA - Event Bus Abstraction Layer

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Go Version](https://img.shields.io/badge/go-1.23.2-00ADD8.svg)
![Status](https://img.shields.io/badge/status-beta-yellow)

**ABSRA** (Abstract Event Bus RESTful API) provides a clean HTTP API on top of Apache Kafka.  
It lets microservices publish and consume events without embedding Kafka client code—  
handling schema validation, access control, and real‑time streaming for you.

## Problem Statement

Microservices often communicate via events, but direct Kafka integration brings:

- Tight coupling to a specific broker technology  
- Steep learning curve and operational complexity  
- Security risks from granting broker access  
- Schema evolution challenges and data inconsistencies  
- Boilerplate consumer/producer code in every service  

**ABSRA** solves these by exposing a simple, secure REST interface that:

1. Validates events against JSON schemas  
2. Enforces topic‑level access control  
3. Publishes to Kafka under the hood  
4. Streams events to clients via Server‑Sent Events (SSE)  

## Features

- 🔒 JWT‑based authentication & per‑topic ACL  
- 📐 JSON Schema registry and validation  
- 🔄 Real‑time SSE subscription for event streams  
- ⚙️ Zero‑Kafka‑client dependency in your services  
- 🔍 Discoverable event types & automatic topic creation  

## Architecture

┌────────────┐     ┌────────────┐     ┌────────────┐  
│            │     │            │     │            │  
│ Service A  │     │ Service B  │     │ Service C  │  
│            │     │            │     │            │  
└─────┬──────┘     └─────┬──────┘     └─────┬──────┘  
      │                  │                  │         
      │   HTTP/REST API  │                  │         
      ▼                  ▼                  ▼         
┌─────────────────────────────────────────────────┐  
│                                                 │  
│                    ABSRA                        │  
│                                                 │  
├─────────────────────────────────────────────────┤  
│    Schema Registry    │     Auth Service        │  
├─────────────────────────────────────────────────┤  
│    Event Bus          │     Stream Manager      │  
├─────────────────────────────────────────────────┤  
│                  Kafka Client                   │  
│                                                 │  
└─────────────────┬───────────────────────────────┘  
                  │                                  
                  ▼                                  
        ┌───────────────────┐                        
        │                   │                        
        │      Kafka        │                        
        │                   │                        
        └───────────────────┘     