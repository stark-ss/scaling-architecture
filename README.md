# Distributed Job Processing & Queue System
A resilient, production-grade distributed background job queue built with Node.js, Redis, and PostgreSQL. This project is designed to handle asynchronous task execution with support for multi-level priority queues, metrics-driven auto-scaling via IPC, dynamic rate limiting, TTL-based heartbeats, automated crash recovery via a background Watcher process, exponential backoff retries, and a Dead-Letter Queue (DLQ).

## 🚀 Tech Stack
* **Runtime: Node.js**
* **Queue & Cache: Redis (ioredis)**
* **Database & Source of Truth: PostgreSQL (pg pool)**
* **Environment Configuration: dotenv**
  
## ⚙️ Core Components & Features
**1.** **Producer (producer.js)**
* injects jobs into PostgreSQL with database-level idempotency checks (ON CONFLICT DO NOTHING) to prevent duplicate payloads.
* Pushes unique job IDs into prioritized Redis lists

  
**2.** **Worker (worker.js)**
* Concurrently processes tasks using blocking Redis queue reads (BRPOP) up to a defined concurrency limit.
* Enforces a sliding-window rate limiter to throttle high-volume bursts.
* Maintains TTL-based heartbeats (hb:{jobId}) in Redis to signal active processing state.
* Manages error handling with exponential backoff retry math ($\text{delay} = 2^{\text{attempt} + 1}\text{s}$).

**3.** **Watcher (watcher.js)**
* Acts as a self-healing fault-tolerance monitor.
* Scans active tracking sets and checks TTL heartbeats; automatically re-queues or pushes timed-out/abandoned jobs to the Dead-Letter Queue (DLQ).

**4.** **Metrics Reporter (metrics.js)**
* Aggregates real-time system performance metrics, tracking queue lengths, processing throughput, success/failure counts, and average durations.

**5.** **Scaler (scaler.js)**
* Monitors global Redis queue depths and worker counts in real-time.
* Dynamically forks child worker processes up to a defined ceiling and scales them down safely using non-blocking IPC (graceful_shutdown) messages.

**6.** **Scheduler (scheduler.js)**
* Periodically evaluates the delayed sorted set (q:delayed) and automatically promotes matured tasks back into the active priority queues.

## 🏗️ System Architecture

```text
                        API / Producer
                               │
                               ▼
     ┌──────────────────────────────────────────────────┐
     │                   Redis Queues                   │
     │  (q:jobs2, q:jobs1, q:jobs0, q:delayed, q:dlq)   │
     └───────┬──────────────────────────▲───────────▲───┘
             │ (Monitor & Fork)         │           │
             ▼                          │           │
          Scaler                        │           │ (Orphan Rescue)
             │                          │           │
             │                          │           │
     ┌───────────────┐                  │           │
     │ Worker Pool   │                  │           │
     │  - Worker 1   │                  │           │          
     │  - Worker 2   │                  │           │          
     └───────┬───────┘                  │           │                
             │ (State & Persistence)    │           │          
             ▼                          │           │          
     PostgreSQL (DB)                    │           │       
     ┌───────────────┐                  │           │          
     │   Scheduler   │ ─────────────────┘           │          
     │ (Delayed Jobs)│                              │          
     └───────────────┘                              │           
     ┌───────────────┐                              │          
     │    Watcher    │ ─────────────────────────────┘          
     │(Orphan/Crash) │                                         
     └───────────────┘
```
# 📁 Project Structure
```text
scaling/
├── backend/
│   ├── .env              # Database credentials & environment secrets (Git-ignored)
│   ├── .gitignore        # Ignores node_modules, .env, and local artifacts
│   ├── package.json      # Project dependencies and script configurations
│   ├── package-lock.json
│   ├── producer.js       # Job generator and database-backed API injector
│   ├── scaler.js         # Metrics-driven autoscaling controller via IPC
│   ├── worker.js         # Concurrent task processor, atomic Lua script handler & rate limiter
│   ├── scheduler.js      # Delayed job promotion manager
│   ├── watcher.js        # Fault-tolerance service for automated crash and orphan recovery
│   └── metrics.js        # Real-time dashboard performance and throughput logger

  
