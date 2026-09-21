# Distributed Job Processing & Queue System
A resilient, production-grade distributed background job queue built with Node.js, Redis, and PostgreSQL. This project is designed to handle asynchronous task execution with support for multi-level priority queues, dynamic rate limiting, TTL-based heartbeats, automated crash recovery via a background Watcher process, exponential backoff retries, and a Dead-Letter Queue (DLQ).

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

## 🏗️ System Architecture

```text
               API / Producer
                     │
                     ▼
             ┌───────────────┐
             │ Redis (Queue) │
             └───────┬───────┘
                     │
            ┌────────┼────────┐
            ▼        ▼        ▼
          Worker   Worker   Worker
            │        │        │
            └────────┼────────┘
                     ▼
              PostgreSQL (DB)
```
# 📁 Project Structure
```text
scaling/
├── backend/
│   ├── .env               # Database credentials & environment secrets (Git-ignored)
│   ├── .gitignore         # Ignores node_modules, .env, and local artifacts
│   ├── package.json       # Project dependencies and script configurations
│   ├── package-lock.json
│   ├── producer.js        # Job generator and API injector[cite: 2, 7]
│   ├── worker.js          # Concurrent task processor, rate limiter & retry handler[cite: 4, 8]
│   ├── watcher.js         # Fault-tolerance service for crash recovery[cite: 3, 6]
│   └── metrics.js         # Real-time dashboard performance logger[cite: 1]


  
