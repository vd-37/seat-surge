# Distributed Ticket/Seat Booking System — Project Plan

> **Stack:** Java Spring Boot · PostgreSQL · Redis · Apache Kafka  
> **Goal:** Build a production-realistic distributed booking system that demonstrates concurrency control, distributed architecture, event-driven design, and observable scaling — with real numbers to put on your resume.

---

## Why This Project

| Criteria | Rating | Reason |
|---|---|---|
| Distributed architecture depth | ●●●●● | Saga, outbox, CDC, CQRS |
| Concurrent request handling | ●●●●● | "10k users, 50 seats" is the hardest concurrency problem |
| Concurrency internals | ●●●●● | Locking, races, overselling — you *must* solve these |
| Scaling story | ●●●●● | Clear bottlenecks to find, fix, and measure |
| System behavior depth | ●●●●● | Lock waits, consumer lag, pool saturation |
| Resume numbers | ●●●●● | Oversell=0, p99 latency, req/sec — clean and defensible |
| Free to develop & load-test | ✅ | Docker Compose + k6 + Grafana, all local and free |

---

## Learning Strategy

**Do NOT learn everything upfront. Do NOT build blindly either.**

The right approach is: **learn the minimum viable concept → build the specific thing you need → hit a real problem → go deeper on that specific thing → move on.**

```
Understand what it does and why  (1–2 hours)
          ↓
Build the specific thing you need right now  (1–3 days)
          ↓
Hit a real problem, go deeper on that specific thing  (hours, not days)
          ↓
Move to the next concept
```

### Why this works
- You never learn Redis fully upfront. You learn *"Redis can store a key with an expiry"* when you need seat holds.
- Later you learn *"Redis Lua scripts for atomic operations"* when you hit a race condition.
- The second lesson sticks because you **felt the pain** that motivated it.
- Every bug you hit becomes an interview story. Document them.

---

## Project Phases

### Phase 0 — Foundation
**Duration:** ~1 week  
**Goal:** Learn just enough to not be lost. Build one working end-to-end flow.

#### What to learn
| Technology | What to learn (and nothing more) |
|---|---|
| Spring Boot | `@RestController`, `@Service`, `@Repository`, Spring Data JPA, `application.yml` wiring |
| PostgreSQL | What a transaction is, `COMMIT`/`ROLLBACK`, what happens when two queries hit the same row simultaneously — run this by hand in psql |
| Docker Compose | Get Postgres + pgAdmin running locally in one `docker-compose.yml` |

#### What to build
- `POST /events` — create an event (concert/match) with a name, date, and total seat count
- `GET /events/{id}` — return event details
- `POST /events/{id}/seats` — seed the seat rows in DB

#### Deliverable
A working Spring Boot app connected to Postgres via Docker. You can create an event and retrieve it. That's it.

---

### Phase 1 — Core Booking with Concurrency
**Duration:** 2–3 weeks  
**Goal:** Build the booking flow, then intentionally break it, then fix it properly.

#### What to build
- `BookingService` — attempt to book a seat
- `GET /events/{id}/seats` — list seats with status (AVAILABLE / HELD / BOOKED)
- `POST /bookings` — book a seat for a user

#### The learning path (do it in this order)

**Step 1 — Build the naive version first**
```
if (seat.isAvailable()) {
    seat.setStatus(BOOKED);   // <-- race condition lives here
    save(seat);
}
```
Ship it. It works in single-user testing.

**Step 2 — Write a concurrency test**  
Spin up 20 threads all trying to book the last seat simultaneously. Watch it oversell.  
This failure is the best teacher you'll have in this entire project.

**Step 3 — Fix it in sequence, understanding each tradeoff**

| Fix | How | When to use |
|---|---|---|
| Pessimistic locking | `SELECT FOR UPDATE` in JPA (`@Lock(PESSIMISTIC_WRITE)`) | High-contention rows (last few seats) |
| Optimistic locking | `@Version` field on entity — fails with exception on conflict | Low-contention, retry-friendly flows |
| DB constraint | `UNIQUE` constraint as last line of defence | Always — belt and braces |

#### Deliverable
A booking endpoint that handles 500 concurrent requests for the last seat and oversells **exactly zero times**.  
Measure this with a basic k6 script:
```js
// k6 script skeleton
import http from 'k6/http';
export const options = { vus: 500, duration: '10s' };
export default function () {
  http.post('http://localhost:8080/bookings', JSON.stringify({ eventId: 1, seatId: 42 }));
}
```

---

### Phase 2 — Seat Hold + Redis
**Duration:** 1–2 weeks  
**Goal:** Solve the "held but never paid" problem. Introduce Redis.

#### The business problem
Real booking systems let you hold a seat for 10 minutes while you enter payment details. If you don't pay, the hold expires and the seat returns to AVAILABLE automatically.

#### What to build
| Endpoint | Description |
|---|---|
| `POST /bookings/hold` | Reserve seat temporarily (10-min TTL) |
| `POST /bookings/confirm` | Finalise after payment |
| `DELETE /bookings/hold/{holdId}` | Explicit release |

#### Why Redis (not Postgres) for holds
Postgres can do TTL expiry but it requires a polling job. Redis does it natively — store a hold as a key with a 10-minute expiry and it vanishes automatically. No cleanup job needed.

#### Concepts you'll learn (in order, as you hit them)
1. Redis basic operations — `SET key value EX 600`
2. Redis data structures — strings vs hashes for hold metadata
3. **Lua scripts for atomic check-and-set** — you will hit a race condition between "check if hold exists" and "create hold" being two separate Redis calls. A Lua script makes them atomic. This is the key learning moment of this phase.

#### Add to Docker Compose
```yaml
redis:
  image: redis:7-alpine
  ports:
    - "6379:6379"
```

#### Deliverable
- Holds expire automatically after 10 minutes with zero leftover locked seats
- Load test showing no seat is permanently locked by an abandoned hold

---

### Phase 3 — Kafka + Async Event Flow
**Duration:** ~2 weeks  
**Goal:** Decouple confirmation and notifications. Move from synchronous to event-driven.

#### The problem with synchronous flows
Right now every HTTP request waits for the entire booking pipeline to complete synchronously. This breaks under load and creates tight coupling between services.

#### What to build
| Component | Description |
|---|---|
| Kafka topics | `booking.confirmed`, `booking.failed`, `booking.held` |
| `NotificationService` | Listens to Kafka, "sends" email/SMS — fake it with a log statement |
| `AuditService` | Listens to all booking events, writes to an append-only `booking_audit` table |

The `AuditService` here is your **audit ledger idea** — it fits naturally as a downstream Kafka consumer. You get both systems in one project.

#### Concepts you'll learn (in order)
1. Kafka producer in Spring — `KafkaTemplate.send()`
2. Kafka consumer in Spring — `@KafkaListener`
3. Consumer groups — why multiple instances of `NotificationService` don't double-send
4. Offset commits — what happens when a consumer crashes mid-processing
5. **Idempotency** — what if the same event is delivered twice? Your consumer must handle this safely (check if already processed before acting)

#### Add to Docker Compose
```yaml
zookeeper:
  image: confluentinc/cp-zookeeper:7.5.0
  environment:
    ZOOKEEPER_CLIENT_PORT: 2181

kafka:
  image: confluentinc/cp-kafka:7.5.0
  depends_on: [zookeeper]
  ports:
    - "9092:9092"
  environment:
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

#### Deliverable
- Booking confirmation is fully async — HTTP returns fast, notification happens in background
- Kill the `NotificationService` consumer mid-run, restart it, verify:
  - No events were lost
  - No events were double-processed

---

### Phase 4 — Saga Pattern + Outbox
**Duration:** ~2 weeks  
**Goal:** Handle multi-step transactions with partial failure and compensation.

#### What is the Saga Pattern?
A saga manages a transaction that spans multiple steps where each step can fail independently. You can't do a traditional DB transaction across service boundaries — saga is the solution.

**Your booking "transaction" is 3 steps:**
```
1. Reserve seat    →  marks seat as HELD
2. Charge payment  →  calls PaymentService
3. Confirm booking →  marks seat as CONFIRMED, emits event

If step 2 fails:
  ← compensate step 1: release the seat hold
```

#### Two saga flavours
| Type | How it works | When to use |
|---|---|---|
| **Choreography** | Services react to Kafka events, no central brain | Simpler flows, highly decoupled |
| **Orchestration** | One `BookingOrchestrator` directs each step | Complex flows, easier to debug — **use this** |

**Use orchestration** for this project — it makes distributed coordination explicit and traceable, which is exactly what you want to explain in an interview.

#### What to build
- `PaymentService` — fake service that randomly succeeds or fails (configurable failure rate)
- `BookingOrchestrator` — coordinates: hold → charge → confirm, with compensation on failure
- **Outbox pattern** — instead of writing to DB and publishing to Kafka separately (dangerous dual-write), write the Kafka event to an `outbox` DB table in the **same transaction** as your booking record. A separate publisher reads and sends it.

#### Why the Outbox Pattern matters
```
// DANGEROUS — dual-write problem
bookingRepository.save(booking);  // succeeds
kafkaTemplate.send(event);         // crashes here → DB and Kafka out of sync forever

// SAFE — outbox pattern
// In one transaction:
bookingRepository.save(booking);
outboxRepository.save(event);      // same transaction, same commit
// Separate publisher:
// reads outbox table → sends to Kafka → deletes from outbox
```

#### Deliverable
- Run 1,000 bookings with payment randomly failing 30% of the time
- Zero seats permanently stuck in `HELD` state
- Zero lost or duplicate Kafka events
- Full audit trail in `booking_audit` table for every outcome

---

### Phase 5 — Observability + Numbers
**Duration:** ~1 week  
**Goal:** Instrument everything and produce the numbers that go on your resume.

#### Add to Docker Compose
```yaml
prometheus:
  image: prom/prometheus
  volumes:
    - ./prometheus.yml:/etc/prometheus/prometheus.yml
  ports:
    - "9090:9090"

grafana:
  image: grafana/grafana
  ports:
    - "3000:3000"
```

Spring Boot Actuator + Micrometer is already built in — just enable it:
```yaml
# application.yml
management:
  endpoints.web.exposure.include: "*"
  metrics.export.prometheus.enabled: true
```

#### Metrics to track and dashboard in Grafana
| Metric | What it tells you |
|---|---|
| HTTP request throughput (req/sec) | Raw capacity |
| p50 / p95 / p99 latency | Tail latency under load |
| Booking success vs failure rate | Correctness under concurrency |
| Oversell count | Should always be zero |
| Kafka consumer lag | Are consumers keeping up? |
| DB connection pool active/waiting | DB is the bottleneck? |
| Redis hit rate | Cache effectiveness |
| Lock wait time | Contention hotspots |

#### The before/after story (your resume narrative)
Run your k6 load test at each phase and record numbers:

| Checkpoint | Throughput | p99 Latency | Oversell |
|---|---|---|---|
| After Phase 1 (naive) | ~200 req/sec | 640ms | ~12% |
| After Phase 1 (pessimistic lock) | ~800 req/sec | 210ms | 0% |
| After Phase 2 (Redis holds) | ~1,800 req/sec | 95ms | 0% |
| After Phase 3 (async Kafka) | ~3,200 req/sec | 85ms | 0% |

*These are illustrative — your actual numbers will vary. The point is you have a story with real measurements.*

#### Resume bullet example
> Designed and implemented a distributed ticket booking system handling **3,200 concurrent requests/sec** with **zero overselling** across 10,000 simultaneous users; reduced p99 latency from **640ms to 85ms** by replacing pessimistic DB locking with Redis-based distributed seat holds; guaranteed event consistency via the **outbox pattern** with Kafka, achieving zero message loss under 30% simulated payment failure rate.

---

### Phase 6 — Frontend (Optional)
**Duration:** ~2 weeks  
**Goal:** Make it visually demonstrable in a screen share within seconds.

#### What to build (React)
- Event listing page
- **Seat map** — grid of seats colour-coded: green (available) / yellow (held) / red (booked)
- Real-time updates via **WebSocket or SSE** — open two browser tabs, book in one, watch the other update live
- Simple checkout flow: select seat → hold → pay (fake) → confirm

#### Why bother
A 30-second demo where you open two browser windows and show a seat disappearing in real time is worth more in an interview than five minutes of explaining the architecture. It makes the concurrency story *visible*.

---

### Phase 7 — Spring AI Anomaly Explainer (Optional but Recommended)
**Duration:** ~1–2 weeks  
**Goal:** Add an AI-powered incident explanation layer on top of your existing audit ledger — no new infrastructure needed.

#### The business problem
Your monitoring fires an alert that booking failure rate spiked. An on-call engineer currently has to dig through raw logs and audit records manually to figure out why. This phase automates that first-pass diagnosis.

#### What it does
An internal endpoint accepts a time window, pulls booking events from your audit ledger, and returns a plain-English explanation of what happened:

```
"Between 14:00 and 14:15, 340 bookings failed.
87% share error code SEAT_HOLD_EXPIRED, concentrated
on Event ID 42. Likely cause: hold TTL too short for
the checkout flow during peak traffic."
```

No manual log trawling. No dashboards to cross-reference. One API call.

#### Why this is legitimate (not bolted-on AI)
- It uses infrastructure you already built — Kafka, audit ledger, Prometheus metrics
- Log summarisation and incident explanation is one of the most practical real-world AI applications in backend systems right now — actual companies are building this
- It adds a layer on top of existing work without adding new scope
- The interesting part is the data pipeline feeding it, which you already have

#### What to build

**New endpoint:**
```
GET /admin/anomalies/explain?from=2024-01-01T14:00&to=2024-01-01T14:15
```

**`AnomalyExplainerService` flow:**
```
1. Query booking_audit table for the given time window
2. Aggregate: group by error_code, event_id, user_id patterns
3. Pull Prometheus metrics snapshot for the same window (optional enrichment)
4. Build a structured prompt with the aggregated data
5. Call Spring AI → get plain-English explanation back as a typed Java object
6. Return explanation + raw stats to caller
```

#### Spring AI implementation

**Dependency:**
```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-openai-spring-boot-starter</artifactId>
</dependency>
```

**Structured output** — don't return raw text, return a typed object:
```java
public record AnomalyReport(
    String summary,
    String likelyCause,
    String recommendedAction,
    List<String> affectedEventIds,
    String severity   // LOW / MEDIUM / HIGH
) {}
```

**The prompt pattern:**
```java
String prompt = """
    You are a backend systems analyst. Analyse the following booking
    system audit data and explain what went wrong in plain English.
    Respond ONLY as valid JSON matching this schema: %s

    Audit data for window %s to %s:
    - Total bookings attempted: %d
    - Failed: %d (%.1f%%)
    - Error breakdown: %s
    - Top affected events: %s
    - Avg hold duration before failure: %s
    """.formatted(schema, from, to, total, failed, failRate, errors, events, avgHold);
```

This is a **RAG-lite pattern** — retrieve relevant audit records, inject them into context, ask for a structured summary. No vector DB needed.

#### Key Spring AI concepts you'll learn
| Concept | What it is | Where you use it |
|---|---|---|
| `ChatClient` | Spring AI's main abstraction for LLM calls | `AnomalyExplainerService` |
| Structured output | Getting typed Java objects back instead of raw text | `AnomalyReport` record |
| Prompt templating | `PromptTemplate` with variables | Building the audit data prompt |
| Model configuration | Temperature, max tokens, model selection | Tuning for consistent JSON output |

#### Practical notes
- Use **OpenAI** (`gpt-4o-mini`) or **Anthropic Claude** (`claude-haiku`) — both have free/cheap tiers sufficient for dev and testing, costs near zero for a personal project
- Set temperature to `0` or `0.1` for structured output — you want deterministic JSON, not creative writing
- The model string and API key go in `application.yml`, never hardcoded
- Test with intentionally broken scenarios: inject 500 fake `SEAT_HOLD_EXPIRED` events and verify the explanation correctly identifies the pattern

#### Interview angle
*"I added an AI-powered anomaly explainer that reads the audit ledger and generates plain-English incident reports. It uses Spring AI's structured output to return typed Java objects rather than raw text, so the response is directly usable by downstream services. The interesting engineering challenge wasn't the AI call — it was designing the aggregation query and prompt structure so the model consistently identifies root causes rather than just describing symptoms."*

That last sentence is what separates a thoughtful engineer from someone who just called an API.

#### Deliverable
- `GET /admin/anomalies/explain` returns a structured `AnomalyReport` for any time window
- Test with three scenarios: hold expiry spike, payment failure spike, and normal traffic (should return low severity, no anomaly)
- The explanation correctly identifies root cause in at least 2 of 3 test scenarios

---

## Full Timeline

| Phase | Focus | Duration |
|---|---|---|
| 0 | Spring Boot + Postgres basics | 1 week |
| 1 | Core booking + concurrency (locking) | 2–3 weeks |
| 2 | Seat holds + Redis | 1–2 weeks |
| 3 | Kafka async flow + Audit ledger | 2 weeks |
| 4 | Saga orchestration + Outbox | 2 weeks |
| 5 | Observability + load testing + numbers | 1 week |
| 6 | Frontend (optional) | 2 weeks |
| 7 | Spring AI anomaly explainer (optional) | 1–2 weeks |
| **Total** | | **~3.5 months** |

---

## Tech Stack Summary

| Layer | Technology | Purpose |
|---|---|---|
| API | Spring Boot 3.x | REST endpoints, service layer |
| ORM | Spring Data JPA + Hibernate | DB access, optimistic/pessimistic locking |
| Database | PostgreSQL 15 | Source of truth, outbox table |
| Cache / Holds | Redis 7 | TTL-based seat holds, distributed locking |
| Messaging | Apache Kafka | Async event flow, audit trail |
| Observability | Micrometer + Prometheus + Grafana | Metrics, dashboards, load test results |
| Load Testing | k6 | Concurrent user simulation |
| Local Infra | Docker Compose | Entire stack runs free on your laptop |
| Frontend (opt.) | React | Seat map, real-time updates via WebSocket |
| AI (opt.) | Spring AI + OpenAI / Claude | Anomaly explanation, structured LLM output |

---

## Key Concepts Checklist

Track these as you learn them — each one is a potential interview talking point:

### Concurrency & Locking
- [ ] Pessimistic locking (`SELECT FOR UPDATE`)
- [ ] Optimistic locking (`@Version` in JPA)
- [ ] Database constraints as safety net
- [ ] Redis atomic operations (Lua scripts)
- [ ] Distributed locks

### Distributed Architecture
- [ ] Saga pattern (orchestration vs choreography)
- [ ] Compensating transactions
- [ ] Outbox pattern (solving dual-write)
- [ ] Idempotency keys
- [ ] At-least-once vs exactly-once delivery

### Kafka
- [ ] Topics, partitions, consumer groups
- [ ] Offset management and commits
- [ ] Dead-letter queues
- [ ] Idempotent consumers
- [ ] Consumer lag monitoring

### Observability
- [ ] Micrometer metrics in Spring Boot
- [ ] Prometheus scraping
- [ ] Grafana dashboards
- [ ] p50/p95/p99 latency interpretation
- [ ] Identifying bottlenecks from metrics

### Spring AI (Optional)
- [ ] `ChatClient` and prompt templating
- [ ] Structured output (typed Java objects from LLM)
- [ ] RAG-lite pattern (inject DB data into prompt context)
- [ ] Temperature and token tuning for deterministic output
- [ ] Prompt engineering for root-cause analysis

---

## Interview Story Template

For every major bug or design decision you encounter, note it down:

```
Problem:   What broke and when
Context:   What I was trying to do
Root cause: Why it broke (the actual distributed systems reason)
Fix:       What I changed and why
Tradeoff:  What I gave up to get this fix
Metric:    How I confirmed it was fixed (number)
```

These notes become your interview answers. The best answers come from things that actually broke on you.

---

## Resources (learn as you need them, not upfront)

| Topic | Resource |
|---|---|
| Spring Boot basics | [spring.io/guides](https://spring.io/guides) — "Building a RESTful Web Service" |
| JPA locking | Vlad Mihalcea's blog — search "optimistic locking JPA" |
| Redis patterns | Redis documentation — data types, Lua scripting |
| Kafka in Spring | [spring.io/projects/spring-kafka](https://spring.io/projects/spring-kafka) |
| Saga pattern | microservices.io/patterns/data/saga.html |
| Outbox pattern | microservices.io/patterns/data/transactional-outbox.html |
| k6 load testing | [k6.io/docs](https://k6.io/docs) |
| Micrometer + Prometheus | [micrometer.io](https://micrometer.io) |
| Spring AI | [docs.spring.io/spring-ai](https://docs.spring.io/spring-ai/reference/) |

---

*Last updated: July 2026*
