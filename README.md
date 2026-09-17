# stripe_metronome

An event-driven billing platform: it meters usage in **Metronome** (the `metering`
service), manages customers and billing cycles in the `billing` service, and
invoices + collects payment through **Stripe** (the `invoicing` service). Usage
flows over a Kafka event bus; provider calls go to local Metronome/Stripe
stand-ins (`fakemetronome` / `fakestripe`).

It's a working demo — single box, in-memory state (see *Production scaling*) — not
a production system.

## Services

Each service is its own binary under `cmd/`. The **billing pipeline** flows over
the event bus (`internal/events`); **reads and provider calls** are synchronous
HTTP (e.g. `invoicing → metering`, `metering → fakemetronome`, `controlplane →
billing` for customer validation).

| Service     | Binary            | Default addr | Responsibility |
|-------------|-------------------|--------------|----------------|
| `billing`   | `cmd/billing`     | `:8080`      | Business/orchestration. Owns the **customer registry** (`/v1/customers` — unique customers + provider-id mapping), the billing-cycle boundary (`billing_cycle.ended`), and payment tracking. Not in the usage-data path; does not manage generators; does **not** assemble invoices. |
| `invoicing` | `cmd/invoicing`   | `:8081`      | The Stripe integration. Owns the invoice **and flat-fee subscriptions** (`subscription.activated`/`cancelled`). On `billing_cycle.ended`, queries Metronome for usage, creates the invoice (usage items + active subscription fees), maps the payment outcome onto the bus. Calls `STRIPE_BASE_URL` + `METERING_SERVICE_URL`. |
| `metering` | `cmd/metering`   | `:8082`      | Metering only (invoicing disabled). Ingests usage, determines lateness, and reads mid-period priced usage from Metronome's **list-costs** endpoint. Calls `METRONOME_BASE_URL`. |
| `fakemetronome` | `cmd/fakemetronome` | `:8083`  | Local stand-in for the Metronome API: `POST /ingest` (dedup + batching) and `GET /v1/customers/{id}/costs` (list-costs) mirror real endpoints. `close-period` is a **demo shim** (real Metronome has no such call). Swap via `METRONOME_BASE_URL`. |
| `fakestripe` | `cmd/fakestripe`     | `:8084`  | Local stand-in for the Stripe API (invoice items, invoice create/finalize, payment outcome). Swap for real Stripe test mode via `STRIPE_BASE_URL`. |
| `controlplane` | `cmd/controlplane` | `:8085`  | Control plane for *generator processes* (`/v1/generators`). Two types: **usage** (ticks → publishes `usage.ingested`) and **subscription** (flat fee; start/stop → `subscription.activated`/`cancelled`). Frontend calls it directly. |
| `eventfeed` | `cmd/eventfeed`   | `:8086`  | **Read-only** live view of the bus. Tails every event topic into a bounded in-memory ring (last ~500) and serves it at `GET /v1/events` for the frontend's **Live events** panel. Consumes only — no produce path — so it is safe to expose publicly (this is the recruiter-facing way to watch events, in place of the Console). |

Override a listen address with `<SERVICE>_HTTP_ADDR` (e.g. `BILLING_HTTP_ADDR`).

## Event flow

Services publish/subscribe to domain events (each event type is a Kafka topic).
Reads (mid-cycle preview, invoice history) are synchronous HTTP; the bus carries
the billing pipeline. `metering` and `invoicing` call the provider stubs
(`fakemetronome` / `fakestripe`) over HTTP; those mirror the real Metronome /
Stripe APIs.

```
control     frontend ─HTTP─▶ controlplane (/v1/generators…)      provision / start / stop
(HTTP)      frontend ─HTTP─▶ billing (/v1/customers, …/close)    customers, cycle boundary

usage       controlplane usage process ─usage.ingested─▶ [bus] ─▶ metering
(bus)                                                              │  late = occurred_at < period_start
                                                                   └─(batch)─▶ Metronome  (meter; invoicing off)

subs        controlplane subscription process ─subscription.activated/cancelled─▶ [bus] ─▶ invoicing ─▶ Stripe

mid-cycle   frontend ─HTTP─▶ invoicing (/v1/customers/{id}/upcoming-invoice)
(read)                        ├─ list-costs ◀── metering ◀── Metronome   (priced usage, grouped)
                              └─ subscriptions ◀── Stripe

cycle end   frontend ─▶ billing (…/close) ─billing_cycle.ended─▶ [bus] ─┬─▶ invoicing
                                                                        └─▶ metering  (records period boundary)
            invoicing ── close-period (shim) ──▶ Metronome              (finalized priced usage)
            invoicing ── invoice items + invoice ─▶ Stripe              (+ active subscriptions)
            invoicing ─invoice.finalized, payment.succeeded/failed─▶ [bus] ─▶ billing
```

Notes:
- **Lateness** is determined by `metering` (event `occurred_at` vs the period
  start it learns from `billing_cycle.ended`), stamped as a `late` dimension, and
  billed as its own line. The source never marks lateness — see *Late-arriving usage*.
- **`billing_cycle.ended` has two consumers**: `invoicing` (to invoice) and
  `metering` (to record the new period boundary).
- **`close-period` is a demo shim** (real Metronome has no such call) — see
  *Metronome API fidelity*.

Event types live in `internal/events/event.go`, each doubling as its Kafka topic
name. Two `Bus` implementations sit behind one interface:

- `InMemoryBus` (`bus.go`) — single-process local dev / tests (`BUS=memory`, default).
- `KafkaBus` (`kafka.go`, franz-go) — cross-process via a Kafka-API broker
  (`BUS=kafka`, `KAFKA_SEEDS=...`). The Docker stack runs **Redpanda** as the
  broker. Two ways to watch messages propagate:
  - **Live events** panel in the web app — a read-only tail served by the
    `eventfeed` service (`GET /v1/events`), safe to expose publicly.
  - **Redpanda Console** — a fuller UI, but its buttons can create/delete topics,
    so it is bound to **localhost only** (reach it via SSM port forwarding when
    deployed; see the Terraform `console_tunnel` output).

## Layout

```
cmd/            service entrypoints (one binary each)
internal/
  app/          shared HTTP run loop (graceful shutdown, /healthz)
  config/       env-based configuration
  events/       event types + bus contract, in-memory + Kafka implementations
  billing/      billing orchestration service (usage ingest, cycle boundary)
  invoicing/    Stripe invoicing integration
  metering/    Metronome metering integration
  fakemetronome/ local stand-in for the Metronome API
  fakestripe/   local stand-in for the Stripe API
  controlplane/ control plane for usage/subscription generator processes
web/            React + Vite + TypeScript frontend (usage dashboard)
infra/terraform/ provisions the demo EC2 instance (t4g.small)
```

## Getting started

Backend:

```sh
make build          # build all service binaries into ./bin
make run-billing    # or run-invoicing / run-metering / run-controlplane / run-fake*
make vet            # go vet ./...
```

Frontend:

```sh
make web-install    # npm install (in ./web)
make web-dev        # vite dev server (proxies /v1 to billing; full routing needs the Docker stack's nginx)
```

## Run the full stack (Docker Compose)

Brings up Redpanda + Console + all services (on the Kafka bus) + the web app:

```sh
docker compose up --build
```

Then open:

- **App**: <http://localhost/> — includes a **Live events** panel (read-only tail
  of the bus, served by `eventfeed`).
- **Redpanda Console** (fuller UI): <http://localhost:8080> — bound to localhost
  only. On the deployed instance it isn't exposed publicly; reach it by forwarding
  its port over AWS SSM (see the Terraform `console_tunnel` output), then open
  <http://localhost:8080>.

### Watch the billing lifecycle

Use the app at <http://localhost/> to create a customer, provision a generator,
and start/stop it, or drive it with curl as below.

**1. Create a customer** (the canonical, unique record everything ties to). The
customer registry is in-memory — not durable across restarts:

```sh
CUST=$(curl -s -X POST http://localhost/v1/customers -d '{"name":"Acme Inc"}' | jq -r .id)
```

**2. Provision + start a generator process** for that customer (provisioning is
rejected for an unknown customer). It publishes `usage.ingested` every
`interval_ms`; metering meters it:

```sh
ID=$(curl -s -X POST http://localhost/v1/generators \
      -d "{\"customer_id\":\"$CUST\",\"interval_ms\":2000}" | jq -r .id)
curl -X POST http://localhost/v1/generators/$ID/start
```

Watch `usage.ingested` populate in the app's **Live events** panel (or the
Console). Stop it anytime: `curl -X POST http://localhost/v1/generators/$ID/stop`.

**3. Check the mid-cycle invoice** (the thing Stripe alone can't show) — the
`invoicing` service reads priced usage from Metronome (list-costs) + active
subscriptions and previews what would be billed:

```sh
curl http://localhost/v1/customers/$CUST/upcoming-invoice
# {"amount_cents":9,"amount_micros":90000,"lines":[
#   {"description":"API requests · gen 1a2b3c4d","quantity":900,"amount_micros":90000,"unit_price_micros":100}],
#  "upcoming":true,...}
```

**4. Close the billing cycle** — billing emits `billing_cycle.ended`; the
`invoicing` service pulls the priced usage from Metronome and invoices it:

```sh
curl -X POST http://localhost/v1/billing-cycles/$CUST/close
```

Watch the invoicing chain light up in the **Live events** panel (or the Console),
each hop on its own topic:

```
billing_cycle.ended → (invoicing queries metering) → invoice.finalized → payment.succeeded
```

Metering + pricing (via `fakemetronome`) and the invoice (via `fakestripe`) are
real; only the provider APIs themselves are stubbed. The usage rate is a hardcoded
**$0.0001 per API request** (see *Pricing & rounding* below). Zero-amount cycles
are skipped (no invoice).

**Hybrid billing:** provision a **subscription** generator too and start it — then
close the cycle. The invoice carries both a usage line (`API requests · gen … (period N)`)
and a subscription line, because **Stripe** manages the flat-fee subscription and
adds it when it creates the invoice, alongside the metered-usage items the
`invoicing` service pulled from Metronome. The subscription price is a **fixed plan
price owned by Stripe** — the frontend can display it but not set it:

```sh
curl -s http://localhost/v1/subscription-plan            # {"amount_cents":5000,"currency":"usd"}
curl -s -X POST http://localhost/v1/generators \
  -d "{\"customer_id\":\"$CUST\",\"type\":\"subscription\"}" | jq .   # no amount
# then start it, and close the cycle as in step 4
```

**5. Late-arriving usage:** the `emit` endpoint **backdates** usage timestamps to
simulate usage that occurred before the current period started (works even for a
stopped generator). The `metering` service flags it late (`occurred_at <
period_start`), so it bills as its own **`Late usage`** line on the next invoice —
separate from on-time usage. (Close a cycle first so a period boundary exists.)

```sh
curl -X POST "http://localhost/v1/generators/$ID/emit?count=2000"   # backdated -> late
curl -X POST http://localhost/v1/billing-cycles/$CUST/close          # bills as a "Late usage" line
```

**Payment failure branch:** a customer whose id contains `fail` doesn't pay (e.g.
create one named "Fail Co"), so the chain ends on `payment.failed`:

```sh
FAIL=$(curl -s -X POST http://localhost/v1/customers -d '{"name":"Fail Co"}' | jq -r .id)
ID=$(curl -s -X POST http://localhost/v1/generators \
      -d "{\"customer_id\":\"$FAIL\",\"interval_ms\":2000}" | jq -r .id)
curl -X POST http://localhost/v1/generators/$ID/start
# ...let it emit, then:
curl -X POST http://localhost/v1/billing-cycles/$FAIL/close
```

## Pricing & rounding

Money is carried in **micros** (1 USD = 1,000,000 micros; 1¢ = 10,000 micros),
and the flow follows the standard **utility / telecom model: price at high
precision, sum, and round to the currency's smallest unit exactly once.**

- **Price** — Metronome (`fakemetronome`) rates each metered group at
  `unit_price_micros` (here 100 micros = $0.0001 per API request) and returns
  `usage_micros` with **no rounding**.
- **Sum** — Stripe (`fakestripe`) adds each line's micros (usage lines +
  subscription lines) into a single micros total.
- **Round once** — only the **invoice total** is rounded to whole cents
  (`amount_due = round(Σ micros ÷ 10,000)`), like an electricity bill priced in
  mills but charged to the cent.

Why it matters: rounding **per unit or per line** would drop sub-cent usage (13
requests at $0.0001 = $0.0013 → $0.00) and, at volume, distort the bill. Rounding
the **total once** bounds the rounding error to **less than half a cent per
invoice** regardless of how many events or line items there are. The mid-cycle
preview shows the precise per-line micros amounts (so freshly emitted usage is
visible immediately) alongside the rounded total; the finalized invoice bills the
rounded total and skips zero-cent invoices (Stripe won't take a $0 charge).

The other lever real providers use — a **coarser billing unit** (e.g. "per 1,000
requests") so sub-cent amounts rarely arise — is not used here; this project keeps
the fine-grained per-request unit and relies on precise summation instead.

**Subscription proration.** On the **mid-cycle preview**, a flat-fee subscription
is shown *prorated* for the partial period — a `Subscription (prorated)` line at a
default **$25** (vs the **$50** full-period plan price), with a note in the UI
(`subscription_prorated` in the response). The **finalized invoice** at cycle close
bills the **full period** ($50). Stripe owns the amounts (`fakestripe` exposes both
`amount_cents` and `prorated_cents`); `invoicing` just picks the prorated one for
the preview and labels it. The proration is a **fixed default**, not computed from
elapsed period time — real Stripe prorates by the actual time fraction, and only
when a subscription changes mid-period.

**Subscription cancellation.** Cancelling a flat-fee subscription mid-cycle
(stopping the subscription generator) is **terminal** — it can never be resumed;
a new subscription must be provisioned in its place (`controlplane` rejects a
restart with `409`, and `fakestripe` rejects re-registering the same id). The
cancelled subscription is **not** dropped immediately: it stays until the next
invoice, which bills it at the **prorated** amount for the partial period it was
active and lists it as `Subscription … (cancelled, prorated)`, then removes it (so
it never bills again). An **active** subscription, by contrast, bills the full
period at close. The mid-cycle preview reflects the pending cancellation, showing
the sub as `Subscription (cancelled, prorated)`.

## Late-arriving usage

"Late" usage is usage whose event timestamp falls **before the customer's current
billing period began** — i.e. it belongs to a period that already closed but was
reported afterward. It's surfaced as its own invoice line (`Late usage · …`),
separate from on-time usage.

Two design rules make this faithful to how you'd build it on real Metronome:

- **The source does not decide lateness.** `controlplane` only reports *when*
  usage occurred (an `occurred_at` timestamp); it has no concept of billing
  periods. The `emit` endpoint *simulates* a late arrival by **backdating** that
  timestamp — not by setting a flag.
- **Metronome can't determine lateness either.** Real Metronome assigns events to
  periods by timestamp and offers a grace period, but a billable metric can only
  **group by properties you send** — there is no built-in "late" dimension. So
  neither `fakemetronome` nor real Metronome computes it.

Lateness is therefore **determined by the `metering` ingestion service** (the
layer that enriches events before sending them to Metronome):

1. It learns each customer's current period start from `billing_cycle.ended`.
2. On each usage event it computes `late = occurred_at < period_start`.
3. It stamps `late` as an event **property**, so Metronome's `group_by`
   (`generator_id, late`) splits on-time vs late into distinct groups → distinct
   invoice lines. Metronome stays oblivious to what `late` means.

Demo simplification: this project rolls late usage *forward by receipt* into the
current period (so it bills on the next invoice) rather than modeling Metronome's
grace-period/backdating-into-a-closed-period behavior. The determination
mechanism (timestamp vs. period boundary, computed at ingestion) is the part that
mirrors production.

## Metronome API fidelity

`fakemetronome` is meant to only do what the real Metronome API can do:

- **`POST /ingest`** mirrors real Metronome (`v1.usage.ingest`): event shape
  (`transaction_id`, `customer_id`, `timestamp`, `event_type`, `properties`),
  ≤100-event batches, `transaction_id` dedup, and grouping by event **properties**.
- **`GET /v1/customers/{id}/costs`** mirrors real Metronome's **list-costs**
  (`v1.customers.listCosts`) — a faithful subset returning priced usage as
  `data[].line_item_breakdown[]` (`name`, `groups`, `quantity`, `unit_price`,
  `cost` in USD). This is the read the `metering` service uses for the mid-cycle
  preview; real Metronome exposes mid-period *priced* usage via costs / the draft
  invoice, not a bespoke usage endpoint.

**Known deliberate divergence:** `POST /v1/customers/{id}/close-period` is a
**demo shim** — real Metronome has **no** on-demand "close/rotate a billing
period" call; periods are contract/schedule-driven and invoices auto-finalize
after the grace period. It exists only so the demo can bill on demand instead of
waiting for a real period boundary. Lesser simplifications: the fake dedups
indefinitely (real: 34-day window), doesn't reject future-dated or >34-day-old
events, and uses one hardcoded rate instead of configured rate cards.

## Deploy to an AWS t4g.small

The stack is memory-tuned for a 2 GB `t4g.small` (free via the EC2 T4g trial
through Dec 31 2026 — 750 hrs/month ≈ one always-on instance).

**Terraform** (`infra/terraform/`) provisions the instance, security group, IAM
role, swap, and Docker, and can self-deploy on boot (clone + `docker compose up`).
Management is via **AWS SSM (Session Manager)** — no SSH, no key pair, and no
dependency on a static IP. See that directory's README. To do it by hand instead,
on the instance:

1. Add swap for headroom: `sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile`.
2. Install Docker + the compose plugin.
3. `docker compose up --build -d`.
4. Security group inbound: only `80` (app). Recruiters watch events via the app's
   read-only **Live events** panel. The Console is bound to localhost — reach it by
   forwarding its port over SSM (`console_tunnel` output); leave `19092` (Kafka)
   closed too unless you need external tooling.

Cost note: the T4g trial covers the compute, but a **public IPv4 address bills
~$3.60/month** even while attached; use IPv6-only or your free-tier credits to
avoid it.

## Production scaling

This is a single-box demo. The **architecture** (event-driven, Kafka bus,
service separation, usage keyed by customer, batched ingest, provider offload)
is the shape a real usage-billing system takes and scales fine. The **current
implementation** takes demo shortcuts that would not. Here's the gap and the path.

### Demo shortcuts that won't scale
- **In-memory state, single instance.** The customer registry (`billing`),
  processes (`controlplane`), drafts (`fakemetronome`), and invoices/subs
  (`fakestripe`) are in-process maps: not durable, not shared, so you can't run
  more than one instance of a service or fail over.
- **Single Kafka partition per topic** (auto-created) — a throughput ceiling and
  only one consumer per group; no parallelism.
- **Unbounded memory** — `fakemetronome.seen` (every `transaction_id` forever)
  and `fakestripe.invoices` grow without limit.
- **Serial flush, no retry** — the metering ingest batcher drops a batch on a
  failed `/ingest` (the `transaction_id` makes retry *safe*, it's just not done).
- **No caching on the mid-cycle read** — every dashboard poll hits Metronome live.
- **Cycle-close fan-out** — all cycles closing at once funnel through one
  partition and can exceed Stripe's invoice rate limits.

### Path to production (roughly in priority)
1. **Partition the topics by `customer_id`** (already the record key) and run
   **multiple stateless consumer instances per group** so `metering`/`invoicing`
   scale horizontally.
2. **Externalize state** — move the customer registry and orchestration state to
   a real datastore (Postgres/Dynamo); lean on Metronome/Stripe as the source of
   truth for usage/invoices. Make the services stateless.
3. **Bound memory** — TTL the dedup set (mirror Metronome's 34-day window); page
   invoices from Stripe instead of holding them in RAM.
4. **Retry + backpressure** on ingest flush; **rate-limit** invoice creation
   against Stripe; **stagger** cycle closes.
5. **Cache the mid-cycle read** (see below).

### Caching the mid-cycle invoice
The mid-cycle preview is read-only and eventually-consistent, so it caches well.
Layered, cheapest first:
- **Client**: poll only when the panel is open *and* the tab is visible; back off
  to 5–10s; send `ETag`/`If-None-Match` for cheap `304`s.
- **Service cache**: short-TTL (a few seconds) cache in `invoicing`, keyed by
  `customer_id + period`, with **single-flight** so concurrent misses for one
  customer collapse to a single Metronome read. Use a shared cache (Redis) once
  there is more than one `invoicing` instance. Invalidate on `billing_cycle.ended`.
- **Rate-limit backstop** toward Metronome so a cache-wide miss can't exceed its
  limits.
- **Only if metrics demand it**: a materialized per-customer usage counter
  updated off the `usage.ingested` stream, so reads never touch Metronome
  (reconcile against Metronome at close). This adds real operational complexity
  (a second source of truth) — don't build it preemptively.

**Sizing note.** For a seat-based infra SaaS (thousands–tens-of-thousands of
paying orgs, few concurrent billing-page viewers), the short-TTL shared cache +
single-flight + polite clients is sufficient; the materialized view is overkill.
Measure Metronome read QPS and cache hit rate before escalating.
