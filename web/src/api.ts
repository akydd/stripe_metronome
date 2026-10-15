// Client for the platform API (customer registry + generator process control).

export interface Customer {
  id: string
  name: string
  stripe_customer_id: string
  metronome_customer_id: string
  created_at: string
}

export interface GeneratorProcess {
  id: string
  type: 'usage'
  customer_id: string
  interval_ms?: number
  events_per_tick?: number
  running: boolean
  emitted: number
}

export interface SubscriptionPlan {
  amount_cents: number
  currency: string
}

// Subscription is a customer's subscription container (holds flat_fee/usage items).
export interface Subscription {
  id: string
  customer_id: string
  stripe_subscription_id: string
  status: string // pending | active | canceled
  items: string[] // "flat_fee", "usage"
  current_period_end: string // when the next invoice auto-finalizes
  period: number
  created_at: string
}

export type ItemType = 'flat_fee' | 'usage'

export interface InvoiceLine {
  description: string
  amount_micros?: number
  quantity?: number
  unit_price_micros?: number
}

export interface UpcomingInvoice {
  customer_id: string
  usage_micros: number
  subscription_micros: number
  subscription_prorated: boolean
  amount_micros: number
  amount_cents: number // rounded once from amount_micros
  currency: string
  lines: InvoiceLine[]
}

export interface Invoice {
  id: string
  customer: string
  period: number
  amount_due: number // cents, rounded once
  currency: string
  status: string
  paid: boolean
  lines: InvoiceLine[]
  created: string
}

const BASE = '/v1/generators'

async function asJSON<T>(res: Response): Promise<T> {
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}`)
  }
  return (await res.json()) as T
}

// --- customer registry ---

export async function listCustomers(): Promise<Customer[]> {
  return asJSON<Customer[]>(await fetch('/v1/customers'))
}

export async function createCustomer(name: string): Promise<Customer> {
  return asJSON<Customer>(
    await fetch('/v1/customers', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    }),
  )
}

// --- generator processes ---

export async function listGenerators(): Promise<GeneratorProcess[]> {
  return asJSON<GeneratorProcess[]>(await fetch(BASE))
}

export async function getSubscriptionPlan(): Promise<SubscriptionPlan> {
  return asJSON<SubscriptionPlan>(await fetch('/v1/subscription-plan'))
}

export async function provisionGenerator(
  customerId: string,
  opts: { intervalMs?: number; eventsPerTick?: number } = {},
): Promise<GeneratorProcess> {
  return asJSON<GeneratorProcess>(
    await fetch(BASE, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        customer_id: customerId,
        type: 'usage',
        interval_ms: opts.intervalMs,
        events_per_tick: opts.eventsPerTick,
      }),
    }),
  )
}

// --- flat-fee subscriptions (owned by invoicing; created/cancelled via the
// control plane). The price is Stripe's fixed plan price — no amount is sent. ---

export async function listSubscriptions(customerId: string): Promise<Subscription[]> {
  return asJSON<Subscription[]>(await fetch(`/v1/customers/${customerId}/subscriptions`))
}

// addSubscriptionItem adds a flat_fee or usage item to the customer's container
// (creating it if needed). An Idempotency-Key makes a retried add safe.
export async function addSubscriptionItem(
  customerId: string,
  itemType: ItemType,
): Promise<Subscription> {
  return asJSON<Subscription>(
    await fetch('/v1/subscriptions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() },
      body: JSON.stringify({ customer_id: customerId, item_type: itemType }),
    }),
  )
}

export async function removeSubscriptionItem(customerId: string, itemType: ItemType): Promise<void> {
  const res = await fetch('/v1/subscriptions/remove-item', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ customer_id: customerId, item_type: itemType }),
  })
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}`)
  }
}

export async function startGenerator(id: string): Promise<GeneratorProcess> {
  return asJSON<GeneratorProcess>(await fetch(`${BASE}/${id}/start`, { method: 'POST' }))
}

export async function stopGenerator(id: string): Promise<GeneratorProcess> {
  return asJSON<GeneratorProcess>(await fetch(`${BASE}/${id}/stop`, { method: 'POST' }))
}

export async function deleteGenerator(id: string): Promise<void> {
  const res = await fetch(`${BASE}/${id}`, { method: 'DELETE' })
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}`)
  }
}

// emitUsage fires a burst of usage events on demand (independent of the tick loop),
// timestamped now.
export async function emitUsage(id: string, count = 1): Promise<void> {
  const res = await fetch(`${BASE}/${id}/emit?count=${count}`, { method: 'POST' })
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}`)
  }
}

// --- invoices ---

export async function getUpcomingInvoice(customerId: string): Promise<UpcomingInvoice> {
  return asJSON<UpcomingInvoice>(await fetch(`/v1/customers/${customerId}/upcoming-invoice`))
}

export async function getInvoices(customerId: string): Promise<Invoice[]> {
  return asJSON<Invoice[]>(await fetch(`/v1/customers/${customerId}/invoices`))
}

// --- live event feed (read-only) ---

export interface EventEntry {
  seq: number
  topic: string
  partition: number
  offset: number
  timestamp: string
  customer_id?: string
  type?: string
  payload?: unknown
}

export interface EventsResponse {
  events: EventEntry[] // ascending seq order (oldest first)
  last_seq: number
}

// getEvents returns buffered events with seq greater than `since` (0 = the whole
// buffer), plus the newest seq to poll from next.
export async function getEvents(since = 0): Promise<EventsResponse> {
  return asJSON<EventsResponse>(await fetch(`/v1/events?since=${since}`))
}
