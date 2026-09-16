// Client for the platform API (customer registry + generator process control).

export interface Customer {
  id: string
  name: string
  stripe_customer_id: string
  metronome_customer_id: string
  created_at: string
}

export type GeneratorType = 'usage' | 'subscription'

export interface GeneratorProcess {
  id: string
  type: GeneratorType
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
  type: GeneratorType,
  opts: { intervalMs?: number; eventsPerTick?: number } = {},
): Promise<GeneratorProcess> {
  // A subscription's price is fixed by Stripe; the frontend never sends an amount.
  return asJSON<GeneratorProcess>(
    await fetch(BASE, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        customer_id: customerId,
        type,
        interval_ms: opts.intervalMs,
        events_per_tick: opts.eventsPerTick,
      }),
    }),
  )
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

// emitLate fires usage events on demand (independent of the tick loop) — e.g. to
// generate late-arriving usage after a billing cycle has been closed.
export async function emitLate(id: string, count = 1): Promise<void> {
  const res = await fetch(`${BASE}/${id}/emit?count=${count}`, { method: 'POST' })
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}`)
  }
}

// closeCycle ends a customer's billing cycle (triggers Stripe invoicing).
export async function closeCycle(customerId: string): Promise<void> {
  const res = await fetch(`/v1/billing-cycles/${customerId}/close`, { method: 'POST' })
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
