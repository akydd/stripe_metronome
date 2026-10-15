import { Fragment, useEffect, useRef, useState } from 'react'
import {
  Customer,
  EventEntry,
  GeneratorProcess,
  Invoice,
  InvoiceLine,
  Subscription,
  UpcomingInvoice,
  addSubscriptionItem,
  createCustomer,
  deleteGenerator,
  emitUsage,
  getEvents,
  getInvoices,
  getSubscriptionPlan,
  getUpcomingInvoice,
  listCustomers,
  listGenerators,
  listSubscriptions,
  provisionGenerator,
  removeSubscriptionItem,
  startGenerator,
  stopGenerator,
} from './api'

type InvoiceView =
  | { kind: 'upcoming'; customer: string; data: UpcomingInvoice }
  | { kind: 'all'; customer: string; finalized: Invoice[]; upcoming: UpcomingInvoice }

const money = (cents: number) => `$${(cents / 100).toFixed(2)}`

// countdown renders time remaining until an ISO deadline, given a current-time ms.
function countdown(endISO: string, nowMs: number): string {
  if (!endISO) return '—'
  const ms = new Date(endISO).getTime() - nowMs
  if (ms <= 0) return 'finalizing…'
  const s = Math.ceil(ms / 1000)
  if (s < 60) return `in ${s}s`
  return `in ${Math.floor(s / 60)}m ${s % 60}s`
}

// fmtMicros shows a micros amount as dollars: 2 decimals when it's a whole cent,
// otherwise up to 4 (so sub-cent usage is visible).
function fmtMicros(micros: number): string {
  const dollars = micros / 1e6
  const cents = dollars * 100
  if (Math.abs(cents - Math.round(cents)) < 1e-9) return `$${dollars.toFixed(2)}`
  return `$${dollars.toFixed(4)}`
}

function InvoiceLines({ lines, totalCents }: { lines: InvoiceLine[]; totalCents: number }) {
  return (
    <table className="table table-sm w-auto mb-0">
      <tbody>
        {(lines ?? []).map((l, i) => (
          <tr key={i}>
            <td className="pe-4">
              {l.description}
              {l.quantity != null && l.unit_price_micros != null && (
                <span className="text-secondary small">
                  {' '}
                  — {l.quantity.toLocaleString()} × {fmtMicros(l.unit_price_micros)}
                </span>
              )}
            </td>
            <td className="text-end">{fmtMicros(l.amount_micros ?? 0)}</td>
          </tr>
        ))}
        <tr className="fw-semibold border-top">
          <td className="pe-4">Total (rounded)</td>
          <td className="text-end">{money(totalCents)}</td>
        </tr>
      </tbody>
    </table>
  )
}

// Bootstrap badge class per event topic, so the feed is scannable by color.
const TOPIC_BADGE: Record<string, string> = {
  'usage.ingested': 'text-bg-info',
  'subscription.created': 'text-bg-primary',
  'subscription.cancelled': 'text-bg-secondary',
  'billing_cycle.ended': 'text-bg-warning',
  'invoice.finalized': 'text-bg-primary',
  'payment.succeeded': 'text-bg-success',
  'payment.failed': 'text-bg-danger',
}
const ALL_TOPICS = Object.keys(TOPIC_BADGE)
const MAX_ROWS = 200

// LiveEvents shows events as they flow across the Kafka bus, read from the
// eventfeed service. It polls independently of the main console refresh so a
// recruiter can watch events stream in — with the broker partition/offset shown
// as proof they really traversed Kafka.
function LiveEvents() {
  const [events, setEvents] = useState<EventEntry[]>([])
  const [paused, setPaused] = useState(false)
  const [filter, setFilter] = useState('')
  const [expanded, setExpanded] = useState<number | null>(null)
  const lastSeq = useRef(0)
  const pausedRef = useRef(paused)

  useEffect(() => {
    pausedRef.current = paused
  }, [paused])

  useEffect(() => {
    async function tick() {
      if (pausedRef.current) return
      try {
        const res = await getEvents(lastSeq.current)
        if (res.last_seq > lastSeq.current) lastSeq.current = res.last_seq
        if (res.events.length > 0) {
          // Server returns oldest→newest; show newest on top.
          const incoming = [...res.events].reverse()
          setEvents((prev) => [...incoming, ...prev].slice(0, MAX_ROWS))
        }
      } catch {
        /* ignore transient errors; next tick retries */
      }
    }
    tick()
    const t = setInterval(tick, 1500)
    return () => clearInterval(t)
  }, [])

  const shown = filter ? events.filter((e) => e.topic === filter) : events
  const fmtTime = (ts: string) => new Date(ts).toLocaleTimeString()

  return (
    <>
      <h2 className="h5 mt-4">Live events</h2>
      <p className="text-secondary small mb-2">
        Read-only tail of the Kafka bus (via the eventfeed service). Each row shows the broker
        partition:offset — proof the event really flowed through Redpanda, not the browser.
      </p>
      <div className="d-flex flex-wrap gap-2 align-items-center mb-2">
        <button
          className={`btn btn-sm ${paused ? 'btn-outline-success' : 'btn-outline-secondary'}`}
          onClick={() => setPaused((p) => !p)}
        >
          {paused ? '▶ Resume' : '⏸ Pause'}
        </button>
        <select
          className="form-select form-select-sm w-auto"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        >
          <option value="">all topics</option>
          {ALL_TOPICS.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
        <span className="text-secondary small">
          {shown.length} shown{paused ? ' · paused' : ''}
        </span>
      </div>
      <div className="table-responsive" style={{ maxHeight: 360, overflowY: 'auto' }}>
        <table className="table table-sm align-middle mb-0">
          <thead>
            <tr>
              <th>Time</th>
              <th>Topic</th>
              <th>Customer</th>
              <th className="text-end">part:offset</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {shown.length === 0 && (
              <tr>
                <td colSpan={5} className="text-secondary">
                  Waiting for events — start a generator above.
                </td>
              </tr>
            )}
            {shown.map((e) => (
              <Fragment key={e.seq}>
                <tr
                  style={{ cursor: 'pointer' }}
                  onClick={() => setExpanded((x) => (x === e.seq ? null : e.seq))}
                >
                  <td className="text-secondary small">{fmtTime(e.timestamp)}</td>
                  <td>
                    <span className={`badge ${TOPIC_BADGE[e.topic] ?? 'text-bg-secondary'}`}>
                      {e.topic}
                    </span>
                  </td>
                  <td>
                    <code className="small">{e.customer_id || '—'}</code>
                  </td>
                  <td className="text-end text-secondary small">
                    {e.partition}:{e.offset}
                  </td>
                  <td className="text-secondary small">{expanded === e.seq ? '▾' : '▸'}</td>
                </tr>
                {expanded === e.seq && (
                  <tr>
                    <td colSpan={5}>
                      <pre className="small mb-0 p-2 bg-body-tertiary rounded">
                        {JSON.stringify(e.payload ?? {}, null, 2)}
                      </pre>
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}

// HowToDemo is a dismissible notes card explaining the two headline demos. The
// dismissed state is remembered per browser so it doesn't nag on every visit.
function HowToDemo() {
  const [dismissed, setDismissed] = useState(() => {
    try {
      return localStorage.getItem('howtoDismissed') === '1'
    } catch {
      return false
    }
  })
  if (dismissed) return null
  const dismiss = () => {
    setDismissed(true)
    try {
      localStorage.setItem('howtoDismissed', '1')
    } catch {
      /* ignore */
    }
  }
  return (
    <div className="card mb-4">
      <div className="card-body">
        <div className="d-flex justify-content-between align-items-start">
          <h2 className="h5 mb-2">How this demo works</h2>
          <button className="btn-close" aria-label="Dismiss" onClick={dismiss} />
        </div>
        <p className="mb-3">
          Usage is metered in <strong>Metronome</strong> and invoiced by <strong>Stripe</strong>{' '}
          (both local stand-ins), wired together over a <strong>Kafka</strong> (Redpanda) event bus.
          Provision <em>generators</em> for a customer to simulate activity, then watch billing
          react. Every event is visible live in the <strong>Live events</strong> panel at the bottom
          of the page.
        </p>

        <p className="mb-1">
          <strong>Demo 1 — Live mid-cycle usage</strong>{' '}
          <span className="text-secondary">(the thing Stripe alone can't show)</span>
        </p>
        <ol className="mb-3">
          <li>
            In <strong>Customers</strong>, enter a name and click <strong>Create customer</strong>.
          </li>
          <li>
            In <strong>Generators</strong>, select that customer, click{' '}
            <strong>Provision generator</strong>, then click <strong>Start</strong>.
          </li>
          <li>
            Click <strong>Mid-cycle</strong> on the customer's row and leave it open. The preview
            refreshes as usage is metered, so the amount climbs in real time — priced per generator
            at sub-cent precision, before any invoice is finalized.
          </li>
          <li>
            <em>(Optional)</em> In <strong>Subscriptions</strong>, pick that customer and click{' '}
            <strong>Add flat-fee</strong> — the mid-cycle preview adds a <em>prorated</em> flat-fee
            line that accrues $1/s toward the $30 period fee.
          </li>
        </ol>

        <p className="mb-1">
          <strong>Demo 2 — Combined invoice + independent items</strong>
        </p>
        <p className="mb-1">
          A subscription is a container with a <strong>30-second period</strong> holding a{' '}
          <strong>flat-fee</strong> and/or <strong>usage</strong> item. At the period end Stripe
          auto-finalizes <em>one</em> invoice = the flat fee <strong>+</strong> that period's usage
          (pulled from Metronome for the exact window). Removing the flat-fee item keeps usage
          billing on the same cycle.
        </p>
        <ol className="mb-2">
          <li>
            In <strong>Subscriptions</strong>, click <strong>Add flat-fee</strong> and{' '}
            <strong>Add usage</strong> for a customer, then provision + start a{' '}
            <strong>usage generator</strong> for them.
          </li>
          <li>
            Watch the subscription's <strong>Next invoice</strong> countdown. When it hits zero, no
            click needed — an invoice finalizes on its own.
          </li>
          <li>
            Open <strong>All invoices</strong>: the finalized invoice shows both a{' '}
            <strong>Flat-fee Subscription</strong> line and a <strong>Usage-based Billing</strong>{' '}
            line for the period. The mid-cycle preview then resets for the next period.
          </li>
          <li>
            Click <strong>Remove flat-fee</strong>: the container stays <em>active</em> on just the
            usage item and keeps billing usage each period — cancelling the flat fee doesn't stop
            usage billing.
          </li>
        </ol>
        <p className="text-secondary small mb-0">
          The subscription period is the billing authority; Metronome is queried for usage in that
          exact window and pushed onto the Stripe invoice as line items before it finalizes.
        </p>
      </div>
    </div>
  )
}

export function App() {
  const [customers, setCustomers] = useState<Customer[]>([])
  const [procs, setProcs] = useState<GeneratorProcess[]>([])
  const [subs, setSubs] = useState<Subscription[]>([])
  const [now, setNow] = useState(() => Date.now())
  const [newName, setNewName] = useState('Acme Inc')
  const [selected, setSelected] = useState('')
  const [intervalMs, setIntervalMs] = useState(1000)
  const [perTick, setPerTick] = useState(10)
  const [planCents, setPlanCents] = useState<number | null>(null)
  const [view, setView] = useState<InvoiceView | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [theme, setTheme] = useState<'light' | 'dark'>(() => {
    try {
      return localStorage.getItem('theme') === 'light' ? 'light' : 'dark'
    } catch {
      return 'dark'
    }
  })

  useEffect(() => {
    document.documentElement.setAttribute('data-bs-theme', theme)
    try {
      localStorage.setItem('theme', theme)
    } catch {
      /* ignore */
    }
  }, [theme])

  async function refresh() {
    try {
      const [cs, ps] = await Promise.all([listCustomers(), listGenerators()])
      setCustomers(cs)
      setProcs(ps)
      setError(null)
    } catch (e) {
      setError(String(e))
    }
  }

  // Default the dropdown to the first customer only until the user picks one.
  // (Kept out of refresh() so the 3s poll can't clobber the user's selection.)
  useEffect(() => {
    if (!selected && customers.length > 0) setSelected(customers[0].id)
  }, [customers, selected])

  useEffect(() => {
    refresh()
    getSubscriptionPlan()
      .then((p) => setPlanCents(p.amount_cents))
      .catch(() => setPlanCents(null))
    const t = setInterval(refresh, 3000)
    return () => clearInterval(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Tick every second so subscription countdowns stay live.
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(t)
  }, [])

  // Poll the selected customer's flat-fee subscriptions (owned by invoicing).
  useEffect(() => {
    if (!selected) {
      setSubs([])
      return
    }
    let stop = false
    const load = () =>
      listSubscriptions(selected)
        .then((s) => {
          if (!stop) setSubs(s)
        })
        .catch(() => {
          if (!stop) setSubs([])
        })
    load()
    const t = setInterval(load, 3000)
    return () => {
      stop = true
      clearInterval(t)
    }
  }, [selected])

  // Auto-refresh the open invoice view so live mid-cycle data updates on its own.
  useEffect(() => {
    if (!view) return
    const { kind, customer } = view
    const t = setInterval(() => {
      if (kind === 'upcoming') showUpcoming(customer)
      else showInvoices(customer)
    }, 3000)
    return () => clearInterval(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [view?.kind, view?.customer])

  const planLabel = planCents != null ? `$${(planCents / 100).toFixed(2)}/period` : '…'
  const nameFor = (id: string) => customers.find((c) => c.id === id)?.name ?? id

  async function run(action: () => Promise<unknown>) {
    try {
      await action()
      await refresh()
    } catch (e) {
      setError(String(e))
    }
  }

  // Like run(), but reloads the selected customer's subscriptions afterward.
  async function runSub(action: () => Promise<unknown>) {
    try {
      await action()
      if (selected) setSubs(await listSubscriptions(selected))
      setError(null)
    } catch (e) {
      setError(String(e))
    }
  }

  async function showUpcoming(customer: string) {
    try {
      setView({ kind: 'upcoming', customer, data: await getUpcomingInvoice(customer) })
      setError(null)
    } catch (e) {
      setError(String(e))
    }
  }

  async function showInvoices(customer: string) {
    try {
      const [finalized, upcoming] = await Promise.all([
        getInvoices(customer),
        getUpcomingInvoice(customer),
      ])
      finalized.sort((a, b) => a.period - b.period)
      setView({ kind: 'all', customer, finalized, upcoming })
      setError(null)
    } catch (e) {
      setError(String(e))
    }
  }

  // Re-fetch the currently open invoice view (live mid-cycle data updates as a
  // running generator emits, without reloading the page).
  async function refreshView() {
    if (!view) return
    if (view.kind === 'upcoming') await showUpcoming(view.customer)
    else await showInvoices(view.customer)
  }

  return (
    <div className="container py-4">
      <div className="d-flex justify-content-between align-items-start">
        <h1 className="h3 mb-1">Billing Console</h1>
        <button
          className="btn btn-sm btn-outline-secondary"
          onClick={() => setTheme((t) => (t === 'dark' ? 'light' : 'dark'))}
        >
          {theme === 'dark' ? '☀ Light' : '🌙 Dark'}
        </button>
      </div>
      <p className="text-secondary mb-1">
        Metronome + Stripe demo — provision usage generators and flat-fee subscriptions. Each
        subscription auto-invoices at the end of its billing period (flat fee + that period's usage).
      </p>
      <p className="text-secondary small">
        <span className="badge text-bg-secondary">note</span> All demo data resets daily at{' '}
        <strong>00:00 UTC</strong>.
      </p>

      <HowToDemo />

      {error && <div className="alert alert-danger py-2">{error}</div>}

      {/* Customers */}
      <h2 className="h5 mt-4">Customers</h2>
      <div className="input-group mb-3" style={{ maxWidth: 420 }}>
        <input
          className="form-control"
          value={newName}
          onChange={(e) => setNewName(e.target.value)}
          placeholder="customer name"
        />
        <button className="btn btn-primary" onClick={() => run(() => createCustomer(newName))}>
          Create customer
        </button>
      </div>

      <div className="table-responsive">
        <table className="table table-sm align-middle">
          <thead>
            <tr>
              <th>Customer</th>
              <th>ID</th>
              <th>Stripe</th>
              <th>Metronome</th>
              <th>Invoices</th>
            </tr>
          </thead>
          <tbody>
            {customers.length === 0 && (
              <tr>
                <td colSpan={5} className="text-secondary">
                  No customers yet.
                </td>
              </tr>
            )}
            {customers.map((c) => (
              <tr key={c.id}>
                <td>{c.name}</td>
                <td>
                  <code>{c.id}</code>
                </td>
                <td>
                  <code>{c.stripe_customer_id}</code>
                </td>
                <td>
                  <code>{c.metronome_customer_id}</code>
                </td>
                <td>
                  <div className="btn-group btn-group-sm">
                    <button className="btn btn-outline-primary" onClick={() => showUpcoming(c.id)}>
                      Mid-cycle
                    </button>
                    <button className="btn btn-outline-secondary" onClick={() => showInvoices(c.id)}>
                      All invoices
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Invoice details */}
      {view && (
        <div className="card mb-4">
          <div className="card-body">
            <div className="d-flex justify-content-between align-items-center">
              <h3 className="h6 mb-0">
                {view.kind === 'upcoming' ? 'Mid-cycle (upcoming) invoice' : 'Invoices'} —{' '}
                {nameFor(view.customer)}
              </h3>
              <div className="d-flex align-items-center gap-2">
                <button className="btn btn-sm btn-outline-primary" onClick={() => refreshView()}>
                  ⟳ Refresh
                </button>
                <button className="btn-close" aria-label="Close" onClick={() => setView(null)} />
              </div>
            </div>
            <hr />
            {view.kind === 'upcoming' && (
              <>
                <p className="text-secondary small">
                  Current period · what would be billed if the cycle closed now (usage read from
                  Metronome list-costs + active subscriptions; not finalized).
                </p>
                <InvoiceLines lines={view.data.lines} totalCents={view.data.amount_cents} />
                {view.data.subscription_prorated && (
                  <p className="text-secondary small mt-2 mb-0">
                    * Flat-fee subscription shown prorated for the partial period; the full amount
                    bills at cycle close.
                  </p>
                )}
              </>
            )}
            {view.kind === 'all' && (
              <>
                {view.finalized.map((inv) => (
                  <div key={inv.id} className="mb-3">
                    <div>
                      <span className="fw-semibold">Period {inv.period}</span>{' '}
                      <span className="badge text-bg-primary">finalized</span>{' '}
                      <span className={`badge ${inv.paid ? 'text-bg-success' : 'text-bg-warning'}`}>
                        {inv.paid ? 'paid' : inv.status}
                      </span>{' '}
                      <code className="text-secondary">{inv.id}</code>
                    </div>
                    <InvoiceLines lines={inv.lines} totalCents={inv.amount_due} />
                  </div>
                ))}
                <div className="mb-1">
                  <span className="fw-semibold">Current period</span>{' '}
                  <span className="badge text-bg-secondary">mid-cycle</span>{' '}
                  <span className="text-secondary small">(accruing, not finalized)</span>
                </div>
                <InvoiceLines lines={view.upcoming.lines} totalCents={view.upcoming.amount_cents} />
                {view.upcoming.subscription_prorated && (
                  <p className="text-secondary small mt-2 mb-0">
                    * Flat-fee subscription shown prorated for the partial period; the full amount
                    bills at cycle close.
                  </p>
                )}
                {view.finalized.length === 0 && (
                  <p className="text-secondary small mt-2 mb-0">No finalized invoices yet.</p>
                )}
              </>
            )}
          </div>
        </div>
      )}

      {/* Generators (usage-based) */}
      <h2 className="h5 mt-4">Generators</h2>
      <div className="d-flex flex-wrap gap-2 align-items-center mb-3">
        <select
          className="form-select form-select-sm w-auto"
          value={selected}
          onChange={(e) => setSelected(e.target.value)}
        >
          {customers.length === 0 && <option value="">(create a customer first)</option>}
          {customers.map((c) => (
            <option key={c.id} value={c.id}>
              {c.name} ({c.id})
            </option>
          ))}
        </select>
        <div className="input-group input-group-sm w-auto">
          <span className="input-group-text">every</span>
          <input
            type="number"
            className="form-control"
            style={{ width: 90 }}
            min={10}
            value={intervalMs}
            onChange={(e) => setIntervalMs(Number(e.target.value))}
          />
          <span className="input-group-text">ms ×</span>
          <input
            type="number"
            className="form-control"
            style={{ width: 80 }}
            min={1}
            value={perTick}
            onChange={(e) => setPerTick(Number(e.target.value))}
          />
          <span className="input-group-text">events</span>
        </div>
        <span className="text-secondary small">
          ≈ {intervalMs > 0 ? Math.round((perTick * 1000) / intervalMs) : 0}/s
        </span>
        <button
          className="btn btn-sm btn-primary"
          disabled={!selected}
          onClick={() => run(() => provisionGenerator(selected, { intervalMs, eventsPerTick: perTick }))}
        >
          Provision generator
        </button>
      </div>

      <div className="table-responsive">
        <table className="table table-sm align-middle">
          <thead>
            <tr>
              <th>Process</th>
              <th>Customer</th>
              <th>Rate</th>
              <th>Status</th>
              <th>Emitted</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {procs.length === 0 && (
              <tr>
                <td colSpan={6} className="text-secondary">
                  No generators yet.
                </td>
              </tr>
            )}
            {procs.map((p) => (
              <tr key={p.id}>
                <td>
                  <code>{p.id.slice(0, 8)}</code>
                </td>
                <td>{nameFor(p.customer_id)}</td>
                <td>{`${p.events_per_tick ?? 1} / ${p.interval_ms} ms`}</td>
                <td>
                  {p.running ? (
                    <span className="text-success">● running</span>
                  ) : (
                    <span className="text-secondary">■ stopped</span>
                  )}
                </td>
                <td>{p.emitted}</td>
                <td>
                  <div className="btn-group btn-group-sm">
                    {p.running ? (
                      <button className="btn btn-outline-secondary" onClick={() => run(() => stopGenerator(p.id))}>
                        Stop
                      </button>
                    ) : (
                      <button className="btn btn-outline-success" onClick={() => run(() => startGenerator(p.id))}>
                        Start
                      </button>
                    )}
                    <button
                      className="btn btn-outline-primary"
                      title="Emit a burst of usage now"
                      onClick={() => run(() => emitUsage(p.id, 10))}
                    >
                      Emit usage
                    </button>
                    <button className="btn btn-outline-danger" onClick={() => run(() => deleteGenerator(p.id))}>
                      Delete
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Subscriptions — one container per customer, holding flat_fee/usage items */}
      <h2 className="h5 mt-4">Subscriptions</h2>
      <p className="text-secondary small mb-2">
        A subscription is one container per customer with a billing period, holding a{' '}
        <strong>flat-fee</strong> item ({planLabel}) and/or a <strong>usage</strong> item. At the
        period end it bills all items on <em>one</em> combined
        invoice. Removing the flat fee keeps usage billing; the subscription ends when it has no items.
      </p>
      <div className="d-flex flex-wrap gap-2 align-items-center mb-3">
        <select
          className="form-select form-select-sm w-auto"
          value={selected}
          onChange={(e) => setSelected(e.target.value)}
        >
          {customers.length === 0 && <option value="">(create a customer first)</option>}
          {customers.map((c) => (
            <option key={c.id} value={c.id}>
              {c.name} ({c.id})
            </option>
          ))}
        </select>
        <button
          className="btn btn-sm btn-primary"
          disabled={!selected}
          onClick={() => runSub(() => addSubscriptionItem(selected, 'flat_fee'))}
        >
          Add flat-fee
        </button>
        <button
          className="btn btn-sm btn-outline-primary"
          disabled={!selected}
          onClick={() => runSub(() => addSubscriptionItem(selected, 'usage'))}
        >
          Add usage
        </button>
        <span className="text-secondary small">for the selected customer</span>
      </div>

      <div className="table-responsive">
        <table className="table table-sm align-middle">
          <thead>
            <tr>
              <th>Subscription</th>
              <th>Customer</th>
              <th>Items</th>
              <th>Status</th>
              <th>Next invoice</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {subs.length === 0 && (
              <tr>
                <td colSpan={6} className="text-secondary">
                  No subscription for this customer.
                </td>
              </tr>
            )}
            {subs.map((s) => (
              <tr key={s.id}>
                <td>
                  <code>{s.id}</code>
                </td>
                <td>{nameFor(s.customer_id)}</td>
                <td>
                  {(s.items ?? []).length === 0 ? (
                    <span className="text-secondary small">—</span>
                  ) : (
                    (s.items ?? []).map((it) => (
                      <span
                        key={it}
                        className={`badge me-1 ${it === 'flat_fee' ? 'text-bg-primary' : 'text-bg-info'}`}
                      >
                        {it === 'flat_fee' ? 'flat-fee' : 'usage'}
                      </span>
                    ))
                  )}
                </td>
                <td>
                  <span
                    className={`badge ${
                      s.status === 'active'
                        ? 'text-bg-success'
                        : s.status === 'canceled'
                          ? 'text-bg-secondary'
                          : 'text-bg-warning'
                    }`}
                  >
                    {s.status}
                  </span>
                </td>
                <td className="small text-secondary">
                  {s.status === 'active' ? countdown(s.current_period_end, now) : '—'}
                </td>
                <td>
                  <div className="btn-group btn-group-sm">
                    {(s.items ?? []).includes('flat_fee') && (
                      <button
                        className="btn btn-outline-danger"
                        onClick={() => runSub(() => removeSubscriptionItem(s.customer_id, 'flat_fee'))}
                      >
                        Remove flat-fee
                      </button>
                    )}
                    {(s.items ?? []).includes('usage') && (
                      <button
                        className="btn btn-outline-secondary"
                        onClick={() => runSub(() => removeSubscriptionItem(s.customer_id, 'usage'))}
                      >
                        Remove usage
                      </button>
                    )}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Live event feed */}
      <LiveEvents />
    </div>
  )
}
