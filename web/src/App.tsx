import { Fragment, useEffect, useRef, useState } from 'react'
import {
  Customer,
  EventEntry,
  GeneratorProcess,
  Invoice,
  InvoiceLine,
  Subscription,
  UpcomingInvoice,
  cancelSubscription,
  closeCycle,
  createCustomer,
  createSubscription,
  deleteGenerator,
  emitLate,
  getEvents,
  getInvoices,
  getSubscriptionPlan,
  getUpcomingInvoice,
  listCustomers,
  listGenerators,
  listSubscriptions,
  provisionGenerator,
  startGenerator,
  stopGenerator,
} from './api'

type InvoiceView =
  | { kind: 'upcoming'; customer: string; data: UpcomingInvoice }
  | { kind: 'all'; customer: string; finalized: Invoice[]; upcoming: UpcomingInvoice }

const money = (cents: number) => `$${(cents / 100).toFixed(2)}`

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
            <strong>Add subscription</strong> — the mid-cycle preview now adds a <em>prorated</em>{' '}
            flat-fee line ($25 of the $50 plan) on top of usage.
          </li>
        </ol>

        <p className="mb-1">
          <strong>Demo 2 — Late usage rolls into the current cycle</strong>
        </p>
        <p className="mb-1">
          <em>Late usage</em> = events timestamped <strong>before the current billing period began</strong>{' '}
          (they belong to a period that already closed). Rather than dropping them or reopening the
          closed invoice, the system rolls them forward into the current cycle as a separate line.
        </p>
        <ol className="mb-2">
          <li>
            With a usage generator running (from Demo 1), click <strong>Close cycle</strong>. This
            finalizes the first invoice and starts a fresh period — the boundary that defines "late."
          </li>
          <li>
            Click <strong>Emit late</strong> a few times on that generator. Those events are
            backdated to <em>before</em> the new period began.
          </li>
          <li>
            Click <strong>Mid-cycle</strong> again. A distinct{' '}
            <strong>"Usage-based Billing (late)"</strong> line now appears alongside the current
            period's on-time <strong>"Usage-based Billing"</strong> — the late events billed in
            the current cycle, not the closed one.
          </li>
        </ol>
        <p className="text-secondary small mb-0">
          Lateness is determined by the metering service from each event's timestamp versus the
          period boundary — the sender never marks an event as late.
        </p>
      </div>
    </div>
  )
}

export function App() {
  const [customers, setCustomers] = useState<Customer[]>([])
  const [procs, setProcs] = useState<GeneratorProcess[]>([])
  const [subs, setSubs] = useState<Subscription[]>([])
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
        Metronome + Stripe demo — provision usage generators and flat-fee subscriptions, then close
        a cycle to invoice.
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
                    <button className="btn btn-outline-warning" onClick={() => run(() => closeCycle(c.id))}>
                      Close cycle
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
                      title="Emit one usage event now"
                      onClick={() => run(() => emitLate(p.id))}
                    >
                      Emit late
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

      {/* Subscriptions (flat-fee) — owned by invoicing + fakestripe */}
      <h2 className="h5 mt-4">Subscriptions</h2>
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
        <span className="text-secondary small">fixed plan price: {planLabel}</span>
        <button
          className="btn btn-sm btn-primary"
          disabled={!selected}
          onClick={() => runSub(() => createSubscription(selected))}
        >
          Add subscription
        </button>
        <span className="text-secondary small">showing subscriptions for the selected customer</span>
      </div>

      <div className="table-responsive">
        <table className="table table-sm align-middle">
          <thead>
            <tr>
              <th>Subscription</th>
              <th>Customer</th>
              <th>Stripe sub</th>
              <th>Status</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {subs.length === 0 && (
              <tr>
                <td colSpan={5} className="text-secondary">
                  No subscriptions for this customer.
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
                  <code className="small">{s.stripe_subscription_id || '—'}</code>
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
                <td>
                  {s.status === 'canceled' ? (
                    <span className="text-secondary small">—</span>
                  ) : (
                    <button
                      className="btn btn-sm btn-outline-danger"
                      onClick={() => runSub(() => cancelSubscription(s.id))}
                    >
                      Cancel
                    </button>
                  )}
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
