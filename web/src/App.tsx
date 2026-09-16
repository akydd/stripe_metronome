import { useEffect, useState } from 'react'
import {
  Customer,
  GeneratorProcess,
  GeneratorType,
  Invoice,
  InvoiceLine,
  UpcomingInvoice,
  closeCycle,
  createCustomer,
  deleteGenerator,
  emitLate,
  getInvoices,
  getSubscriptionPlan,
  getUpcomingInvoice,
  listCustomers,
  listGenerators,
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

export function App() {
  const [customers, setCustomers] = useState<Customer[]>([])
  const [procs, setProcs] = useState<GeneratorProcess[]>([])
  const [newName, setNewName] = useState('Acme Inc')
  const [selected, setSelected] = useState('')
  const [genType, setGenType] = useState<GeneratorType>('usage')
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
      <p className="text-secondary">
        Metronome + Stripe demo — provision usage &amp; subscription generators, then close a cycle
        to invoice.
      </p>

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

      {/* Generators */}
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
        <select
          className="form-select form-select-sm w-auto"
          value={genType}
          onChange={(e) => setGenType(e.target.value as GeneratorType)}
        >
          <option value="usage">usage-based</option>
          <option value="subscription">flat-fee subscription</option>
        </select>
        {genType === 'subscription' && (
          <span className="text-secondary small">fixed plan price: {planLabel}</span>
        )}
        {genType === 'usage' && (
          <>
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
          </>
        )}
        <button
          className="btn btn-sm btn-primary"
          disabled={!selected}
          onClick={() =>
            run(() =>
              provisionGenerator(
                selected,
                genType,
                genType === 'usage' ? { intervalMs, eventsPerTick: perTick } : {},
              ),
            )
          }
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
              <th>Type</th>
              <th>Detail</th>
              <th>Status</th>
              <th>Emitted</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {procs.length === 0 && (
              <tr>
                <td colSpan={7} className="text-secondary">
                  No processes yet.
                </td>
              </tr>
            )}
            {procs.map((p) => (
              <tr key={p.id}>
                <td>
                  <code>{p.id.slice(0, 8)}</code>
                </td>
                <td>{nameFor(p.customer_id)}</td>
                <td>
                  <span className="badge text-bg-secondary">{p.type}</span>
                </td>
                <td>
                  {p.type === 'subscription'
                    ? planLabel
                    : `${p.events_per_tick ?? 1} / ${p.interval_ms} ms`}
                </td>
                <td>
                  {p.running ? (
                    <span className="text-success">● running</span>
                  ) : (
                    <span className="text-secondary">■ stopped</span>
                  )}
                </td>
                <td>{p.type === 'usage' ? p.emitted : '—'}</td>
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
                    {p.type === 'usage' && (
                      <button
                        className="btn btn-outline-primary"
                        title="Emit one usage event now"
                        onClick={() => run(() => emitLate(p.id))}
                      >
                        Emit late
                      </button>
                    )}
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
    </div>
  )
}
