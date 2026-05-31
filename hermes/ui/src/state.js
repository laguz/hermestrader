import { reactive, computed } from 'vue'

const CALM_KEY = 'hermes.calmMode'

export const state = reactive({
  status: {},
  approvals: {
    pending: [],
    all: []
  },
  calmMode: (typeof localStorage !== 'undefined' && localStorage.getItem(CALM_KEY) === '1'),
  hotkeysHelpOpen: false,
  logs: [],
  watchlistData: {
    per_strategy: {},
    global_default: [],
    strategies: []
  },
  lotsData: {},
  chartsData: {
    analyses: {},
    watchlist: []
  },
  analyticsData: {
    performance: {},
    open_trades: [],
    closed_trades: [],
    pnl_series: []
  },
  keyLevelsData: {},
  keyLevelsHorizon: '3m',
  keyLevelsLoading: false,
  keyLevelsError: null,
  soul: {
    soul: '',
    autonomy: 'advisory'
  },
  llm: {
    provider: 'mock',
    base_url: '',
    model: '',
    temperature: 0.2,
    timeout_s: 120,
    has_api_key: false,
    api_key_hint: '',
    last_error: '',
    last_ok_age_s: null
  },
  lastUpdated: '',
  isConnected: false,
  toast: {
    message: '',
    show: false,
    isError: false
  }
})

export function showToast(msg, isErr = false) {
  state.toast.message = msg
  state.toast.isError = isErr
  state.toast.show = true
  setTimeout(() => {
    state.toast.show = false
  }, 2800)
}

export async function api(method, path, body) {
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json' }
  }
  if (body !== undefined) opts.body = JSON.stringify(body)
  const r = await fetch(path, opts)
  if (!r.ok) {
    const e = await r.json().catch(() => ({ detail: r.statusText }))
    throw new Error(e.detail || r.statusText)
  }
  return r.json()
}

// Audible alert for new pending approvals (Web Audio, no asset needed)
let audioCtx = null
let lastPendingIds = new Set()

function ensureAudio() {
  if (audioCtx) return audioCtx
  try {
    const Ctor = window.AudioContext || window.webkitAudioContext
    if (Ctor) audioCtx = new Ctor()
  } catch (_) {
    audioCtx = null
  }
  return audioCtx
}

export function beep(freq = 880, durationMs = 140) {
  if (state.calmMode) return
  const ctx = ensureAudio()
  if (!ctx) return
  try {
    const osc = ctx.createOscillator()
    const gain = ctx.createGain()
    osc.type = 'sine'
    osc.frequency.value = freq
    gain.gain.setValueAtTime(0.0001, ctx.currentTime)
    gain.gain.exponentialRampToValueAtTime(0.18, ctx.currentTime + 0.01)
    gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + durationMs / 1000)
    osc.connect(gain).connect(ctx.destination)
    osc.start()
    osc.stop(ctx.currentTime + durationMs / 1000 + 0.02)
  } catch (_) {
    // ignore — audio is best-effort
  }
}

export function setCalmMode(on) {
  state.calmMode = !!on
  try {
    localStorage.setItem(CALM_KEY, state.calmMode ? '1' : '0')
  } catch (_) { /* ignore */ }
}

// First pending approval — what hotkeys A/R act on.
export const firstPending = computed(() => state.approvals.pending[0] || null)

// SSE Connection Management
let eventSource = null

function onPendingChanged(pending) {
  const ids = new Set(pending.map(p => p.id))
  // Detect newly arrived pending items (id present now, absent before).
  let isNew = false
  for (const id of ids) {
    if (!lastPendingIds.has(id)) { isNew = true; break }
  }
  if (isNew && lastPendingIds.size > 0) beep(880, 160)
  lastPendingIds = ids
}

export function connectSSE() {
  if (eventSource) return

  eventSource = new EventSource('/api/status/stream')

  eventSource.onopen = () => {
    state.isConnected = true
  }

  eventSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data)
      state.isConnected = true

      // Update status
      if (data.status) {
        state.status = data.status
      }

      // Update approvals
      if (data.approvals) {
        const pending = data.approvals.filter(r => r.status === 'PENDING')
        onPendingChanged(pending)
        state.approvals.pending = pending
        state.approvals.all = data.approvals
      }

      // Update logs
      if (data.logs) {
        state.logs = data.logs
      }
    } catch (e) {
      console.error('SSE message processing error', e)
    }
  }

  eventSource.onerror = (err) => {
    console.error('SSE error, reconnecting...', err)
    state.isConnected = false
  }
}

export function disconnectSSE() {
  if (eventSource) {
    eventSource.close()
    eventSource = null
    state.isConnected = false
  }
}

// API functions
export async function loadStatus() {
  try {
    const data = await api('GET', '/api/status')
    state.status = data
    return data
  } catch (e) {
    console.error('Failed to load status:', e)
  }
}

export async function loadApprovals() {
  try {
    const [pending, all] = await Promise.all([
      api('GET', '/api/approvals?status=PENDING&limit=50'),
      api('GET', '/api/approvals?limit=30')
    ])
    state.approvals.pending = pending
    state.approvals.all = all
  } catch (e) {
    console.error('Failed to load approvals:', e)
  }
}

export async function loadWatchlist() {
  try {
    const [wl, lots] = await Promise.all([
      api('GET', '/api/watchlist'),
      api('GET', '/api/lots').catch(() => ({}))
    ])
    state.watchlistData = wl
    state.lotsData = lots
  } catch (e) {
    console.error('Failed to load watchlist:', e)
  }
}

export async function loadLots() {
  try {
    const lots = await api('GET', '/api/lots').catch(() => ({}))
    state.lotsData = lots
  } catch (e) {
    console.error('Failed to load lots:', e)
  }
}

export async function loadSoul() {
  try {
    const data = await api('GET', '/api/soul')
    state.soul = data
  } catch (e) {
    console.error('Failed to load soul:', e)
  }
}

export async function loadLLM() {
  try {
    const data = await api('GET', '/api/llm')
    state.llm = data
  } catch (e) {
    console.error('Failed to load LLM:', e)
  }
}

export async function loadLogs() {
  try {
    const data = await api('GET', '/api/logs?limit=120')
    state.logs = data
  } catch (e) {
    console.error('Failed to load logs:', e)
  }
}

export async function loadAnalytics() {
  try {
    const data = await api('GET', '/api/analytics')
    state.analyticsData = data
    state.lastUpdated = new Date().toLocaleTimeString()
  } catch (e) {
    showToast('Failed to load analytics: ' + e.message, true)
    throw e
  }
}

export async function loadAnalysis(horizon) {
  state.keyLevelsHorizon = horizon
  state.keyLevelsLoading = true
  state.keyLevelsError = null
  try {
    const data = await api('GET', `/api/analysis?period=${horizon}&_=${Date.now()}`)
    if (data.error) {
      state.keyLevelsError = data.error
    } else {
      state.keyLevelsData = data
    }
  } catch (e) {
    state.keyLevelsError = e.message
  } finally {
    state.keyLevelsLoading = false
  }
}

// Action triggers
export async function decide(id, action, notes = '') {
  try {
    await api('POST', `/api/approvals/${id}/${action}`, { notes })
    showToast(action === 'approve' ? '✓ Trade approved — executes next tick' : '✗ Trade rejected', action !== 'approve')
    await Promise.all([loadStatus(), loadApprovals()])
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function bulkDecide(action) {
  const label = action === 'approve' ? 'Approve ALL pending trades?' : 'Reject ALL pending trades?'
  if (!confirm(label + '\n\nThis cannot be undone.')) return
  try {
    const res = await api('POST', '/api/approvals/bulk', { action, notes: 'bulk ' + action })
    showToast((action === 'approve' ? '✓ Approved ' : '✗ Rejected ') + res.count + ' trades', action !== 'approve')
    await Promise.all([loadStatus(), loadApprovals()])
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function saveSoul(soulText) {
  try {
    await api('PUT', '/api/soul', { soul: soulText })
    state.soul.soul = soulText
    showToast('✓ Soul saved — agent picks it up on next tick')
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function saveAutonomy(autonomyVal) {
  try {
    await api('PUT', '/api/soul', { autonomy: autonomyVal })
    state.soul.autonomy = autonomyVal
    showToast('✓ Autonomy updated')
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function toggleStrategy(sid, enabled) {
  try {
    await api('PUT', `/api/strategies/${sid}`, { enabled })
    showToast(`✓ ${sid} ${enabled ? 'enabled' : 'disabled'} — takes effect next tick`)
    await loadStatus()
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function togglePause() {
  const isPaused = state.status.paused
  try {
    await api('POST', isPaused ? '/api/agent/resume' : '/api/agent/pause')
    await loadStatus()
    showToast(isPaused ? '▶ Agent resumed' : '⏸ Agent paused')
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function setMode(mode) {
  if (mode === 'live' && !confirm('Switch to LIVE trading? Real money will be at risk.')) return
  try {
    await api('PUT', '/api/mode', { mode })
    await loadStatus()
    showToast('✓ Mode → ' + mode.toUpperCase())
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function setApprovalMode(enabled) {
  try {
    await api('PUT', '/api/approval-mode', { enabled })
    await loadStatus()
    showToast(enabled ? 'Approval mode ON' : 'Approval mode OFF — agent fires automatically')
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function saveLLM(config) {
  try {
    const res = await api('PUT', '/api/llm', config)
    showToast('✓ LLM config saved' + (res.has_api_key ? ' — API key stored' : ''))
    await loadLLM()
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function saveLots(sid, targetVal, maxVal, hasTarget) {
  const body = { strategy_id: sid }
  if (maxVal != null) body.max_lots = parseInt(maxVal) || 1
  if (hasTarget && targetVal != null) body.target_lots = parseInt(targetVal) || 1
  try {
    const updated = await api('PUT', '/api/lots', body)
    state.lotsData = updated
    showToast('✓ ' + sid + ' lots saved — takes effect next tick')
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function addSymbol(sid, sym) {
  const cleanedSym = sym.trim().toUpperCase()
  if (!cleanedSym) return
  const current = (state.watchlistData.per_strategy[sid] || []).length === 0
    ? [...state.watchlistData.global_default]
    : [...state.watchlistData.per_strategy[sid]]
  if (current.includes(cleanedSym)) return
  try {
    await api('PUT', `/api/watchlist/${sid}`, { symbols: [...current, cleanedSym] })
    showToast('✓ ' + cleanedSym + ' added to ' + sid)
    await loadWatchlist()
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function removeSymbol(sid, sym) {
  const current = (state.watchlistData.per_strategy[sid] || []).length === 0
    ? [...state.watchlistData.global_default]
    : [...state.watchlistData.per_strategy[sid]]
  const updated = current.filter(s => s !== sym)
  try {
    await api('PUT', `/api/watchlist/${sid}`, { symbols: updated })
    showToast('✓ ' + sym + ' removed from ' + sid)
    await loadWatchlist()
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function resetWatchlist(sid) {
  try {
    await api('DELETE', `/api/watchlist/${sid}`)
    showToast('↺ ' + sid + ' reset to global default')
    await loadWatchlist()
  } catch (e) {
    showToast('Error: ' + e.message, true)
  }
}

export async function forceTriggerML() {
  try {
    await api('POST', '/api/ml/trigger')
    showToast('ML predictor triggered. Background training may take a minute.')
  } catch (e) {
    showToast('Failed to trigger ML: ' + e.message, true)
    throw e
  }
}

export async function decideFirstPending(action) {
  const first = state.approvals.pending[0]
  if (!first) {
    showToast('No pending trades', true)
    return
  }
  await decide(first.id, action, '')
}

// Lightweight background loop to keep the trader cockpit's open positions /
// today's P&L visible across views. The analytics endpoint is heavier than
// /api/status, so we poll it on a longer cadence than the SSE status stream.
let analyticsTimer = null
export function startAnalyticsAutoLoad(intervalMs = 60000) {
  if (analyticsTimer) return
  loadAnalytics().catch(() => {})
  analyticsTimer = setInterval(() => {
    loadAnalytics().catch(() => {})
  }, intervalMs)
}
export function stopAnalyticsAutoLoad() {
  if (analyticsTimer) {
    clearInterval(analyticsTimer)
    analyticsTimer = null
  }
}

export async function loadCharts() {
  try {
    const data = await api('GET', '/api/charts')
    state.chartsData = data
  } catch (e) {
    console.error('Failed to load charts:', e)
  }
}
