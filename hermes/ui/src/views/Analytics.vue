<script setup>
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from 'vue'
import {
  state,
  loadAnalytics,
  loadLots,
  loadAnalysis,
  forceTriggerML
} from '../state'
import Icon from '../components/Icon.vue'

// Local UI state
const activeTab = ref('performance')
const isMLTriggering = ref(false)
const pnlCanvasRef = ref(null)

onMounted(async () => {
  await loadData()
  
  // Start periodic refresh every 30s
  startPolling()
  
  // Trigger initial chart draw if performance is active
  if (activeTab.value === 'performance') {
    nextTick(drawPnlChart)
  }
})

onUnmounted(() => {
  stopPolling()
  window.removeEventListener('resize', drawPnlChart)
})

// Tab routing / switching
function switchTab(tab) {
  activeTab.value = tab
  if (tab === 'keylevels') {
    if (!state.keyLevelsData || Object.keys(state.keyLevelsData).length === 0) {
      loadAnalysis(state.keyLevelsHorizon)
    }
  }
  if (tab === 'performance') {
    nextTick(drawPnlChart)
  }
}

// Watchers
watch(() => state.analyticsData.pnl_series, () => {
  if (activeTab.value === 'performance') {
    nextTick(drawPnlChart)
  }
}, { deep: true })

watch(activeTab, (tab) => {
  if (tab === 'performance') {
    nextTick(() => {
      window.addEventListener('resize', drawPnlChart)
      drawPnlChart()
    })
  } else {
    window.removeEventListener('resize', drawPnlChart)
  }
})

// Polling
let pollInterval = null
function startPolling() {
  pollInterval = setInterval(async () => {
    await loadAnalytics()
    await loadLots()
  }, 30000)
}

function stopPolling() {
  if (pollInterval) {
    clearInterval(pollInterval)
    pollInterval = null
  }
}

async function loadData() {
  try {
    await Promise.all([
      loadAnalytics(),
      loadLots()
    ])
  } catch (e) {
    console.error('Failed to load initial analytics:', e)
  }
}

async function triggerMLRetrain() {
  isMLTriggering.value = true
  try {
    await forceTriggerML()
  } catch (e) {
    console.error('ML trigger failed:', e)
  } finally {
    isMLTriggering.value = false
  }
}

async function handleHorizonChange(horizon) {
  await loadAnalysis(horizon)
}

// Formatting helpers
function fmt$(v) {
  const n = parseFloat(v || 0)
  return (n >= 0 ? '+' : '') + '$' + Math.abs(n).toFixed(2)
}

function fmtPct(v) {
  const n = parseFloat(v || 0)
  return (n >= 0 ? '+' : '') + n.toFixed(2) + '%'
}

function getRelativeTime(iso) {
  if (!iso) return '—'
  const d = Math.round((Date.now() - new Date(iso)) / 1000)
  if (d < 60) return d + 's ago'
  if (d < 3600) return Math.round(d / 60) + 'm ago'
  if (d < 86400) return Math.round(d / 3600) + 'h ago'
  return Math.round(d / 86400) + 'd ago'
}

function pnlColor(v) {
  const n = parseFloat(v || 0)
  if (n > 0) return 'var(--color-green)'
  if (n < 0) return 'var(--color-red)'
  return 'var(--text-muted)'
}

// KPI aggregation
const performanceSummary = computed(() => {
  const perf = state.analyticsData.performance || {}
  let totalPnl = 0
  let totalWin = 0
  let totalLoss = 0
  let totalClosed = 0
  let openCount = 0

  Object.values(perf).forEach(s => {
    totalPnl += s.total_pnl || 0
    totalWin += s.winners || 0
    totalLoss += s.losers || 0
    totalClosed += s.total_closed || 0
    openCount += s.open_count || 0
  })

  const wr = totalClosed > 0 ? Math.round((totalWin / totalClosed) * 100) : 0
  const avgPnl = totalClosed > 0 ? totalPnl / totalClosed : 0

  return {
    totalPnl,
    winRate: wr,
    totalWin,
    totalLoss,
    totalClosed,
    avgPnl,
    openCount
  }
})

// Strategy color scheme
const STRAT_COLORS = {
  CS75: 'var(--color-blue)',
  CS7:  'var(--color-yellow)',
  TT45: 'var(--color-orange)',
  WHEEL: 'var(--color-purple)',
  HermesAlpha: 'var(--color-green)'
}

// Win rate circles parameters
function getSvgDashArray(winRate) {
  const r = 65
  const circ = 2 * Math.PI * r
  const dashLen = circ * (winRate / 100)
  return `${dashLen.toFixed(1)} ${circ.toFixed(1)}`
}

// Matplotlib fallback
function formatPriceList(raw) {
  if (raw === null || raw === undefined) return '—'
  const s = String(raw).trim()
  if (!s) return '—'
  return s.replace(/\$/g, '').replace(/\d+(?:\.\d+)?/g, m => '$' + m)
}

// ML maximum price changes
const maxMLReturn = computed(() => {
  const preds = state.analyticsData.predictions || []
  return Math.max(0.01, ...preds.map(p => Math.abs(p.predicted_return)))
})

// Canvas drawing algorithm
function drawPnlChart() {
  const canvas = pnlCanvasRef.value
  if (!canvas) return

  const series = state.analyticsData.pnl_series || []
  if (!series.length) return

  // Aggregate daily realized PnL
  const dayMap = {}
  series.forEach(r => {
    const d = r.day
    dayMap[d] = (dayMap[d] || 0) + parseFloat(r.realized_pnl || 0)
  })
  const days = Object.keys(dayMap).sort()
  
  // Create cumulative values
  let cum = 0
  const vals = days.map(d => {
    cum += dayMap[d]
    return cum
  })

  // Resize canvas based on container width
  const W = canvas.parentElement.offsetWidth || 800
  const H = 180
  canvas.width = W
  canvas.height = H

  const ctx = canvas.getContext('2d')
  const pad = { l: 60, r: 20, t: 15, b: 30 }
  const iW = W - pad.l - pad.r
  const iH = H - pad.t - pad.b

  const mn = Math.min(0, ...vals)
  const mx = Math.max(0, ...vals)
  const range = mx - mn || 1

  const getX = i => pad.l + i * (iW / (days.length - 1 || 1))
  const getY = v => pad.t + iH - (((v - mn) / range) * iH)

  ctx.clearRect(0, 0, W, H)

  // Zero horizontal line
  ctx.strokeStyle = 'rgba(255, 255, 255, 0.08)'
  ctx.lineWidth = 1
  ctx.setLineDash([4, 4])
  ctx.beginPath()
  ctx.moveTo(pad.l, getY(0))
  ctx.lineTo(W - pad.r, getY(0))
  ctx.stroke()
  ctx.setLineDash([])

  if (days.length < 2) return

  // Fill gradient path
  const grad = ctx.createLinearGradient(0, pad.t, 0, H - pad.b)
  const isPositive = vals[vals.length - 1] >= 0
  grad.addColorStop(0, isPositive ? 'rgba(16, 185, 129, 0.18)' : 'rgba(239, 68, 68, 0.18)')
  grad.addColorStop(1, 'transparent')
  
  ctx.beginPath()
  ctx.moveTo(getX(0), getY(vals[0]))
  vals.forEach((v, i) => ctx.lineTo(getX(i), getY(v)))
  ctx.lineTo(getX(vals.length - 1), H - pad.b)
  ctx.lineTo(getX(0), H - pad.b)
  ctx.closePath()
  ctx.fillStyle = grad
  ctx.fill()

  // Trend line drawing
  ctx.beginPath()
  ctx.strokeStyle = isPositive ? 'var(--color-green)' : 'var(--color-red)'
  ctx.lineWidth = 2
  vals.forEach((v, i) => {
    if (i === 0) ctx.moveTo(getX(i), getY(v))
    else ctx.lineTo(getX(i), getY(v))
  })
  ctx.stroke()

  // Drawing Y ticks/labels
  ctx.fillStyle = 'var(--text-muted)'
  ctx.font = '10px var(--font-sans)'
  ctx.textAlign = 'right'
  
  const yTicks = [mn, 0, (mx + mn) / 2, mx]
  // Eliminate duplicates
  const uniqueTicks = [...new Set(yTicks)].sort((a,b)=>a-b)
  
  uniqueTicks.forEach(v => {
    ctx.fillText('$' + v.toFixed(0), pad.l - 8, getY(v) + 3)
  })

  // Drawing X label dates
  ctx.textAlign = 'center'
  if (days.length) {
    ctx.fillText(days[0].slice(5), getX(0), H - 8)
    ctx.fillText(days[days.length - 1].slice(5), getX(days.length - 1), H - 8)
  }
}

// Strategy parameters mapping
const strategyLotMeta = computed(() => {
  const d = state.lotsData || {}
  return {
    CS75: { target: d.CS75?.target, max: d.CS75?.max },
    CS7:  { target: d.CS7?.target, max: d.CS7?.max },
    TT45: { target: d.TT45?.target, max: d.TT45?.max },
    WHEEL: { max: d.WHEEL?.max },
    HermesAlpha: { max: d.HermesAlpha?.max }
  }
})

// Sort support/resistance clusters
function getSupportLevels(sym) {
  const levels = state.keyLevelsData[sym]?.key_levels || []
  return levels.filter(l => l.type === 'support').sort((a, b) => b.price - a.price)
}

function getResistanceLevels(sym) {
  const levels = state.keyLevelsData[sym]?.key_levels || []
  return levels.filter(l => l.type === 'resistance').sort((a, b) => a.price - b.price)
}

function getMaxStrength(sym) {
  const levels = state.keyLevelsData[sym]?.key_levels || []
  return Math.max(1, ...levels.map(l => l.strength))
}
</script>

<template>
  <div class="analytics-tabs-container">
    <!-- Sub-tab Bar -->
    <div class="tab-bar-analytics">
      <button
        class="tab-btn"
        :class="{ active: activeTab === 'performance' }"
        @click="switchTab('performance')"
      ><Icon name="chart-line" :size="15" /> Performance</button>
      <button
        class="tab-btn"
        :class="{ active: activeTab === 'predictions' }"
        @click="switchTab('predictions')"
      ><Icon name="bot" :size="15" /> ML Predictions</button>
      <button
        class="tab-btn"
        :class="{ active: activeTab === 'keylevels' }"
        @click="switchTab('keylevels')"
      ><Icon name="sliders" :size="15" /> Key Levels</button>
      <button
        class="tab-btn"
        :class="{ active: activeTab === 'logic' }"
        @click="switchTab('logic')"
      ><Icon name="calculator" :size="15" /> Strategy Logic</button>
      <button
        class="tab-btn"
        :class="{ active: activeTab === 'trades' }"
        @click="switchTab('trades')"
      ><Icon name="clipboard" :size="15" /> Trade History</button>

      <div class="header-refresh-box">
        <span class="last-updated-text" v-if="state.lastUpdated">
          Updated {{ state.lastUpdated }}
        </span>
        <button class="btn-ghost btn-sm" @click="loadData"><Icon name="refresh-cw" :size="13" /> Refresh</button>
      </div>
    </div>

    <!-- PERFORMANCE PANEL -->
    <div v-show="activeTab === 'performance'" class="panel-view">
      <div v-if="state.analyticsData.error" class="err-box">
        Analytics Query Failed: {{ state.analyticsData.error }}
      </div>

      <!-- Aggregated KPI Row -->
      <div class="grid-4 kpi-row">
        <div class="stat-tile">
          <span class="lbl">Total Realized P&amp;L</span>
          <span class="val" :style="{ color: pnlColor(performanceSummary.totalPnl) }">
            {{ fmt$(performanceSummary.totalPnl) }}
          </span>
          <span class="sub">all closed trades</span>
        </div>
        <div class="stat-tile">
          <span class="lbl">Win Rate</span>
          <span class="val" :class="performanceSummary.winRate >= 50 ? 'text-green' : 'text-red'">
            {{ performanceSummary.winRate }}%
          </span>
          <span class="sub">
            {{ performanceSummary.totalWin }} W / {{ performanceSummary.totalLoss }} L ({{ performanceSummary.totalClosed }} closed)
          </span>
        </div>
        <div class="stat-tile">
          <span class="lbl">Avg P&amp;L / Trade</span>
          <span class="val" :style="{ color: pnlColor(performanceSummary.avgPnl) }">
            {{ fmt$(performanceSummary.avgPnl) }}
          </span>
          <span class="sub">closed trades aggregate</span>
        </div>
        <div class="stat-tile">
          <span class="lbl">Open Positions</span>
          <span class="val text-blue">
            {{ performanceSummary.openCount }}
          </span>
          <span class="sub">across active strategies</span>
        </div>
      </div>

      <!-- Realized P&L Line Chart -->
      <div class="card chart-card-container">
        <div class="card-header">
          <span>Cumulative Realized P&amp;L — Last 60 Days</span>
        </div>
        <div class="card-body">
          <div v-show="!state.analyticsData.pnl_series?.length" class="no-data">
            No P&amp;L history stored. Trades will display once positions are closed.
          </div>
          <canvas ref="pnlCanvasRef" height="180" class="pnl-chart-canvas"></canvas>
        </div>
      </div>

      <!-- Per-Strategy Performance Cards -->
      <div class="section-title-bar">Strategy Performance Breakdown</div>
      <div class="grid-2 strategy-performance-grid">
        <div 
          v-for="sid in Object.keys(state.analyticsData.performance || {})" 
          :key="sid"
          class="card strategy-card"
        >
          <div class="card-header border-left-indicator" :style="{ borderLeftColor: STRAT_COLORS[sid] || 'var(--color-blue)' }">
            <span>{{ sid }}</span>
            <span class="header-open-badge">{{ state.analyticsData.performance[sid].open_count }} active positions</span>
          </div>
          <div class="card-body strat-card-body">
            <div class="strat-metric-ring-wrap">
              <svg width="74" height="74" viewBox="0 0 160 160" class="radial-ring">
                <circle cx="80" cy="80" r="65" fill="none" stroke="rgba(255, 255, 255, 0.04)" stroke-width="16"/>
                <circle 
                  cx="80" 
                  cy="80" 
                  r="65" 
                  fill="none" 
                  :stroke="STRAT_COLORS[sid] || 'var(--color-blue)'" 
                  stroke-width="16"
                  :stroke-dasharray="getSvgDashArray(state.analyticsData.performance[sid].win_rate)"
                  stroke-linecap="round" 
                  transform="rotate(-90 80 80)"
                />
                <text 
                  x="80" 
                  y="75" 
                  text-anchor="middle" 
                  dominant-baseline="central"
                  :fill="STRAT_COLORS[sid] || 'var(--color-blue)'" 
                  font-size="28" 
                  font-weight="800"
                >{{ Math.round(state.analyticsData.performance[sid].win_rate) }}%</text>
                <text 
                  x="80" 
                  y="108" 
                  text-anchor="middle" 
                  dominant-baseline="central"
                  fill="var(--text-muted)" 
                  font-size="14" 
                  font-weight="600"
                >WIN RATE</text>
              </svg>
              
              <div class="strat-metrics-details">
                <div class="detail-metric-item">
                  <span class="m-lbl">Total P&amp;L</span>
                  <span class="m-val" :style="{ color: pnlColor(state.analyticsData.performance[sid].total_pnl) }">
                    {{ fmt$(state.analyticsData.performance[sid].total_pnl) }}
                  </span>
                </div>
                <div class="detail-metric-item">
                  <span class="m-lbl">Avg / Trade</span>
                  <span class="m-val" :style="{ color: pnlColor(state.analyticsData.performance[sid].avg_pnl) }">
                    {{ fmt$(state.analyticsData.performance[sid].avg_pnl) }}
                  </span>
                </div>
                <div class="detail-metric-item">
                  <span class="m-lbl">Best Trade</span>
                  <span class="m-val text-green">{{ fmt$(state.analyticsData.performance[sid].best_trade) }}</span>
                </div>
                <div class="detail-metric-item">
                  <span class="m-lbl">Worst Trade</span>
                  <span class="m-val text-red">{{ fmt$(state.analyticsData.performance[sid].worst_trade) }}</span>
                </div>
                <div class="detail-metric-item">
                  <span class="m-lbl">Trades Count</span>
                  <span class="m-val">{{ state.analyticsData.performance[sid].total_closed }} closed</span>
                </div>
                <div class="detail-metric-item">
                  <span class="m-lbl">Winners / Losers</span>
                  <span class="m-val">
                    <span class="text-green">{{ state.analyticsData.performance[sid].winners }}</span> / 
                    <span class="text-red">{{ state.analyticsData.performance[sid].losers }}</span>
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Open Positions Table -->
      <div class="section-title-bar">Current Open Positions</div>
      <div class="card table-card">
        <div class="card-body no-padding overflow-x">
          <div v-if="!state.analyticsData.open_trades?.length" class="no-data">
            No active open trades in broker account.
          </div>
          <table v-else class="tbl">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Strategy</th>
                <th>Side</th>
                <th>Options Contract Legs</th>
                <th>Short Strike</th>
                <th>Long Strike</th>
                <th>Width</th>
                <th>Lots</th>
                <th>Credit</th>
                <th>Expiry</th>
                <th>Age</th>
                <th>AI</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="t in state.analyticsData.open_trades" :key="t.id">
                <td><strong class="text-blue">{{ t.symbol }}</strong></td>
                <td>{{ t.strategy_id }}</td>
                <td><span class="tag" :class="t.side_type">{{ t.side_type || '—' }}</span></td>
                <td class="font-mono text-muted text-xs">
                  <div>{{ t.short_leg ? t.short_leg.slice(-12) : '—' }}</div>
                  <div>{{ t.long_leg ? t.long_leg.slice(-12) : '—' }}</div>
                </td>
                <td>{{ t.short_strike != null ? '$' + t.short_strike.toFixed(2) : '—' }}</td>
                <td>{{ t.long_strike != null ? '$' + t.long_strike.toFixed(2) : '—' }}</td>
                <td>{{ t.width != null ? '$' + t.width.toFixed(2) : '—' }}</td>
                <td>{{ t.lots }}</td>
                <td class="text-green">${{ (t.entry_credit || 0).toFixed(2) }}</td>
                <td class="text-xs">{{ t.expiry || '—' }}</td>
                <td class="text-xs text-muted">{{ getRelativeTime(t.opened_at) }}</td>
                <td><span v-if="t.ai_authored" class="tag ai">AI</span><span v-else>—</span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- ML PREDICTIONS PANEL -->
    <div v-show="activeTab === 'predictions'" class="panel-view">
      <div class="card prediction-card">
        <div class="card-header">
          <span>XGBoost Return Forecasting Models</span>
          <button 
            class="btn-ghost btn-sm" 
            :disabled="isMLTriggering"
            @click="triggerMLRetrain"
          >
            <Icon v-if="!isMLTriggering" name="bolt" :size="14" />
            {{ isMLTriggering ? 'Triggering...' : 'Force Retrain & Predict' }}
          </button>
        </div>
        <div class="card-body">
          <p class="tab-sec-desc">
            Hermes trains an independent <strong>XGBoost regressor</strong> per watchlist symbol, computing 10 volume-normalized alpha parameters. Predictions target next-day forward return. Trains weekly.
          </p>

          <div v-if="state.analyticsData.ml_last_error" class="err-box text-xs">
            <strong>ML Error/Warning:</strong> {{ state.analyticsData.ml_last_error }}
          </div>

          <div v-if="!state.analyticsData.predictions?.length" class="no-data">
            No predictions generated. Models require at least 60 daily bars of symbol data to train.
          </div>
          <div v-else class="predictions-bars-wrap">
            <div 
              v-for="p in state.analyticsData.predictions" 
              :key="p.symbol"
              class="pred-row-card"
            >
              <span class="pred-sym-title">{{ p.symbol }}</span>
              <div class="pred-bar-container">
                <div class="pred-bar-track">
                  <div 
                    class="pred-bar-fill" 
                    :style="{ 
                      width: (Math.abs(p.predicted_return) / maxMLReturn * 100).toFixed(1) + '%',
                      backgroundColor: p.predicted_return >= 0 ? 'var(--color-green)' : 'var(--color-red)' 
                    }"
                  ></div>
                </div>
              </div>
              <div class="pred-meta-info">
                <span>Spot: <strong>${{ p.spot.toFixed(2) }}</strong></span>
                <span><Icon name="arrow-right" :size="12" /> Target: <strong :style="{ color: p.predicted_return >= 0 ? 'var(--positive)' : 'var(--negative)' }">${{ p.predicted_price.toFixed(2) }}</strong></span>
                <span class="pred-pct" :style="{ color: p.predicted_return >= 0 ? 'var(--color-green)' : 'var(--color-red)' }">
                  ({{ p.predicted_return >= 0 ? '+' : '' }}{{ (p.predicted_return * 100).toFixed(3) }}%)
                </span>
                <span class="pred-time-tag text-muted text-xs">{{ getRelativeTime(p.as_of) }}</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Feature Dictionary -->
      <div class="section-title-bar">Feature Dictionary — the 10 Alpha Inputs</div>
      <div class="grid-3 feat-grid">
        <div class="feat-card">
          <div class="feat-name">① Overnight Gap</div>
          <div class="feat-val">(open − prev_close) / prev_close</div>
          <div class="feat-desc">Measures the gap at open vs. prior close. Large gaps often mean-revert intraday.</div>
        </div>
        <div class="feat-card">
          <div class="feat-name">② Vol-Norm 5d Momentum</div>
          <div class="feat-val">(close − close₋₅) / (σ₂₀ · close₋₅)</div>
          <div class="feat-desc">5-day price momentum scaled by 20-day realized vol. Removes vol-regime bias.</div>
        </div>
        <div class="feat-card">
          <div class="feat-name">③ SPY Beta Residual</div>
          <div class="feat-val">ret − β₆₀ · SPY_ret</div>
          <div class="feat-desc">Symbol return minus SPY beta contribution. Isolates idiosyncratic alpha from market drift.</div>
        </div>
        <div class="feat-card">
          <div class="feat-name">④ Intraday Return</div>
          <div class="feat-val">(close − open) / open</div>
          <div class="feat-desc">Full day return from open to close. Captures conviction.</div>
        </div>
        <div class="feat-card">
          <div class="feat-name">⑤ VWAP Distance</div>
          <div class="feat-val">(close − VWAP₃:₅₉) / close</div>
          <div class="feat-desc">Deviation from closing-period VWAP. Positive = closed above institutional average.</div>
        </div>
        <div class="feat-card">
          <div class="feat-name">⑥ Range Position</div>
          <div class="feat-val">(close − low) / (high − low)</div>
          <div class="feat-desc">Where close sits in day's range. 1.0 = close at high; 0.0 = close at low.</div>
        </div>
        <div class="feat-card">
          <div class="feat-name">⑦ Volume Z-Score (20d)</div>
          <div class="feat-val">(vol − SMA₂₀) / σ₂₀</div>
          <div class="feat-desc">Unusual volume vs 20-day average. High z-score = elevated conviction.</div>
        </div>
        <div class="feat-card">
          <div class="feat-name">⑧ Last-30min Volume %</div>
          <div class="feat-val">vol₁₅:₃₀–₁₆:₀₀ / vol_total</div>
          <div class="feat-desc">Fraction of volume traded in the closing 30 mins. Smart money acts late.</div>
        </div>
        <div class="feat-card">
          <div class="feat-name">⑨ Realized Vol 5d (ann.)</div>
          <div class="feat-val">σ(log_ret₅) × √252</div>
          <div class="feat-desc">Short-term annualised volatility. Rising vol precedes mean-reversion.</div>
        </div>
        <div class="feat-card w-full" style="grid-column: span 3">
          <div class="feat-name">⑩ Seasonality</div>
          <div class="feat-val">day_of_week, month</div>
          <div class="feat-desc">Calendar indices. Captures Monday effect, January effect, OpEx patterns.</div>
        </div>
      </div>
    </div>

    <!-- KEY LEVELS PANEL -->
    <div v-show="activeTab === 'keylevels'" class="panel-view">
      <div class="card key-levels-card">
        <div class="card-header">
          <span>K-Means S/R Pivots Clustering</span>
          <div class="horizon-toggles">
            <button 
              v-for="h in ['3m', '6m', '1y']" 
              :key="h" 
              class="horizon-btn"
              :class="{ active: state.keyLevelsHorizon === h }"
              @click="handleHorizonChange(h)"
            >{{ h.toUpperCase() }}</button>
          </div>
        </div>
        <div class="card-body">
          <p class="tab-sec-desc">
            Support and resistance pivots are clustered into 6 distinct price levels. Pivots are weighted by <strong>Volume × Recency²</strong> to prioritize high-conviction, recent levels.
          </p>

          <div v-if="state.keyLevelsLoading" class="loading">
            Loading {{ state.keyLevelsHorizon.toUpperCase() }} Analysis Pivots...
          </div>
          <div v-else-if="state.keyLevelsError" class="err-box">
            Failed to load pivots: {{ state.keyLevelsError }}
          </div>
          <div v-else-if="!Object.keys(state.keyLevelsData).length" class="no-data">
            No clustering data available.
          </div>
          <div v-else class="keylevels-symbols-list">
            <div 
              v-for="sym in Object.keys(state.keyLevelsData)" 
              :key="sym"
              class="card key-levels-sym-card"
            >
              <div class="card-header">
                <div class="k-level-spot-info">
                  <span class="text-blue font-bold font-lg">{{ sym }}</span>
                  <span class="text-xs text-muted">Spot: <strong>${{ (state.keyLevelsData[sym]?.current_price || 0).toFixed(2) }}</strong></span>
                  <span class="k-levels-period-badge">
                    {{ (state.keyLevelsData[sym]?.period || state.keyLevelsHorizon).toUpperCase() }} · {{ state.keyLevelsData[sym]?.samples || 0 }}d analyzed
                  </span>
                </div>
              </div>
              <div class="card-body no-padding grid-2">
                
                <!-- Support table -->
                <div class="k-levels-sub-panel border-right">
                  <div class="k-levels-sub-title text-green">Support Zones</div>
                  <table class="tbl text-xs">
                    <thead>
                      <tr>
                        <th>Price</th>
                        <th>Strength (touches)</th>
                        <th>Distance</th>
                        <th>POP</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="l in getSupportLevels(sym)" :key="l.price">
                        <td class="text-green font-bold">${{ l.price.toFixed(2) }}</td>
                        <td>
                          <div class="strength-bar-col">
                            <span class="touches-lbl">{{ l.strength }} touches</span>
                            <div class="strength-bar-track">
                              <div class="strength-bar-fill green-fill" :style="{ width: (l.strength / getMaxStrength(sym) * 100) + '%' }"></div>
                            </div>
                          </div>
                        </td>
                        <td class="text-muted">
                          -{{ (state.keyLevelsData[sym].current_price > 0 ? 100 * (state.keyLevelsData[sym].current_price - l.price) / state.keyLevelsData[sym].current_price : 0).toFixed(2) }}%
                        </td>
                        <td class="font-bold" :style="{ color: l.pop >= 0.75 ? 'var(--color-green)' : l.pop >= 0.6 ? 'var(--color-yellow)' : 'var(--text-muted)' }">
                          {{ (l.pop * 100).toFixed(1) }}%
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>

                <!-- Resistance table -->
                <div class="k-levels-sub-panel">
                  <div class="k-levels-sub-title text-red">Resistance Zones</div>
                  <table class="tbl text-xs">
                    <thead>
                      <tr>
                        <th>Price</th>
                        <th>Strength (touches)</th>
                        <th>Distance</th>
                        <th>POP</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="l in getResistanceLevels(sym)" :key="l.price">
                        <td class="text-red font-bold">${{ l.price.toFixed(2) }}</td>
                        <td>
                          <div class="strength-bar-col">
                            <span class="touches-lbl">{{ l.strength }} touches</span>
                            <div class="strength-bar-track">
                              <div class="strength-bar-fill red-fill" :style="{ width: (l.strength / getMaxStrength(sym) * 100) + '%' }"></div>
                            </div>
                          </div>
                        </td>
                        <td class="text-muted">
                          +{{ (state.keyLevelsData[sym].current_price > 0 ? 100 * (l.price - state.keyLevelsData[sym].current_price) / state.keyLevelsData[sym].current_price : 0).toFixed(2) }}%
                        </td>
                        <td class="font-bold" :style="{ color: l.pop >= 0.75 ? 'var(--color-green)' : l.pop >= 0.6 ? 'var(--color-yellow)' : 'var(--text-muted)' }">
                          {{ (l.pop * 100).toFixed(1) }}%
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>

              </div>
              
              <div class="keylevels-card-footer text-muted text-xs">
                <span>Realized Vol (21d): <strong>{{ ((state.keyLevelsData[sym]?.current_vol || 0) * 100).toFixed(1) }}%</strong></span>
                <span>Avg Vol ({{ state.keyLevelsData[sym]?.period || 'N/A' }}): <strong>{{ ((state.keyLevelsData[sym]?.avg_vol || 0) * 100).toFixed(1) }}%</strong></span>
                <span>Samples: <strong>{{ state.keyLevelsData[sym]?.samples || 0 }} trading days</strong></span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- STRATEGY LOGIC PANEL -->
    <div v-show="activeTab === 'logic'" class="panel-view">
      <p class="tab-sec-desc">
        Hermes trading logic runs 4 credit-spread strategies in sequential order of execution priority, writing allocations to the money manager dynamically.
      </p>

      <!-- CS75 Card -->
      <div class="card logic-card">
        <h3 class="logic-title">
          <span class="p-pill pill-blue">P1</span> CS75 — Credit Spread (Institutional POP)
        </h3>
        <div class="logic-summary-row">
          <div class="logic-summary-box">
            <span class="l-lbl">Strategy Type</span>
            <span class="l-val">Iron Condor / Credit Spreads</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">DTE Target Window</span>
            <span class="l-val">39 – 45 Days</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Spread Width</span>
            <span class="l-val">$5.00 width</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Target Lots</span>
            <span class="l-val">{{ strategyLotMeta.CS75?.target ?? '—' }}</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Max Lots</span>
            <span class="l-val">{{ strategyLotMeta.CS75?.max ?? '—' }}</span>
          </div>
        </div>
        <div class="logic-steps">
          <div class="logic-step"><span class="step-num">1</span><span><strong>Mode A (Initial Entry):</strong> Find expiry date inside DTE target window (39–45 days).</span></div>
          <div class="logic-step"><span class="step-num">2</span><span><strong>Mode B (Completions):</strong> Re-scans the options chain for missing sides on partially-filled condors (14-45 DTE).</span></div>
          <div class="logic-step"><span class="step-num">3</span><span><strong>K-Means Clustering:</strong> Feeds pivot walls to model. Calculates POP. Picks strike closest to 75% POP.</span></div>
          <div class="logic-step"><span class="step-num">4</span><span><strong>Premium:</strong> Verifies credit is &gt;= $1.25 for condor entry, and &gt;= $1.00 for completions.</span></div>
          <div class="formula">TP: Close at 50% max profit (DTE &gt;= 21) or 75% profit (DTE &lt; 21). SL: 2.5× entry debit. Exit: hard close at 8 DTE.</div>
        </div>
      </div>

      <!-- CS7 Card -->
      <div class="card logic-card">
        <h3 class="logic-title">
          <span class="p-pill pill-yellow">P2</span> CS7 — 7-Day Credit Spread
        </h3>
        <div class="logic-summary-row">
          <div class="logic-summary-box">
            <span class="l-lbl">Strategy Type</span>
            <span class="l-val">Short-term Spreads</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">DTE Target Window</span>
            <span class="l-val">Fixed 7 Days</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Spread Width</span>
            <span class="l-val">$1.00 width</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Target Lots</span>
            <span class="l-val">{{ strategyLotMeta.CS7?.target ?? '—' }}</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Max Lots</span>
            <span class="l-val">{{ strategyLotMeta.CS7?.max ?? '—' }}</span>
          </div>
        </div>
        <div class="logic-steps">
          <div class="logic-step"><span class="step-num">1</span><span><strong>Mode A (Initial Entry):</strong> Find expiry at exactly 7 DTE.</span></div>
          <div class="logic-step"><span class="step-num">2</span><span><strong>Mode B (Completions):</strong> Re-scans the options chain for missing sides on partially-filled condors (4-7 DTE).</span></div>
          <div class="logic-step"><span class="step-num">3</span><span><strong>K-Means Clustering:</strong> Feeds pivot walls to model. Calculates POP based on 3-month lookback. Require &gt; 75% POP on strikes.</span></div>
          <div class="logic-step"><span class="step-num">4</span><span><strong>Premium:</strong> Verifies min_credit = width × 0.12 → $1 × 0.12 = $0.12 minimum.</span></div>
          <div class="formula">TP: credit &lt;= $0.02. SL: 3× credit debit.</div>
        </div>
      </div>

      <!-- TT45 Card -->
      <div class="card logic-card">
        <h3 class="logic-title">
          <span class="p-pill pill-orange">P3</span> TT45 — TastyTrade 16-Delta Spread
        </h3>
        <div class="logic-summary-row">
          <div class="logic-summary-box">
            <span class="l-lbl">Strategy Type</span>
            <span class="l-val">Delta directional</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">DTE Target Window</span>
            <span class="l-val">30 – 60 Days</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Spread Width</span>
            <span class="l-val">$5.00 width</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Target Lots</span>
            <span class="l-val">{{ strategyLotMeta.TT45?.target ?? '—' }}</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Max Lots</span>
            <span class="l-val">{{ strategyLotMeta.TT45?.max ?? '—' }}</span>
          </div>
        </div>
        <div class="logic-steps">
          <div class="logic-step"><span class="step-num">1</span><span>Scan 30–60 DTE. Select short strike at delta ~0.16 (±0.05). Statistically yields OTM probabilities matching 84%.</span></div>
          <div class="logic-step"><span class="step-num">2</span><span>Buy wing protection at short strike ± $5. Require net credit &gt; $0.</span></div>
          <div class="formula">Time Exit: close at 21 DTE. Challenged: exit if short contract delta exceeds 0.30 (ITM protection).</div>
        </div>
      </div>

      <!-- WHEEL Card -->
      <div class="card logic-card">
        <h3 class="logic-title">
          <span class="p-pill pill-purple">P4</span> WHEEL — Equity Rotation
        </h3>
        <div class="logic-summary-row">
          <div class="logic-summary-box">
            <span class="l-lbl">Strategy Type</span>
            <span class="l-val">Cash-Secured Puts / Covered Calls</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">DTE Target Window</span>
            <span class="l-val">30 – 45 Days</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Strike Delta</span>
            <span class="l-val">Delta ~0.30</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Max Lots</span>
            <span class="l-val">{{ strategyLotMeta.WHEEL?.max ?? '—' }}</span>
          </div>
        </div>
        <div class="logic-steps">
          <div class="logic-step"><span class="step-num">1</span><span>Check shares. If holding stock, sell Covered Calls up to shares/100 count.</span></div>
          <div class="logic-step"><span class="step-num">2</span><span>If cash available, sell Cash-Secured Puts to top off up to max lots threshold.</span></div>
          <div class="formula">Wanted Calls = min(shares/100, max_lots) − open_calls. Wanted Puts = max_lots − (open_calls + open_puts). Roll ITM if DTE &lt; 7.</div>
        </div>
      </div>

      <!-- HermesAlpha Card -->
      <div class="card logic-card">
        <h3 class="logic-title">
          <span class="p-pill pill-green">P5</span> HermesAlpha — Autonomous LLM Book
        </h3>
        <div class="logic-summary-row">
          <div class="logic-summary-box">
            <span class="l-lbl">Strategy Type</span>
            <span class="l-val">Self-Directed LLM-Driven Setups</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">DTE Target Window</span>
            <span class="l-val">Overseer Defined</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Strike Selection</span>
            <span class="l-val">LLM Discretionary</span>
          </div>
          <div class="logic-summary-box">
            <span class="l-lbl">Max Lots</span>
            <span class="l-val">{{ strategyLotMeta.HermesAlpha?.max ?? '—' }}</span>
          </div>
        </div>
        <div class="logic-steps">
          <div class="logic-step"><span class="step-num">1</span><span>Evaluates market structure, technical indicators, and option chain details through the LLM Overseer.</span></div>
          <div class="logic-step"><span class="step-num">2</span><span>Selects custom credit spread or option structures based on the LLM's high-conviction ideas.</span></div>
          <div class="formula">Runs autonomously when Autonomy is set to Autonomous. Sized within the alpha max lots threshold.</div>
        </div>
      </div>
    </div>

    <!-- TRADE HISTORY PANEL -->
    <div v-show="activeTab === 'trades'" class="panel-view">
      <div class="card table-card">
        <div class="card-header">
          <span>Closed Trade Log — Last 50 Closed Trades</span>
        </div>
        <div class="card-body no-padding overflow-x">
          <div v-if="!state.analyticsData.closed_trades?.length" class="no-data">
            No closed trades recorded.
          </div>
          <table v-else class="tbl">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Strategy</th>
                <th>Side</th>
                <th>Lots</th>
                <th>Entry Credit</th>
                <th>Realized P&amp;L</th>
                <th>Exit Reason</th>
                <th>Expiry</th>
                <th>Opened</th>
                <th>Closed</th>
                <th>AI</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="t in state.analyticsData.closed_trades" :key="t.id">
                <td><strong class="text-blue">{{ t.symbol }}</strong></td>
                <td>{{ t.strategy_id }}</td>
                <td><span class="tag" :class="t.side_type">{{ t.side_type || '—' }}</span></td>
                <td>{{ t.lots }}</td>
                <td>${{ (t.entry_credit || 0).toFixed(2) }}</td>
                <td 
                  class="font-bold" 
                  :style="{ color: pnlColor(t.pnl) }"
                  :title="t.exit_price != null ? 'Exit price: $' + t.exit_price.toFixed(2) : ''"
                >
                  {{ t.pnl == null ? '—' : fmt$(t.pnl) }}
                </td>
                <td>
                  <span class="reason-pill" :title="t.close_tag || ''">
                    {{ t.close_reason || '—' }}
                  </span>
                </td>
                <td class="text-xs">{{ t.expiry || '—' }}</td>
                <td class="text-xs text-muted">{{ getRelativeTime(t.opened_at) }}</td>
                <td class="text-xs text-muted">{{ getRelativeTime(t.closed_at) }}</td>
                <td><span v-if="t.ai_authored" class="tag ai">AI</span><span v-else>—</span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

  </div>
</template>

<style scoped>
.analytics-tabs-container {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.tab-bar-analytics {
  display: flex;
  align-items: center;
  border-bottom: 1px solid var(--border-color);
  margin-bottom: 10px;
  flex-wrap: wrap;
  gap: 6px;
}

.tab-btn {
  background: transparent;
  color: var(--text-muted);
  border: none;
  border-bottom: 2px solid transparent;
  border-radius: 0;
  padding: 10px 16px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.15s ease;
}
.tab-btn:hover {
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.02);
}
.tab-btn.active {
  color: var(--color-blue);
  border-bottom-color: var(--color-blue);
  background: rgba(59, 130, 246, 0.05);
}

.header-refresh-box {
  margin-left: auto;
  display: flex;
  align-items: center;
  gap: 12px;
}
.last-updated-text {
  font-size: 11px;
  color: var(--text-muted);
}

.panel-view {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.grid-4 {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
}
.grid-3 {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}
.grid-2 {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 16px;
}

@media (max-width: 1024px) {
  .grid-4, .grid-3 {
    grid-template-columns: repeat(2, 1fr);
  }
}
@media (max-width: 640px) {
  .grid-4, .grid-3, .grid-2 {
    grid-template-columns: 1fr;
  }
  .tab-bar-analytics {
    flex-direction: column;
    align-items: stretch;
  }
  .header-refresh-box {
    margin-left: 0;
    justify-content: space-between;
    margin-top: 10px;
  }
}

.stat-tile {
  background: var(--surface-glass);
  backdrop-filter: blur(12px);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  padding: 20px;
  text-align: center;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
}
.stat-tile .lbl {
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  color: var(--text-muted);
  letter-spacing: 0.05em;
  margin-bottom: 8px;
}
.stat-tile .val {
  font-size: 26px;
  font-weight: 800;
  letter-spacing: -0.02em;
  margin-bottom: 4px;
}
.stat-tile .sub {
  font-size: 11px;
  color: var(--text-muted);
}

.chart-card-container {
  width: 100%;
}

.pnl-chart-canvas {
  width: 100%;
  display: block;
}

.section-title-bar {
  font-size: 14px;
  font-weight: 700;
  color: var(--text-primary);
  margin-top: 10px;
  border-left: 3px solid var(--color-blue);
  padding-left: 10px;
}

.strategy-card {
  background: var(--surface-glass);
}

.border-left-indicator {
  border-left: 3px solid var(--color-blue);
  padding-left: 17px;
}

.header-open-badge {
  font-size: 11px;
  font-weight: 400;
  color: var(--text-muted);
}

.strat-card-body {
  padding: 20px;
}

.strat-metric-ring-wrap {
  display: flex;
  align-items: center;
  gap: 24px;
}

@media (max-width: 500px) {
  .strat-metric-ring-wrap {
    flex-direction: column;
    text-align: center;
  }
}

.radial-ring {
  flex-shrink: 0;
}

.strat-metrics-details {
  flex-grow: 1;
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}

.detail-metric-item {
  display: flex;
  flex-direction: column;
}
.detail-metric-item .m-lbl {
  font-size: 10px;
  color: var(--text-muted);
  font-weight: 600;
}
.detail-metric-item .m-val {
  font-size: 14px;
  font-weight: 700;
  margin-top: 2px;
}

.table-card {
  width: 100%;
}

.overflow-x {
  overflow-x: auto;
}

.text-blue {
  color: var(--color-blue);
}
.text-green {
  color: var(--color-green);
}
.text-red {
  color: var(--color-red);
}

.reason-pill {
  font-size: 11px;
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid var(--border-color);
  padding: 2px 8px;
  border-radius: 4px;
  cursor: help;
  white-space: nowrap;
}

.prediction-card {
  width: 100%;
}

.predictions-bars-wrap {
  display: flex;
  flex-direction: column;
  gap: 14px;
  margin-top: 10px;
}

.pred-row-card {
  display: flex;
  align-items: center;
  gap: 16px;
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid var(--border-color);
  padding: 12px 16px;
  border-radius: var(--radius-md);
}

@media (max-width: 768px) {
  .pred-row-card {
    flex-direction: column;
    align-items: flex-start;
    gap: 8px;
  }
  .pred-bar-container {
    width: 100%;
  }
  .pred-meta-info {
    width: 100%;
    justify-content: space-between;
  }
}

.pred-sym-title {
  font-size: 15px;
  font-weight: 800;
  color: var(--color-blue);
  width: 60px;
  flex-shrink: 0;
}

.pred-bar-container {
  flex-grow: 1;
}

.pred-bar-track {
  height: 6px;
  background: rgba(255, 255, 255, 0.06);
  border-radius: 9999px;
  overflow: hidden;
}

.pred-bar-fill {
  height: 100%;
  border-radius: 9999px;
  transition: width 0.4s ease;
}

.pred-meta-info {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 12px;
}
.pred-pct {
  font-weight: 700;
  background: rgba(255, 255, 255, 0.02);
  padding: 1px 6px;
  border-radius: 4px;
}
.pred-time-tag {
  color: var(--text-muted);
}

.feat-card {
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid var(--border-color);
  padding: 14px;
  border-radius: var(--radius-md);
}
.feat-name {
  font-size: 12px;
  font-weight: 700;
  margin-bottom: 4px;
  color: var(--text-primary);
}
.feat-val {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--color-yellow);
  margin-bottom: 8px;
}
.feat-desc {
  font-size: 11px;
  color: var(--text-muted);
  line-height: 1.4;
}

.horizon-toggles {
  display: flex;
  background: rgba(0, 0, 0, 0.2);
  border-radius: var(--radius-md);
  padding: 2px;
}

.horizon-btn {
  background: transparent;
  color: var(--text-muted);
  border: none;
  font-size: 11px;
  padding: 4px 10px;
  border-radius: 4px;
}
.horizon-btn.active {
  background: var(--color-blue);
  color: #060913;
  font-weight: 700;
}

.keylevels-symbols-list {
  display: flex;
  flex-direction: column;
  gap: 16px;
  margin-top: 16px;
}

.key-levels-sym-card {
  background: rgba(0, 0, 0, 0.15);
}

.k-level-spot-info {
  display: flex;
  align-items: baseline;
  gap: 14px;
  flex-wrap: wrap;
}

.k-levels-period-badge {
  background: rgba(59, 130, 246, 0.1);
  color: var(--color-blue);
  border: 1px solid rgba(59, 130, 246, 0.2);
  padding: 1px 8px;
  border-radius: 4px;
  font-size: 10px;
  font-weight: 700;
}

.k-levels-sub-panel {
  padding: 20px;
}
.k-levels-sub-panel.border-right {
  border-right: 1px solid var(--border-color);
}

@media (max-width: 768px) {
  .k-levels-sub-panel.border-right {
    border-right: none;
    border-bottom: 1px solid var(--border-color);
  }
}

.k-levels-sub-title {
  font-size: 11px;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin-bottom: 12px;
}

.strength-bar-col {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.touches-lbl {
  font-size: 10px;
  color: var(--text-muted);
}

.strength-bar-track {
  width: 60px;
  height: 4px;
  background: rgba(255, 255, 255, 0.06);
  border-radius: 9999px;
  overflow: hidden;
}

.strength-bar-fill {
  height: 100%;
  border-radius: 9999px;
}
.strength-bar-fill.green-fill {
  background: var(--color-green);
}
.strength-bar-fill.red-fill {
  background: var(--color-red);
}

.keylevels-card-footer {
  display: flex;
  gap: 24px;
  padding: 10px 20px;
  background: rgba(255, 255, 255, 0.02);
  border-top: 1px solid var(--border-color);
  color: var(--text-muted);
}

.logic-card {
  background: var(--surface-glass);
  padding: 20px;
  margin-bottom: 16px;
}

.logic-title {
  font-size: 14px;
  font-weight: 800;
  margin-bottom: 14px;
  display: flex;
  align-items: center;
  gap: 10px;
}

.p-pill {
  font-size: 10px;
  font-weight: 700;
  padding: 2px 8px;
  border-radius: 4px;
  color: #060913;
}
.pill-blue { background: var(--color-blue); }
.pill-yellow { background: var(--color-yellow); }
.pill-orange { background: var(--color-orange); }
.pill-purple { background: var(--color-purple); }
.pill-green { background: var(--color-green); }

.logic-summary-row {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  margin-bottom: 16px;
}

.logic-summary-box {
  flex-grow: 1;
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid var(--border-color);
  padding: 10px 12px;
  border-radius: var(--radius-md);
  min-width: 120px;
}
.logic-summary-box .l-lbl {
  display: block;
  font-size: 9px;
  font-weight: 700;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.03em;
  margin-bottom: 4px;
}
.logic-summary-box .l-val {
  font-size: 13px;
  font-weight: 700;
}

.logic-steps {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.logic-step {
  display: flex;
  gap: 10px;
  font-size: 13px;
  align-items: flex-start;
}

.step-num {
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid var(--border-color);
  width: 20px;
  height: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 50%;
  font-size: 10px;
  font-weight: 700;
  flex-shrink: 0;
  margin-top: 1px;
}

.formula {
  background: rgba(0, 0, 0, 0.3);
  border: 1px solid var(--border-color);
  padding: 8px 12px;
  border-radius: var(--radius-md);
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--color-yellow);
  margin-top: 8px;
}
</style>
