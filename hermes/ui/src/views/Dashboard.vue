<script setup>
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from 'vue'
import {
  state,
  loadStatus,
  loadApprovals,
  loadLogs,
  decide,
  bulkDecide,
  togglePause,
  setMode,
  setCalmMode
} from '../state'
import Icon from '../components/Icon.vue'

// Local UI state
const approvalNotes = ref({})

// Chart state
const selectedSymbol = ref('SPY')
const chartPeriod = ref('30d')

// Default symbol fallback if watchlist is loaded
watch(() => state.watchlistData, (wl) => {
  if (wl && wl.global_default && wl.global_default.length > 0 && selectedSymbol.value === 'SPY') {
    selectedSymbol.value = wl.global_default[0]
  }
}, { deep: true })

// Polling for updates while Dashboard is open
let pollInterval = null

onMounted(async () => {
  // Load initial data
  await Promise.all([
    loadStatus(),
    loadApprovals(),
    loadLogs()
  ])
  
  pollInterval = setInterval(() => {
    loadApprovals()
  }, 10000)
})

onUnmounted(() => {
  if (pollInterval) clearInterval(pollInterval)
})

// Relative time formatting
function getRelativeTime(iso) {
  if (!iso) return '—'
  const d = Math.round((Date.now() - new Date(iso)) / 1000)
  if (d < 60) return d + 's ago'
  if (d < 3600) return Math.round(d / 60) + 'm ago'
  return Math.round(d / 3600) + 'h ago'
}

function getLegsList(actionJson) {
  return actionJson.legs || []
}

function isBuyLeg(leg) {
  const side = (leg.side || leg.action || '').toLowerCase()
  return side.includes('buy')
}

// -----------------------------------------------------------------
// Mock Dynamic / Real Data Integration
// -----------------------------------------------------------------

const activeSymbolPrice = computed(() => {
  const sym = selectedSymbol.value.toUpperCase()
  if (sym === 'BTC') return { value: '$67,842.15', change: '+3.12%', isPositive: true }
  if (sym === 'SPY') return { value: '$438.52', change: '+1.24%', isPositive: true }
  if (sym === 'QQQ') return { value: '$382.44', change: '-0.45%', isPositive: false }
  if (sym === 'IWM') return { value: '$198.12', change: '+0.08%', isPositive: true }
  return { value: '$150.00', change: '+0.50%', isPositive: true }
})

// Computes 30 data points for SVG Chart rendering (Wide layout)
const chartDataPoints = computed(() => {
  const sym = selectedSymbol.value.toUpperCase()
  const seed = sym.charCodeAt(0) || 100
  const points = []
  let price = sym === 'BTC' ? 66000 : sym === 'SPY' ? 431 : sym === 'QQQ' ? 385 : 194
  const step = sym === 'BTC' ? 240 : sym === 'SPY' ? 1.2 : sym === 'QQQ' ? 1.0 : 0.6
  
  for (let i = 0; i < 30; i++) {
    const change = Math.sin(i * 0.4 + seed) * step * 1.5 + (Math.cos(i * 0.25) * step * 0.7) + (i * 0.15)
    const open = price
    const close = price + change
    const high = Math.max(open, close) + Math.abs(Math.sin(i)) * (step * 0.5)
    const low = Math.min(open, close) - Math.abs(Math.cos(i)) * (step * 0.5)
    points.push({ open, close, high, low, time: i })
    price = close
  }
  return points
})

const svgPathAndCandles = computed(() => {
  const data = chartDataPoints.value
  if (!data.length) return { linePath: '', areaPath: '', candles: [], gridLines: [] }
  
  const width = 940
  const height = 260
  const padding = 20
  const chartWidth = width - padding * 2
  const chartHeight = height - padding * 2
  
  const prices = data.flatMap(d => [d.high, d.low])
  const maxPrice = Math.max(...prices)
  const minPrice = Math.min(...prices)
  const priceRange = maxPrice - minPrice || 1
  
  const getX = (index) => padding + (index / (data.length - 1)) * chartWidth
  const getY = (price) => height - padding - ((price - minPrice) / priceRange) * chartHeight
  
  const candles = data.map((d, i) => {
    const cx = getX(i)
    const yOpen = getY(d.open)
    const yClose = getY(d.close)
    const yHigh = getY(d.high)
    const yLow = getY(d.low)
    const isGreen = d.close >= d.open
    
    return {
      cx,
      yHigh,
      yLow,
      yOpen: Math.min(yOpen, yClose),
      yClose: Math.max(yOpen, yClose),
      height: Math.max(2, Math.abs(yOpen - yClose)),
      isGreen,
      width: Math.max(4, chartWidth / data.length - 6)
    }
  })
  
  // Smooth close price curve
  let linePath = `M ${getX(0)} ${getY(data[0].close)}`
  for (let i = 1; i < data.length; i++) {
    linePath += ` L ${getX(i)} ${getY(data[i].close)}`
  }
  
  const areaPath = `${linePath} L ${getX(data.length - 1)} ${height - padding} L ${getX(0)} ${height - padding} Z`
  
  const gridCount = 4
  const gridLines = []
  for (let i = 0; i <= gridCount; i++) {
    const val = minPrice + (i / gridCount) * priceRange
    const y = getY(val)
    gridLines.push({ y, label: val.toFixed(selectedSymbol.value.toUpperCase() === 'BTC' ? 0 : 2) })
  }
  
  return { linePath, areaPath, candles, gridLines }
})

const portfolioValue = computed(() => {
  const val = state.analyticsData?.performance?.total_value
  return val ? '$' + val.toLocaleString() : '$148,650'
})

const totalPnl = computed(() => {
  const pnl = state.analyticsData?.performance?.total_pnl
  const pct = state.analyticsData?.performance?.total_pnl_pct
  if (pnl != null) {
    const sign = pnl >= 0 ? '+' : ''
    const formattedPct = pct != null ? ` (${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%)` : ''
    return `${sign}$${pnl.toLocaleString()}${formattedPct}`
  }
  return '+$3,450.78 (+12.5%)'
})

const isPnlPositive = computed(() => {
  const pnl = state.analyticsData?.performance?.total_pnl
  return pnl != null ? pnl >= 0 : true
})

const lastActionText = computed(() => {
  const list = state.logs || []
  const tradingLogs = list.filter(l => l.text && !l.text.includes('Heartbeat') && !l.text.includes('tick'))
  return tradingLogs.length > 0 ? tradingLogs[tradingLogs.length - 1].text : 'Initialized agent loop'
})
</script>

<template>
  <div class="cockpit-container">
    
    <!-- Top Row: Full Width Wide-Screen Chart Card -->
    <section class="card chart-card">
      <div class="card-header">
        <div class="chart-title-group">
          <div class="symbol-title-row">
            <span class="symbol-title">{{ selectedSymbol }} Index</span>
            <select v-model="selectedSymbol" class="symbol-select">
              <option v-for="sym in (state.watchlistData?.global_default || ['SPY', 'QQQ', 'BTC'])" :key="sym" :value="sym">
                {{ sym }}
              </option>
            </select>
          </div>
          
          <div class="chart-stats-row">
            <div class="c-stat">
              <span class="lbl">Current Price</span>
              <span class="val" :class="{ 'text-green': activeSymbolPrice.isPositive, 'text-red': !activeSymbolPrice.isPositive }">
                {{ activeSymbolPrice.value }}
              </span>
            </div>
            <div class="c-stat">
              <span class="lbl">24h Change</span>
              <span class="val" :class="{ 'text-green': activeSymbolPrice.isPositive, 'text-red': !activeSymbolPrice.isPositive }">
                {{ activeSymbolPrice.change }}
              </span>
            </div>
            <div class="c-stat">
              <span class="lbl">Volume</span>
              <span class="val text-muted">$327.7M</span>
            </div>
            <div class="c-stat">
              <span class="lbl">Volatility</span>
              <span class="val text-green">+12.5%</span>
            </div>
          </div>
        </div>
        
        <div class="btn-toggle-group">
          <button class="btn-toggle-option" :class="{ active: chartPeriod === '30d' }" @click="chartPeriod = '30d'">30D</button>
          <button class="btn-toggle-option" :class="{ active: chartPeriod === '60d' }" @click="chartPeriod = '60d'">60D</button>
        </div>
      </div>
      
      <div class="card-body chart-body">
        <div class="svg-container">
          <svg viewBox="0 0 940 260" class="neon-svg">
            <!-- Grids -->
            <line v-for="(g, idx) in svgPathAndCandles.gridLines" :key="idx" x1="0" :y1="g.y" x2="940" :y2="g.y" class="grid-line" />
            
            <!-- Close Price Gradient Area -->
            <defs>
              <linearGradient id="chartGlow" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="var(--color-blue)" stop-opacity="0.25" />
                <stop offset="100%" stop-color="var(--color-blue)" stop-opacity="0.0" />
              </linearGradient>
            </defs>
            <path :d="svgPathAndCandles.areaPath" fill="url(#chartGlow)" />
            
            <!-- Close Price Glowing Stroke -->
            <path :d="svgPathAndCandles.linePath" fill="none" stroke="var(--color-blue)" stroke-width="2.5" class="glowing-line" />
            
            <!-- Candlestick Shadows & Bodies -->
            <g v-for="(c, idx) in svgPathAndCandles.candles" :key="idx" class="candle-g">
              <line :x1="c.cx" :y1="c.yHigh" :x2="c.cx" :y2="c.yLow" :stroke="c.isGreen ? 'var(--color-green)' : 'var(--color-red)'" stroke-width="1.2" />
              <rect :x="c.cx - c.width/2" :y="c.yOpen" :width="c.width" :height="c.height" :fill="c.isGreen ? 'var(--color-green)' : 'var(--color-red)'" rx="1" />
            </g>
            
            <!-- Price Labels -->
            <text v-for="(g, idx) in svgPathAndCandles.gridLines" :key="'txt' + idx" x="895" :y="g.y - 4" class="grid-label">{{ g.label }}</text>
          </svg>
        </div>
      </div>
    </section>

    <!-- Bottom Row Layout -->
    <div class="primary-layout">
      
      <!-- Left Column: Active Bot Status -->
      <section class="card bot-status-card">
        <div class="card-header">
          <span class="header-title">Active Bot Status</span>
          <div class="status-summary-header">
            <div class="pulse-dot" :class="{ running: state.status.hermes_running }"></div>
            <span class="status-txt">{{ state.status.hermes_running ? 'RUNNING' : 'STOPPED' }}</span>
          </div>
        </div>
        <div class="card-body bot-content">
          <div class="bot-info-table">
            <div class="bot-info-row">
              <span class="lbl">Daemon Loop</span>
              <div class="val-actions">
                <span class="val" :class="state.status.hermes_running ? 'text-green' : 'text-red'">
                  {{ state.status.hermes_running ? 'Online' : 'Offline' }}
                </span>
                <button 
                  v-if="state.status.hermes_running" 
                  class="btn-pause-inline" 
                  :class="state.status.paused ? 'btn-inline-resume' : 'btn-inline-pause'" 
                  @click="togglePause"
                >
                  {{ state.status.paused ? 'Resume' : 'Pause' }}
                </button>
              </div>
            </div>
            
            <div class="bot-info-row">
              <span class="lbl">Trading Route</span>
              <div class="mode-toggles-inline">
                <button 
                  class="btn-inline-toggle" 
                  :class="{ active: state.status.mode === 'paper' }"
                  @click="setMode('paper')"
                >Paper</button>
                <button
                  class="btn-inline-toggle btn-live-inline"
                  :class="{ active: state.status.mode === 'live' }"
                  @click="setMode('live')"
                >Live</button>
              </div>
            </div>
            
            <div class="bot-info-row">
              <span class="lbl">Auto-Pilot Mode</span>
              <span class="val mode-badge" :class="state.soul?.autonomy">{{ (state.soul?.autonomy || 'advisory').toUpperCase() }}</span>
            </div>

            <div class="bot-info-row">
              <span class="lbl">Market Session</span>
              <span class="val" :class="state.status.market_is_open ? 'text-green' : 'text-muted'">
                {{ state.status.market_is_open ? '● OPEN' : '● CLOSED' }}
              </span>
            </div>

            <div class="bot-info-row">
              <span class="lbl">Calm Mode</span>
              <button
                class="calm-btn-inline"
                :class="{ active: state.calmMode }"
                @click="setCalmMode(!state.calmMode)"
              >
                {{ state.calmMode ? 'ON' : 'OFF' }}
              </button>
            </div>
            
            <div class="bot-info-row">
              <span class="lbl">Diagnostics</span>
              <span class="val diag-indicators">
                <span :class="state.status.tradier_ok ? 'text-green' : 'text-red'" title="Tradier API Status">TRADIER</span>
                <span class="separator">·</span>
                <span :class="state.status.ml_ok ? 'text-green' : 'text-red'" title="XGBoost ML Status">ML</span>
                <span class="separator">·</span>
                <span :class="state.status.llm_ok ? 'text-green' : 'text-red'" title="LLM Overseer Status">LLM</span>
              </span>
            </div>

            <div class="bot-info-row">
              <span class="lbl">Profit/Loss</span>
              <span class="val text-green font-bold" :class="{ 'text-red': !isPnlPositive }">{{ totalPnl }}</span>
            </div>
            
            <div class="bot-info-row">
              <span class="lbl">Open Positions</span>
              <span class="val">{{ state.analyticsData?.open_trades?.length || '4' }} Trades</span>
            </div>
            
            <div class="bot-info-row last-action-row">
              <span class="lbl">Last Agent Log</span>
              <span class="val action-text" :title="lastActionText">{{ lastActionText }}</span>
            </div>
          </div>
          
          <router-link to="/settings" class="btn-primary w-full btn-configure-bot" style="text-decoration: none;">
            <Icon name="settings" :size="14" /> CONFIGURE BOT
          </router-link>
        </div>
      </section>

      <!-- Right Column: Pending Approvals Queue -->
      <section class="card approvals-card">
        <div class="card-header">
          <div class="header-title">
            <span>Pending Trade Approvals</span>
            <span class="count-badge" v-if="state.approvals.pending.length">
              {{ state.approvals.pending.length }}
            </span>
          </div>
          <div class="actions">
            <button class="btn-action-text btn-approve-text" @click="bulkDecide('approve')">Approve All</button>
            <button class="btn-action-text btn-reject-text" @click="bulkDecide('reject')">Reject All</button>
          </div>
        </div>
        
        <div class="card-body no-padding queue-body">
          <div v-if="state.approvals.pending.length === 0" class="empty-state">
            <div class="empty-icon text-muted"><Icon name="check" :size="24" /></div>
            <p class="empty-text">No pending trade entries.</p>
          </div>
          <div v-else class="dashboard-queue-list">
            <div v-for="item in state.approvals.pending" :key="item.id" class="mini-trade-card">
              <div class="mini-card-header">
                <span class="m-symbol">{{ item.symbol }}</span>
                <span class="m-strategy">{{ item.strategy_id }}</span>
                <span class="m-type" :class="item.action_type || 'entry'">
                  {{ (item.action_type || 'entry').toUpperCase() }}
                </span>
                <span class="m-age">{{ getRelativeTime(item.created_at) }}</span>
              </div>
              
              <div class="mini-card-legs">
                <div v-for="(leg, idx) in getLegsList(item.action_json)" :key="idx" class="mini-leg-row">
                  <span class="mini-side" :class="{ buy: isBuyLeg(leg), sell: !isBuyLeg(leg) }">
                    {{ (leg.side || leg.action || '').replace(/_/g, ' ').toUpperCase() }}
                  </span>
                  <span class="mini-option">{{ leg.option_symbol || leg.symbol || '—' }}</span>
                  <span class="mini-qty">×{{ leg.quantity || 1 }}</span>
                </div>
              </div>
              
              <div class="mini-card-meta">
                <span v-if="item.action_json?.price != null" class="m-price pnl-green">
                  ${{ Math.abs(item.action_json.price).toFixed(2) }} {{ item.action_json.price >= 0 ? 'Cr' : 'Dr' }}
                </span>
                <span v-if="item.action_json?.dte != null" class="m-dte">{{ item.action_json.dte }} DTE</span>
              </div>
              
              <div class="mini-card-actions">
                <input type="text" v-model="approvalNotes[item.id]" placeholder="Review notes..." class="mini-notes-input" />
                <button class="btn-approve btn-action-xs" @click="decide(item.id, 'approve', approvalNotes[item.id])">Approve</button>
                <button class="btn-reject btn-action-xs" @click="decide(item.id, 'reject', approvalNotes[item.id])">Reject</button>
              </div>
            </div>
          </div>
        </div>
      </section>
      
    </div>

  </div>
</template>

<style scoped>
.cockpit-container {
  display: flex;
  flex-direction: column;
  gap: 20px;
  width: 100%;
}

/* Primary Layout: Chart Card dominates top */
.chart-card {
  background: var(--surface-glass);
  width: 100%;
}

.chart-title-group {
  display: flex;
  flex-direction: column;
  gap: 8px;
  width: 80%;
}

.symbol-title-row {
  display: flex;
  align-items: center;
  gap: 12px;
}

.symbol-title {
  font-weight: var(--fw-bold);
  font-size: var(--fs-lg);
}

.symbol-select {
  background: rgba(0, 0, 0, 0.3);
  border: 1px solid var(--border-color);
  padding: 4px 8px;
  font-size: var(--fs-sm);
  border-radius: var(--radius-sm);
  color: var(--text-primary);
  width: 90px;
}

.chart-stats-row {
  display: flex;
  gap: 24px;
  flex-wrap: wrap;
}

.c-stat {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.c-stat .lbl {
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  color: var(--text-muted);
  letter-spacing: 0.03em;
}

.c-stat .val {
  font-size: var(--fs-md);
  font-weight: var(--fw-bold);
}

.btn-toggle-group {
  display: inline-flex;
  background: rgba(0,0,0,0.3);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  overflow: hidden;
  padding: 2px;
}

.btn-toggle-option {
  background: transparent;
  color: var(--text-muted);
  border: none;
  font-size: 10px;
  padding: 4px 10px;
  font-weight: 700;
  border-radius: 3px;
  cursor: pointer;
  transition: all 0.15s ease;
}

.btn-toggle-option.active {
  background: var(--color-blue);
  color: #ffffff;
}

.chart-body {
  padding: 16px;
  display: flex;
  justify-content: center;
}

.svg-container {
  width: 100%;
  max-width: 940px;
  background: rgba(0, 0, 0, 0.2);
  border-radius: var(--radius-md);
  padding: 10px;
  border: 1px solid rgba(255, 255, 255, 0.02);
}

.neon-svg {
  width: 100%;
  height: auto;
  overflow: visible;
}

.grid-line {
  stroke: rgba(255, 255, 255, 0.05);
  stroke-dasharray: 2 4;
}

.glowing-line {
  filter: drop-shadow(0px 0px 5px rgba(59, 130, 246, 0.4));
}

.grid-label {
  fill: var(--text-muted);
  font-family: var(--font-mono);
  font-size: 10px;
  text-anchor: end;
}

/* Bottom Grid Layout */
.primary-layout {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
  align-items: start;
}

@media (max-width: 900px) {
  .primary-layout {
    grid-template-columns: 1fr;
  }
}

/* Bot Status Card Styling */
.bot-status-card {
  background: var(--surface-glass);
}

.status-summary-header {
  display: flex;
  align-items: center;
  gap: 8px;
}

.pulse-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--color-red);
  box-shadow: 0 0 6px var(--color-red);
}

.pulse-dot.running {
  background: var(--color-green);
  box-shadow: 0 0 8px var(--color-green);
  animation: s-pulse 2s infinite;
}

@keyframes s-pulse {
  0%, 100% { transform: scale(1); opacity: 1; }
  50% { transform: scale(1.2); opacity: 0.7; }
}

.status-txt {
  font-size: var(--fs-2xs);
  font-weight: var(--fw-bold);
  letter-spacing: var(--tracking-wide);
}

.bot-content {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.bot-info-table {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.bot-info-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid rgba(255, 255, 255, 0.03);
  padding: 8px 0;
  font-size: var(--fs-sm);
}

.bot-info-row .lbl {
  color: var(--text-muted);
}

.bot-info-row .val {
  font-weight: var(--fw-semibold);
}

.val-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.btn-pause-inline {
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid var(--border-color);
  color: var(--text-primary);
  font-size: 10px;
  padding: 2px 6px;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.15s ease;
}
.btn-pause-inline:hover {
  background: rgba(255, 255, 255, 0.1);
}
.btn-inline-resume {
  color: var(--color-green);
  border-color: rgba(16, 185, 129, 0.3);
}
.btn-inline-pause {
  color: var(--color-yellow);
  border-color: rgba(245, 158, 11, 0.3);
}

.mode-toggles-inline {
  display: inline-flex;
  background: rgba(0,0,0,0.3);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  padding: 1px;
}
.btn-inline-toggle {
  background: transparent;
  color: var(--text-muted);
  border: none;
  font-size: 10px;
  padding: 2px 8px;
  font-weight: var(--fw-semibold);
  border-radius: 3px;
  cursor: pointer;
  transition: all 0.15s ease;
}
.btn-inline-toggle:hover {
  color: var(--text-primary);
}
.btn-inline-toggle.active {
  background: var(--color-blue);
  color: #ffffff;
}
.btn-inline-toggle.btn-live-inline.active {
  background: var(--color-orange);
  color: #ffffff;
}

.mode-badge {
  font-size: 9px;
  font-weight: 800;
  padding: 2px 6px;
  border-radius: 4px;
}
.mode-badge.advisory {
  background: rgba(59, 130, 246, 0.1);
  color: var(--color-blue);
}
.mode-badge.enforcing {
  background: rgba(245, 158, 11, 0.1);
  color: var(--color-yellow);
}
.mode-badge.autonomous {
  background: rgba(139, 92, 246, 0.1);
  color: var(--color-purple);
}

.calm-btn-inline {
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid var(--border-color);
  color: var(--text-muted);
  font-size: 10px;
  padding: 2px 8px;
  border-radius: 4px;
  cursor: pointer;
  font-weight: 700;
  transition: all 0.15s ease;
}
.calm-btn-inline:hover {
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.08);
}
.calm-btn-inline.active {
  background: var(--color-blue);
  color: #ffffff;
  border-color: var(--color-blue);
}

.diag-indicators {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: 10px;
  font-weight: 700;
}
.diag-indicators .separator {
  color: rgba(255, 255, 255, 0.15);
}

.last-action-row {
  flex-direction: column;
  gap: 6px;
  align-items: stretch;
  border-bottom: none;
}

.action-text {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--text-muted);
  line-height: 1.5;
  background: rgba(0,0,0,0.15);
  padding: 8px;
  border-radius: var(--radius-sm);
  max-height: 60px;
  overflow-y: auto;
}

.btn-configure-bot {
  background: linear-gradient(135deg, var(--color-blue), var(--color-purple));
  color: #ffffff;
  border: none;
  font-weight: 700;
  padding: 10px;
  letter-spacing: 0.05em;
  box-shadow: 0 4px 15px rgba(139, 92, 246, 0.2);
  transition: all 0.2s ease;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
}

.btn-configure-bot:hover {
  filter: brightness(1.1);
  box-shadow: 0 4px 20px rgba(139, 92, 246, 0.35);
}

.font-bold {
  font-weight: 700;
}

/* Approvals Card Styling */
.approvals-card {
  max-height: 520px;
  display: flex;
  flex-direction: column;
  background: var(--surface-glass);
}

.count-badge {
  background: var(--color-orange-glow);
  color: var(--color-orange);
  border: 1px solid rgba(249, 115, 22, 0.3);
  padding: 1px 6px;
  border-radius: 4px;
  font-size: 10px;
  font-weight: 600;
}

.no-padding {
  padding: 0;
}

.queue-body {
  overflow-y: auto;
  flex-grow: 1;
}

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 40px;
  text-align: center;
}

.empty-icon {
  margin-bottom: 8px;
}

.empty-text {
  color: var(--text-muted);
  font-size: 13px;
}

.dashboard-queue-list {
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.mini-trade-card {
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  background: rgba(6, 9, 19, 0.4);
  padding: 10px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.mini-card-header {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 11px;
}

.m-symbol {
  font-size: var(--fs-md);
  font-weight: var(--fw-bold);
  color: var(--color-blue);
}

.m-strategy {
  background: rgba(255,255,255,0.05);
  padding: 1px 4px;
  border-radius: 3px;
  color: var(--text-muted);
}

.m-type {
  font-weight: 700;
  padding: 1px 4px;
  border-radius: 3px;
}
.m-type.entry {
  color: var(--color-green);
  background: rgba(16, 185, 129, 0.08);
}
.m-type.management {
  color: var(--color-yellow);
  background: rgba(245, 158, 11, 0.08);
}

.m-age {
  margin-left: auto;
  color: var(--text-muted);
}

.mini-card-legs {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-size: var(--fs-xs);
  background: rgba(0,0,0,0.15);
  padding: 6px;
  border-radius: var(--radius-sm);
}

.mini-leg-row {
  display: flex;
  justify-content: space-between;
}

.mini-side.buy {
  color: var(--color-green);
}
.mini-side.sell {
  color: var(--color-red);
}

.mini-option {
  color: var(--text-muted);
  font-family: var(--font-mono);
  margin-left: 6px;
}

.mini-card-meta {
  display: flex;
  gap: 8px;
  font-size: 11px;
}

.mini-card-actions {
  display: flex;
  gap: 6px;
  align-items: center;
  margin-top: 4px;
}

.mini-notes-input {
  flex-grow: 1;
  padding: 4px 8px;
  font-size: 11px;
  height: 26px;
  border-radius: var(--radius-sm);
}

.btn-action-xs {
  font-size: 10px;
  padding: 4px 8px;
  border-radius: var(--radius-sm);
}

.btn-action-text {
  background: transparent;
  border: none;
  font-size: 11px;
  font-weight: 700;
  cursor: pointer;
  padding: 4px 8px;
  border-radius: 4px;
}
.btn-approve-text {
  color: var(--color-green);
}
.btn-approve-text:hover {
  background: rgba(16, 185, 129, 0.1);
}
.btn-reject-text {
  color: var(--color-red);
}
.btn-reject-text:hover {
  background: rgba(239, 68, 68, 0.1);
}
</style>
