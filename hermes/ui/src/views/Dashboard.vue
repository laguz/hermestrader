<script setup>
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from 'vue'
import {
  state,
  loadStatus,
  loadApprovals,
  loadWatchlist,
  loadSoul,
  loadLLM,
  loadLogs,
  decide,
  bulkDecide,
  saveSoul,
  saveAutonomy,
  toggleStrategy,
  setMode,
  setApprovalMode,
  saveLLM as apiSaveLLM,
  saveLots,
  addSymbol,
  removeSymbol,
  resetWatchlist
} from '../state'
import StatusPill from '../components/StatusPill.vue'
import Icon from '../components/Icon.vue'

// Local UI state
const activeTab = ref('soul')
const logFeedRef = ref(null)
const approvalNotes = ref({})
const newSymbolInputs = ref({})
const lotInputs = ref({})

// Settings Drawer state
const showSettingsDrawer = ref(false)
const settingsActiveTab = ref('soul')

// Option Chain & Chart state
const selectedSymbol = ref('SPY')
const selectedOptionType = ref('calls')
const chartPeriod = ref('30d')

// Soul inputs
const soulText = ref('')
const autonomySelect = ref('advisory')

// LLM inputs
const llmProvider = ref('mock')
const llmBaseUrl = ref('')
const llmModel = ref('')
const llmTemp = ref(0.2)
const llmTimeout = ref(120)
const llmApiKey = ref('')

// Watchers to populate local form inputs from reactive state
watch(() => state.soul, (val) => {
  if (val) {
    soulText.value = val.soul || ''
    autonomySelect.value = val.autonomy || 'advisory'
  }
}, { immediate: true, deep: true })

watch(() => state.llm, (val) => {
  if (val) {
    llmProvider.value = val.provider || 'mock'
    llmBaseUrl.value = val.base_url || ''
    llmModel.value = val.model || ''
    llmTemp.value = val.temperature ?? 0.2
    llmTimeout.value = val.timeout_s ?? 120
    llmApiKey.value = '' // Clear on load, don't show secret
  }
}, { immediate: true, deep: true })

watch(() => state.lotsData, (val) => {
  if (val) {
    Object.keys(val).forEach(sid => {
      if (!lotInputs.value[sid]) lotInputs.value[sid] = {}
      lotInputs.value[sid].target = val[sid]?.target ?? 5
      lotInputs.value[sid].max = val[sid]?.max ?? 5
    })
  }
}, { immediate: true, deep: true })

watch(() => state.watchlistData?.strategies, (sids) => {
  (sids || []).forEach(sid => {
    if (!lotInputs.value[sid]) lotInputs.value[sid] = { target: 5, max: 5 }
  })
}, { immediate: true, deep: true })

// Auto scroll logs
watch(() => state.logs, () => {
  nextTick(() => {
    if (logFeedRef.value) {
      const el = logFeedRef.value
      el.scrollTop = el.scrollHeight
    }
  })
}, { deep: true })

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
    loadWatchlist(),
    loadSoul(),
    loadLLM(),
    loadLogs()
  ])
  
  pollInterval = setInterval(() => {
    loadWatchlist()
  }, 10000)
  
  nextTick(() => {
    if (logFeedRef.value) {
      logFeedRef.value.scrollTop = logFeedRef.value.scrollHeight
    }
  })
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

// Soul byte validation
const soulBytes = computed(() => {
  return new TextEncoder().encode(soulText.value).length
})

const isSoulTooLarge = computed(() => {
  return soulBytes.value > 65536
})

// Lot configuration metadata
const LOT_META = {
  CS75: { hasTarget: true, targetDefault: 10, maxDefault: 10 },
  CS7:  { hasTarget: true, targetDefault: 10, maxDefault: 10 },
  TT45: { hasTarget: true, targetDefault: 5,  maxDefault: 5 },
  WHEEL: { hasTarget: false, targetDefault: 5,  maxDefault: 5 }
}

function adjustLotsLocal(sid, field, delta) {
  if (!lotInputs.value[sid]) lotInputs.value[sid] = { target: 5, max: 5 }
  const current = lotInputs.value[sid][field] || 1
  lotInputs.value[sid][field] = Math.max(1, Math.min(100, current + delta))
}

async function onSaveLots(sid) {
  const hasTarget = LOT_META[sid]?.hasTarget ?? true
  const target = lotInputs.value[sid]?.target
  const max = lotInputs.value[sid]?.max
  await saveLots(sid, target, max, hasTarget)
}

async function onAddSymbol(sid) {
  const sym = newSymbolInputs.value[sid] || ''
  if (!sym.trim()) return
  await addSymbol(sid, sym)
  newSymbolInputs.value[sid] = ''
}

const isLlmLocal = computed(() => llmProvider.value === 'local')
const HOSTED_BASE_URLS = {
  ollama_cloud: 'https://api.ollama.com/v1',
  gemini: 'https://generativelanguage.googleapis.com/v1beta/openai',
  claude: 'https://api.anthropic.com/v1',
}
const isLlmHosted = computed(() => llmProvider.value in HOSTED_BASE_URLS)

const MODEL_OPTIONS = {
  gemini: [
    'gemini-3.5-flash',
    'gemini-3.1-pro-preview',
    'gemini-3-flash-preview',
    'gemini-3.1-flash-lite',
    'gemini-2.5-pro',
    'gemini-2.5-flash',
  ],
  claude: ['claude-opus-4-8', 'claude-sonnet-4-6', 'claude-haiku-4-5-20251001'],
}
const modelOptions = computed(() => MODEL_OPTIONS[llmProvider.value] || [])

function handleProviderChange() {
  const hosted = HOSTED_BASE_URLS[llmProvider.value]
  if (hosted) {
    llmBaseUrl.value = hosted
  } else if (isLlmLocal.value) {
    if (!llmBaseUrl.value || Object.values(HOSTED_BASE_URLS).includes(llmBaseUrl.value)) {
      llmBaseUrl.value = 'http://host.docker.internal:1234/v1'
    }
  }
  const opts = MODEL_OPTIONS[llmProvider.value]
  if (opts && opts.length && !opts.includes(llmModel.value)) {
    llmModel.value = opts[0]
  }
}

async function onSaveLLM() {
  const config = {
    provider: llmProvider.value,
    base_url: llmBaseUrl.value,
    model: llmModel.value,
    temperature: parseFloat(llmTemp.value),
    timeout_s: parseFloat(llmTimeout.value)
  }
  if (llmApiKey.value) {
    config.api_key = llmApiKey.value
  }
  await apiSaveLLM(config)
  llmApiKey.value = ''
}

function getLegsList(actionJson) {
  return actionJson.legs || []
}

function isBuyLeg(leg) {
  const side = (leg.side || leg.action || '').toLowerCase()
  return side.includes('buy')
}

const STRAT_DETAILS = {
  CS75: { name: 'CS75', prio: 1, desc: 'Credit Spreads 75 DTE' },
  CS7:  { name: 'CS7', prio: 2, desc: 'Credit Spreads 7 DTE' },
  TT45: { name: 'TT45', prio: 3, desc: 'TastyTrade 45 DTE' },
  WHEEL: { name: 'WHEEL', prio: 4, desc: 'Wheel Strategy' }
}

const recentDecisions = computed(() => {
  return (state.approvals.all || [])
    .filter(r => r.status !== 'PENDING')
    .slice(0, 20)
})

function getDecisionColor(status) {
  if (status === 'EXECUTED') return 'var(--color-green)'
  if (status === 'REJECTED') return 'var(--color-red)'
  if (status === 'APPROVED') return 'var(--color-yellow)'
  return 'var(--text-muted)'
}

function triggerUpdateInfo() {
  const s = state.status
  if (s.update_status) {
    const sha = s.update_status.latest_commit_sha || ''
    const msg = s.update_status.latest_commit_msg || ''
    alert(`A new version of Hermes is available!\n\nLocal Version: ${s.version}\nRemote Version: ${s.update_status.remote_version}\nLatest Commit: [${sha}] ${msg}\n\nTo upgrade, run:\n./hermes.sh update`)
  }
}

// -----------------------------------------------------------------
// Mock Dynamic Option Chain and High-Fidelity SVG Charts
// -----------------------------------------------------------------

const activeSymbolPrice = computed(() => {
  const sym = selectedSymbol.value.toUpperCase()
  if (sym === 'BTC') return { value: '$67,842.15', change: '+3.12%', isPositive: true }
  if (sym === 'SPY') return { value: '$438.52', change: '+1.24%', isPositive: true }
  if (sym === 'QQQ') return { value: '$382.44', change: '-0.45%', isPositive: false }
  if (sym === 'IWM') return { value: '$198.12', change: '+0.08%', isPositive: true }
  return { value: '$150.00', change: '+0.50%', isPositive: true }
})

// Computes 30 data points for SVG Chart rendering
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
  
  const width = 680
  const height = 240
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
      width: Math.max(3, chartWidth / data.length - 4)
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

// Option chain generation around active symbol price
const optionChainData = computed(() => {
  const sym = selectedSymbol.value.toUpperCase()
  const isBtc = sym === 'BTC'
  const centerStrike = isBtc ? 68000 : sym === 'SPY' ? 438 : sym === 'QQQ' ? 382 : 198
  const strikeStep = isBtc ? 1000 : sym === 'SPY' ? 2 : sym === 'QQQ' ? 2 : 1
  
  const strikes = []
  for (let i = -4; i <= 4; i++) {
    strikes.push(centerStrike + i * strikeStep)
  }
  
  return strikes.map(strike => {
    const isCall = selectedOptionType.value === 'calls'
    const distanceFromCenter = Math.abs(strike - centerStrike)
    const iv = (30 + distanceFromCenter * (isBtc ? 0.05 : 1.2)).toFixed(1)
    
    // Bid / Ask calculations
    let price = isBtc 
      ? Math.max(100, 3200 - distanceFromCenter * 1.8) 
      : Math.max(0.15, 8.50 - distanceFromCenter * 1.6)
    if (!isCall && strike > centerStrike) price = price * 0.4
    if (isCall && strike < centerStrike) price = price * 0.4
    
    const bid = price.toFixed(2)
    const ask = (price + (isBtc ? 20 : 0.15)).toFixed(2)
    const vol = Math.round(150 + Math.random() * 800 - distanceFromCenter * (isBtc ? 0.02 : 10))
    const oi = ((10 + Math.random() * 40 - distanceFromCenter * 0.1) / 10).toFixed(1) + 'M'
    
    return { strike, bid, ask, iv, vol, oi }
  })
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
  return '+$1,420.50 (9.4%)'
})

const isPnlPositive = computed(() => {
  const pnl = state.analyticsData?.performance?.total_pnl
  return pnl != null ? pnl >= 0 : true
})

const marginUsed = computed(() => {
  return '28%'
})
</script>

<template>
  <div class="cockpit-container">
    
    <!-- Top Row: Metrics Cards -->
    <div class="metrics-row">
      <div class="metric-card card">
        <span class="m-lbl">Portfolio Value</span>
        <div class="m-val">{{ portfolioValue }}</div>
        <span class="m-sub pnl-green">{{ totalPnl }}</span>
      </div>
      
      <div class="metric-card card">
        <span class="m-lbl">Margin Used</span>
        <div class="m-val">{{ marginUsed }}</div>
        <span class="m-sub">Capacity Limit: 80%</span>
      </div>
      
      <div class="metric-card card">
        <span class="m-lbl">VIX Index</span>
        <div class="m-val">14.85</div>
        <span class="m-sub text-green">● Calm Regime</span>
      </div>
      
      <div class="metric-card card actions-card">
        <span class="m-lbl">System Oversight</span>
        <div class="sys-actions">
          <button class="btn-ghost btn-config" @click="showSettingsDrawer = true" title="Configure System Parameters">
            <Icon name="settings" :size="15" /> Configure Bot
          </button>
        </div>
      </div>
    </div>

    <!-- Main Grid Layout -->
    <div class="primary-layout">
      
      <!-- Left Column: Chart & Active Bot Status -->
      <div class="left-column">
        
        <!-- Candlestick / Line Chart Card -->
        <section class="card chart-card">
          <div class="card-header">
            <div class="chart-title-group">
              <span class="symbol-title">{{ selectedSymbol }} Options</span>
              <span class="symbol-price" :class="{ 'text-green': activeSymbolPrice.isPositive, 'text-red': !activeSymbolPrice.isPositive }">
                {{ activeSymbolPrice.value }} ({{ activeSymbolPrice.change }})
              </span>
            </div>
            
            <div class="chart-controls">
              <select v-model="selectedSymbol" class="symbol-select">
                <option v-for="sym in (state.watchlistData?.global_default || ['SPY', 'QQQ', 'BTC'])" :key="sym" :value="sym">
                  {{ sym }}
                </option>
              </select>
              <div class="btn-toggle-group">
                <button class="btn-toggle-option" :class="{ active: chartPeriod === '30d' }" @click="chartPeriod = '30d'">30D</button>
                <button class="btn-toggle-option" :class="{ active: chartPeriod === '60d' }" @click="chartPeriod = '60d'">60D</button>
              </div>
            </div>
          </div>
          
          <div class="card-body chart-body">
            <div class="svg-container">
              <svg viewBox="0 0 680 240" class="neon-svg">
                <!-- Grids -->
                <line v-for="(g, idx) in svgPathAndCandles.gridLines" :key="idx" x1="0" :y1="g.y" x2="680" :y2="g.y" class="grid-line" />
                
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
                  <line :x1="c.cx" :y1="c.yHigh" :x2="c.cx" :y2="c.yLow" :stroke="c.isGreen ? 'var(--color-green)' : 'var(--color-red)'" stroke-width="1" />
                  <rect :x="c.cx - c.width/2" :y="c.yOpen" :width="c.width" :height="c.height" :fill="c.isGreen ? 'var(--color-green)' : 'var(--color-red)'" rx="1" />
                </g>
                
                <!-- Price Labels -->
                <text v-for="(g, idx) in svgPathAndCandles.gridLines" :key="'txt' + idx" x="635" :y="g.y - 4" class="grid-label">{{ g.label }}</text>
              </svg>
            </div>
          </div>
        </section>
        
        <!-- Active Bot Status Card -->
        <section class="card bot-status-card">
          <div class="card-header">
            <span class="header-title">Active Bot Status</span>
            <div class="status-summary-header">
              <div class="pulse-dot" :class="{ running: state.status.hermes_running }"></div>
              <span class="status-txt">{{ state.status.hermes_running ? 'RUNNING' : 'STOPPED' }}</span>
            </div>
          </div>
          <div class="card-body bot-body-grid">
            <div class="bot-info-details">
              <div class="bot-title">QUANT-BOT v3.1</div>
              <div class="bot-stat-row">
                <span class="lbl">Current P/L</span>
                <span class="val text-green" :class="{ 'text-red': !isPnlPositive }">{{ totalPnl }}</span>
              </div>
              <div class="bot-stat-row">
                <span class="lbl">Trades Executed</span>
                <span class="val">45/60</span>
              </div>
              <div class="bot-stat-row">
                <span class="lbl">Loop Heartbeat</span>
                <span class="val">{{ state.status.hermes_last_seen_age_s != null ? Math.round(state.status.hermes_last_seen_age_s) + 's' : '—' }}</span>
              </div>
            </div>
            
            <div class="bot-sparklines">
              <span class="spark-lbl">Recent Performance Log</span>
              <div class="spark-container">
                <svg viewBox="0 0 240 50" class="spark-svg">
                  <!-- Simulated sparkline -->
                  <path d="M 0 40 L 30 35 L 60 42 L 90 28 L 120 38 L 150 18 L 180 22 L 210 10 L 240 5" fill="none" stroke="var(--color-green)" stroke-width="2" />
                  <path d="M 0 40 L 30 42 L 60 45 L 90 41 L 120 32 L 150 35 L 180 30 L 210 33 L 240 28" fill="none" stroke="var(--color-purple)" stroke-width="1.5" />
                </svg>
              </div>
            </div>
          </div>
        </section>
        
      </div>

      <!-- Right Column: Option Chain & Trade Queue -->
      <div class="right-column">
        
        <!-- Option Chain Card -->
        <section class="card options-card">
          <div class="card-header">
            <span class="header-title">Option Chain ({{ selectedSymbol }})</span>
            <div class="btn-toggle-group">
              <button class="btn-toggle-option" :class="{ active: selectedOptionType === 'calls' }" @click="selectedOptionType = 'calls'">Calls</button>
              <button class="btn-toggle-option" :class="{ active: selectedOptionType === 'puts' }" @click="selectedOptionType = 'puts'">Puts</button>
            </div>
          </div>
          <div class="card-body no-padding overflow-y">
            <table class="tbl option-tbl">
              <thead>
                <tr>
                  <th>Strike</th>
                  <th>Bid</th>
                  <th>Ask</th>
                  <th>IV</th>
                  <th>Vol</th>
                  <th>OI</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in optionChainData" :key="row.strike" class="option-row">
                  <td class="strike-col"><strong>{{ row.strike }}</strong></td>
                  <td class="bid-col text-green">{{ row.bid }}</td>
                  <td class="ask-col text-green">{{ row.ask }}</td>
                  <td>{{ row.iv }}%</td>
                  <td>{{ row.vol }}</td>
                  <td>{{ row.oi }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>
        
        <!-- Pending Approvals Queue -->
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

    <!-- Slidable System Settings Drawer -->
    <div class="settings-drawer-backdrop" :class="{ show: showSettingsDrawer }" @click.self="showSettingsDrawer = false">
      <aside class="settings-drawer" :class="{ show: showSettingsDrawer }">
        <div class="drawer-header">
          <h3><Icon name="settings" :size="16" /> HermesTrader Settings</h3>
          <button class="btn-close-drawer" @click="showSettingsDrawer = false">×</button>
        </div>
        
        <div class="drawer-tabs">
          <button v-for="tab in ['soul', 'strats', 'watchlists', 'llm', 'diagnostics']" 
                  :key="tab" 
                  class="drawer-tab-btn" 
                  :class="{ active: settingsActiveTab === tab }" 
                  @click="settingsActiveTab = tab"
          >
            {{ tab.toUpperCase() }}
          </button>
        </div>
        
        <div class="drawer-content">
          <!-- SOUL CONFIG -->
          <div v-if="settingsActiveTab === 'soul'" class="tab-panel">
            <div class="tab-sec-title">Agent Operating Doctrine</div>
            <div class="form-group">
              <label>Autonomy Level</label>
              <select v-model="autonomySelect" @change="saveAutonomy(autonomySelect)">
                <option value="advisory">Advisory — AI advises, operator decides</option>
                <option value="enforcing">Enforcing — AI can veto trades</option>
                <option value="autonomous">Autonomous — AI may originate trades</option>
              </select>
            </div>
            <div class="form-group">
              <label class="textarea-label">
                <span>Soul Doctrine Text</span>
                <span class="byte-count" :class="{ 'text-red': isSoulTooLarge }">
                  {{ soulBytes }} / 65536 bytes
                </span>
              </label>
              <textarea v-model="soulText" rows="16" placeholder="Define operating principles..." :class="{ 'border-error': isSoulTooLarge }"></textarea>
            </div>
            <button class="btn-primary w-full" :disabled="isSoulTooLarge" @click="saveSoul(soulText)">Save Doctrine</button>
          </div>
          
          <!-- STRATEGY TOGGLES -->
          <div v-if="settingsActiveTab === 'strats'" class="tab-panel">
            <div class="tab-sec-title">Enable / Disable Strategies</div>
            <p class="tab-sec-desc">Active pipelines tick on schedule. Disabled lines are bypassed.</p>
            <div class="strategy-toggles">
              <div v-for="sid in ['CS75', 'CS7', 'TT45', 'WHEEL']" :key="sid" class="strategy-toggle-row">
                <div class="strat-info-toggle">
                  <span class="strat-name-toggle">{{ sid }}</span>
                  <span class="strat-desc-toggle">P{{ STRAT_DETAILS[sid].prio }} · {{ STRAT_DETAILS[sid].desc }}</span>
                </div>
                <label class="toggle">
                  <input type="checkbox" :checked="state.status.strategy_enabled?.[sid] !== false" @change="toggleStrategy(sid, $event.target.checked)" />
                  <span class="slider"></span>
                </label>
              </div>
            </div>
          </div>
          
          <!-- WATCHLISTS & LOTS -->
          <div v-if="settingsActiveTab === 'watchlists'" class="tab-panel">
            <div class="tab-sec-title">Watchlists &amp; Lots Configuration</div>
            
            <div class="strategies-config">
              <div v-for="sid in state.watchlistData?.strategies" :key="sid" class="strategy-watchlist-section">
                <div class="strategy-sec-header">
                  <span class="strategy-sec-name">{{ sid }}</span>
                  <span class="strategy-sec-desc">
                    {{ state.watchlistData.per_strategy[sid]?.length ? 'custom · ' + state.watchlistData.per_strategy[sid].length + ' syms' : 'using global default' }}
                  </span>
                </div>
                
                <!-- Lots Config -->
                <div class="lots-box">
                  <span class="lots-box-title">{{ LOT_META[sid]?.hasTarget ? 'LOT SIZE CONFIG' : 'MAX LOTS CONFIG' }}</span>
                  <div class="lots-box-controls">
                    <template v-if="LOT_META[sid]?.hasTarget">
                      <div class="lot-ctrl-group">
                        <span class="ctrl-lbl">Target</span>
                        <div class="num-adjuster">
                          <button @click="adjustLotsLocal(sid, 'target', -1)">−</button>
                          <input type="number" v-model.number="lotInputs[sid].target" min="1" max="100" />
                          <button @click="adjustLotsLocal(sid, 'target', 1)">+</button>
                        </div>
                      </div>
                    </template>
                    
                    <div class="lot-ctrl-group">
                      <span class="ctrl-lbl">Max</span>
                      <div class="num-adjuster">
                        <button @click="adjustLotsLocal(sid, 'max', -1)">−</button>
                        <input type="number" v-model.number="lotInputs[sid].max" min="1" max="100" />
                        <button @click="adjustLotsLocal(sid, 'max', 1)">+</button>
                      </div>
                    </div>
                    <button class="btn-primary btn-sm" @click="onSaveLots(sid)">Save</button>
                  </div>
                </div>

                <!-- Custom watchlist symbols list -->
                <div class="symbol-tags custom-watchlist-tags">
                  <span v-for="sym in (state.watchlistData.per_strategy[sid]?.length ? state.watchlistData.per_strategy[sid] : state.watchlistData.global_default)" :key="sym" class="sym-tag editable-tag">
                    {{ sym }}
                    <button v-if="state.watchlistData.per_strategy[sid]?.length" class="btn-remove-tag" @click="removeSymbol(sid, sym)"><Icon name="x" :size="11" /></button>
                  </span>
                </div>

                <!-- Add Symbol -->
                <div class="add-symbol-bar">
                  <input type="text" v-model="newSymbolInputs[sid]" placeholder="Add (e.g. AAPL)" class="add-symbol-input" @keyup.enter="onAddSymbol(sid)" />
                  <button class="btn-ghost btn-sm" @click="onAddSymbol(sid)">+ Add</button>
                  <button v-if="state.watchlistData.per_strategy[sid]?.length" class="btn-ghost btn-sm btn-icon" @click="resetWatchlist(sid)"><Icon name="rotate-ccw" :size="13" /> Reset</button>
                </div>
                <div class="divider"></div>
              </div>
            </div>
          </div>
          
          <!-- LLM CONFIG -->
          <div v-if="settingsActiveTab === 'llm'" class="tab-panel">
            <div class="tab-sec-title">LLM Client Configuration</div>
            <div class="form-group">
              <label>API Provider</label>
              <select v-model="llmProvider" @change="handleProviderChange">
                <option value="mock">Mock Overseer (No LLM)</option>
                <option value="local">Local Client (LM Studio / Ollama)</option>
                <option value="ollama_cloud">Ollama Cloud REST</option>
                <option value="gemini">Google Gemini API</option>
                <option value="claude">Anthropic Claude API</option>
              </select>
            </div>
            <div class="form-group" v-if="!isLlmHosted">
              <label>Base URL Endpoint</label>
              <input type="text" v-model="llmBaseUrl" placeholder="http://localhost:1234/v1" />
            </div>
            <div class="form-group">
              <label>AI Model Identifier</label>
              <select v-if="modelOptions.length" v-model="llmModel">
                <option v-for="m in modelOptions" :key="m" :value="m">{{ m }}</option>
              </select>
              <input v-else type="text" v-model="llmModel" placeholder="Model tag..." />
            </div>
            <div class="form-row">
              <div class="form-group">
                <label>Temp</label>
                <input type="number" v-model.number="llmTemp" min="0" max="2" step="0.05" />
              </div>
              <div class="form-group">
                <label>Timeout (s)</label>
                <input type="number" v-model.number="llmTimeout" min="5" max="600" />
              </div>
            </div>
            <div class="form-group">
              <label>API Auth Token</label>
              <input type="password" v-model="llmApiKey" placeholder="••••••••••••••••" />
              <div v-if="state.llm?.has_api_key" class="llm-key-saved-hint">
                <span>✓ Key Saved</span>
                <span v-if="state.llm.api_key_hint" class="text-muted"> (ends in {{ state.llm.api_key_hint }})</span>
              </div>
            </div>
            <button class="btn-primary w-full" @click="onSaveLLM">Save LLM Config</button>
          </div>
          
          <!-- DIAGNOSTICS & SYSTEM LOGS -->
          <div v-if="settingsActiveTab === 'diagnostics'" class="tab-panel">
            <div class="tab-sec-title">Status Diagnostics</div>
            <div class="status-grid-diag">
              <div class="diag-row">
                <span>Daemon Loop</span>
                <span :class="state.status.hermes_running ? 'text-green' : 'text-red'">{{ state.status.hermes_running ? 'Online' : 'Offline' }}</span>
              </div>
              <div class="diag-row">
                <span>Uptime</span>
                <span>{{ state.status.uptime_s != null ? Math.round(state.status.uptime_s / 60) + 'm' : '—' }}</span>
              </div>
              <div class="diag-row">
                <span>Tradier API</span>
                <span :class="state.status.tradier_ok ? 'text-green' : 'text-red'">{{ state.status.tradier_ok ? 'OK' : 'Error' }}</span>
              </div>
              <div class="diag-row">
                <span>LLM Status</span>
                <span :class="state.status.llm_ok ? 'text-green' : 'text-red'">{{ state.status.llm_ok ? 'OK' : 'Error' }}</span>
              </div>
              <div class="diag-row">
                <span>Mode</span>
                <span :class="state.status.mode === 'live' ? 'text-orange' : 'text-green'" style="text-transform: uppercase; font-weight: 700;">{{ state.status.mode }}</span>
              </div>
            </div>
            
            <div class="divider"></div>
            <div class="tab-sec-title">Activity Logs</div>
            <div ref="logFeedRef" class="log-feed">
              <div v-if="!state.logs.length" class="log-line">Loading logs...</div>
              <div v-for="(log, idx) in state.logs" :key="idx" class="log-line" :class="{ 'log-error': log.text?.includes('ERROR'), 'log-c2': log.text?.includes('[C2]') }">
                {{ log.text }}
              </div>
            </div>
          </div>
        </div>
      </aside>
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

/* Metrics Row styling */
.metrics-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 16px;
}

.metric-card {
  padding: 18px 24px;
  background: var(--surface-glass);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg);
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.m-lbl {
  font-size: var(--fs-2xs);
  font-weight: var(--fw-bold);
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: var(--tracking-wide);
}

.m-val {
  font-size: var(--fs-3xl);
  font-weight: var(--fw-extrabold);
  color: var(--text-primary);
  line-height: 1.2;
}

.m-sub {
  font-size: var(--fs-xs);
  color: var(--text-muted);
}

.actions-card {
  justify-content: space-between;
}

.sys-actions {
  margin-top: 8px;
}

.btn-config {
  width: 100%;
  justify-content: center;
  background: rgba(59, 130, 246, 0.08);
  border: 1px solid rgba(59, 130, 246, 0.2);
  color: var(--color-blue);
  padding: 8px 12px;
}
.btn-config:hover {
  background: rgba(59, 130, 246, 0.15);
  box-shadow: 0 0 10px rgba(59, 130, 246, 0.1);
}

/* Primary Grid Layout */
.primary-layout {
  display: grid;
  grid-template-columns: 1.8fr 1.2fr;
  gap: 20px;
  align-items: start;
}

@media (max-width: 1100px) {
  .primary-layout {
    grid-template-columns: 1fr;
  }
}

.left-column, .right-column {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

/* Chart Card Styling */
.chart-card {
  background: var(--surface-glass);
}

.chart-title-group {
  display: flex;
  flex-direction: column;
}

.symbol-title {
  font-weight: var(--fw-bold);
  font-size: var(--fs-lg);
}

.symbol-price {
  font-size: var(--fs-sm);
  font-weight: var(--fw-semibold);
  margin-top: 2px;
}

.chart-controls {
  display: flex;
  align-items: center;
  gap: 12px;
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
  max-width: 680px;
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
  filter: drop-shadow(0px 0px 5px rgba(59, 130, 246, 0.5));
}

.grid-label {
  fill: var(--text-muted);
  font-family: var(--font-mono);
  font-size: 10px;
  text-anchor: end;
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

.bot-body-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
}

@media (max-width: 600px) {
  .bot-body-grid {
    grid-template-columns: 1fr;
  }
}

.bot-title {
  font-size: var(--fs-lg);
  font-weight: var(--fw-bold);
  margin-bottom: 12px;
  letter-spacing: -0.01em;
}

.bot-stat-row {
  display: flex;
  justify-content: space-between;
  border-bottom: 1px solid rgba(255, 255, 255, 0.03);
  padding: 6px 0;
  font-size: var(--fs-sm);
}

.bot-stat-row:last-child {
  border-bottom: none;
}

.bot-stat-row .lbl {
  color: var(--text-muted);
}

.bot-stat-row .val {
  font-weight: var(--fw-semibold);
}

.bot-sparklines {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.spark-lbl {
  font-size: var(--fs-xs);
  color: var(--text-muted);
  font-weight: 600;
}

.spark-container {
  background: rgba(0, 0, 0, 0.2);
  border-radius: var(--radius-md);
  padding: 8px;
  border: 1px solid var(--border-color);
  height: 60px;
}

.spark-svg {
  width: 100%;
  height: 100%;
}

/* Option Chain Card */
.options-card {
  height: 310px;
  display: flex;
  flex-direction: column;
}

.overflow-y {
  overflow-y: auto;
  flex-grow: 1;
}

.option-tbl th {
  position: sticky;
  top: 0;
  background: #060913;
  z-index: 5;
}

.option-row:hover td {
  background: rgba(255, 255, 255, 0.03);
}

.strike-col {
  color: var(--text-primary);
  font-family: var(--font-mono);
}

/* Approvals Card Styling */
.approvals-card {
  flex-grow: 1;
  display: flex;
  flex-direction: column;
  max-height: 480px;
}

.queue-body {
  overflow-y: auto;
  flex-grow: 1;
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

/* Sliding Settings Drawer */
.settings-drawer-backdrop {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.6);
  backdrop-filter: blur(4px);
  z-index: 1000;
  opacity: 0;
  pointer-events: none;
  transition: opacity 0.25s ease;
}

.settings-drawer-backdrop.show {
  opacity: 1;
  pointer-events: auto;
}

.settings-drawer {
  position: fixed;
  right: -450px;
  top: 0;
  height: 100vh;
  width: 440px;
  max-width: 95vw;
  background: rgba(12, 21, 39, 0.98);
  border-left: 1px solid var(--border-color);
  box-shadow: -10px 0 35px rgba(0, 0, 0, 0.6);
  z-index: 1001;
  display: flex;
  flex-direction: column;
  transition: transform 0.25s cubic-bezier(0.16, 1, 0.3, 1);
}

.settings-drawer.show {
  transform: translateX(-450px);
}

.drawer-header {
  padding: 20px;
  border-bottom: 1px solid var(--border-color);
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.drawer-header h3 {
  font-size: var(--fs-md);
  letter-spacing: var(--tracking-wide);
}

.btn-close-drawer {
  background: transparent;
  border: none;
  font-size: 24px;
  color: var(--text-muted);
  cursor: pointer;
  line-height: 1;
}

.drawer-tabs {
  display: flex;
  overflow-x: auto;
  border-bottom: 1px solid var(--border-color);
  background: rgba(0, 0, 0, 0.2);
}

.drawer-tab-btn {
  flex-shrink: 0;
  border-radius: 0;
  background: transparent;
  color: var(--text-muted);
  border-bottom: 2px solid transparent;
  padding: 10px 14px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.05em;
  cursor: pointer;
}
.drawer-tab-btn:hover {
  color: var(--text-primary);
}
.drawer-tab-btn.active {
  color: var(--color-blue);
  border-bottom-color: var(--color-blue);
  background: rgba(255, 255, 255, 0.03);
}

.drawer-content {
  padding: 20px;
  overflow-y: auto;
  flex-grow: 1;
}

/* Reuse existing log feed block */
.log-feed {
  max-height: 300px;
}
</style>
