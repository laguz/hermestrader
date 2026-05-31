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
  resetWatchlist,
  loadCharts
} from '../state'
import StatusPill from '../components/StatusPill.vue'
import Icon from '../components/Icon.vue'

// Local UI state
const activeTab = ref('soul')
const logFeedRef = ref(null)
const approvalNotes = ref({})
const newSymbolInputs = ref({})
const lotInputs = ref({})

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

// Auto scroll logs — only when the user is already at the bottom,
// so scrolling up to read history isn't interrupted by new entries.
watch(() => state.logs, () => {
  const el = logFeedRef.value
  if (!el) return
  // Within 40px of the bottom counts as "pinned to bottom"
  const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40
  if (!atBottom) return
  nextTick(() => {
    if (logFeedRef.value) {
      logFeedRef.value.scrollTop = logFeedRef.value.scrollHeight
    }
  })
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
  
  // Start background polling every 10s for items not pushed in SSE, or just as safety backup
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

// Watchlist add
async function onAddSymbol(sid) {
  const sym = newSymbolInputs.value[sid] || ''
  if (!sym.trim()) return
  await addSymbol(sid, sym)
  newSymbolInputs.value[sid] = ''
}

// LLM provider fields toggling
const isLlmCloud = computed(() => llmProvider.value === 'ollama_cloud')
const isLlmLocal = computed(() => llmProvider.value === 'local')

function handleProviderChange() {
  if (isLlmCloud.value) {
    llmBaseUrl.value = 'https://api.ollama.com/v1'
  } else if (isLlmLocal.value) {
    if (!llmBaseUrl.value || llmBaseUrl.value === 'https://api.ollama.com/v1') {
      llmBaseUrl.value = 'http://host.docker.internal:1234/v1'
    }
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

// Chart pricing formats
function formatPriceList(raw) {
  if (raw === null || raw === undefined) return '—'
  const s = String(raw).trim()
  if (!s) return '—'
  return s.replace(/\$/g, '').replace(/\d+(?:\.\d+)?/g, m => '$' + m)
}

function getOutlookColor(outlook) {
  const o = String(outlook).toUpperCase()
  if (o.includes('BULL')) return 'var(--color-green)'
  if (o.includes('BEAR')) return 'var(--color-red)'
  return 'var(--color-yellow)'
}

// Format trade legs
function getLegsList(actionJson) {
  return actionJson.legs || []
}

function isBuyLeg(leg) {
  const side = (leg.side || leg.action || '').toLowerCase()
  return side.includes('buy')
}

// Strategy parameters
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
</script>

<template>
  <div class="dashboard-grid">
    <!-- Main Panel -->
    <div class="dashboard-main">
      
      <!-- Trade Approval Queue -->
      <section class="card widget-card">
        <div class="card-header">
          <div class="header-title">
            <span>Trade Approval Queue</span>
            <span class="count-badge" v-if="state.approvals.pending.length">
              {{ state.approvals.pending.length }} pending
            </span>
          </div>
          <div class="actions">
            <button class="btn-action-text btn-approve-text" @click="bulkDecide('approve')"><Icon name="check" :size="14" /> Approve All</button>
            <button class="btn-action-text btn-reject-text" @click="bulkDecide('reject')"><Icon name="x" :size="14" /> Reject All</button>
            <button class="btn-ghost btn-sm-square" title="Refresh approvals" @click="loadApprovals"><Icon name="refresh-cw" :size="14" /></button>
            <button class="btn-ghost btn-sm-square" title="Toggle approval mode" @click="setApprovalMode(!state.status.approval_mode)"><Icon name="lock" :size="14" /></button>
          </div>
        </div>
        <div class="card-body no-padding">
          <div v-if="state.approvals.pending.length === 0" class="empty-state">
            <div class="empty-icon"><Icon name="check" :size="28" /></div>
            <p class="empty-text">No pending trades. The agent queues proposals here for review.</p>
          </div>
          <div v-else class="queue-list">
            <div 
              v-for="item in state.approvals.pending" 
              :key="item.id" 
              class="trade-card"
            >
              <div class="trade-card-header">
                <span class="trade-symbol">{{ item.symbol }}</span>
                <span class="trade-strategy">{{ item.strategy_id }}</span>
                <span class="trade-type" :class="item.action_type || 'entry'">
                  {{ (item.action_type || 'entry').toUpperCase() }}
                </span>
                <span v-if="item.action_json?.ai_authored" class="tag ai">AI</span>
                <span class="trade-age">{{ getRelativeTime(item.created_at) }}</span>
              </div>
              
              <div class="trade-legs">
                <div 
                  v-for="(leg, idx) in getLegsList(item.action_json)" 
                  :key="idx" 
                  class="leg-row"
                >
                  <span class="leg-side" :class="{ buy: isBuyLeg(leg), sell: !isBuyLeg(leg) }">
                    {{ (leg.side || leg.action || '').replace(/_/g, ' ').toUpperCase() }}
                  </span>
                  <span class="leg-symbol">{{ leg.option_symbol || leg.symbol || '—' }}</span>
                  <span class="leg-qty">×{{ leg.quantity || 1 }}</span>
                </div>
                <div v-if="!getLegsList(item.action_json).length" class="no-legs">
                  No legs defined
                </div>
              </div>

              <div class="trade-meta">
                <span v-if="item.action_json?.order_class" class="meta-tag">{{ item.action_json.order_class }}</span>
                <span v-if="item.action_json?.order_type" class="meta-tag">{{ item.action_json.order_type }}</span>
                <span v-if="item.action_json?.price != null" class="meta-tag pnl-green">
                  ${{ Math.abs(item.action_json.price).toFixed(2) }} {{ item.action_json.price >= 0 ? 'credit' : 'debit' }}
                </span>
                <span class="meta-tag">qty {{ item.action_json?.quantity || 1 }}</span>
                <span v-if="item.action_json?.expiry" class="meta-tag">exp {{ item.action_json.expiry }}</span>
                <span v-if="item.action_json?.dte != null" class="meta-tag">{{ item.action_json.dte }} DTE</span>
              </div>

              <div class="trade-actions">
                <input 
                  type="text" 
                  v-model="approvalNotes[item.id]" 
                  placeholder="Optional review note..." 
                  class="notes-input"
                />
                <button class="btn-approve btn-action-sm" @click="decide(item.id, 'approve', approvalNotes[item.id])">
                  Approve
                </button>
                <button class="btn-reject btn-action-sm" @click="decide(item.id, 'reject', approvalNotes[item.id])">
                  Reject
                </button>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- Strategy Watchlists & Lots -->
      <section class="card widget-card">
        <div class="card-header">
          <span class="header-title">Watchlists &amp; Lots</span>
          <span class="header-subtitle">per-strategy parameters</span>
        </div>
        <div class="card-body">
          <p class="section-desc">Each strategy ticks and scans its own watchlist. If empty, the global default is scanned.</p>
          <div class="global-default-bar">
            <strong>Global Default:</strong>
            <span v-if="!state.watchlistData.global_default?.length" class="text-muted">Loading...</span>
            <div v-else class="symbol-tags">
              <span v-for="sym in state.watchlistData.global_default" :key="sym" class="sym-tag">{{ sym }}</span>
            </div>
          </div>

          <div class="strategies-config">
            <div v-for="sid in state.watchlistData.strategies" :key="sid" class="strategy-watchlist-section">
              <div class="strategy-sec-header">
                <span class="strategy-sec-name">{{ sid }}</span>
                <span class="strategy-sec-desc">
                  {{ state.watchlistData.per_strategy[sid]?.length ? 'custom watchlist · ' + state.watchlistData.per_strategy[sid].length + ' symbols' : 'using global default' }}
                </span>
              </div>

              <!-- Lots Control Card -->
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

              <!-- Watchlist Symbols -->
              <div class="symbol-tags custom-watchlist-tags">
                <span 
                  v-for="sym in (state.watchlistData.per_strategy[sid]?.length ? state.watchlistData.per_strategy[sid] : state.watchlistData.global_default)" 
                  :key="sym" 
                  class="sym-tag editable-tag"
                >
                  {{ sym }}
                  <button
                    v-if="state.watchlistData.per_strategy[sid]?.length"
                    class="btn-remove-tag"
                    @click="removeSymbol(sid, sym)"
                    title="Remove symbol"
                  ><Icon name="x" :size="11" /></button>
                </span>
              </div>

              <!-- Add Symbol Inputs -->
              <div class="add-symbol-bar">
                <input 
                  type="text" 
                  v-model="newSymbolInputs[sid]" 
                  placeholder="Add symbol (e.g. AAPL)" 
                  class="add-symbol-input"
                  @keyup.enter="onAddSymbol(sid)"
                />
                <button class="btn-ghost btn-sm" @click="onAddSymbol(sid)">+ Add</button>
                <button
                  v-if="state.watchlistData.per_strategy[sid]?.length"
                  class="btn-ghost btn-sm btn-icon"
                  @click="resetWatchlist(sid)"
                  title="Reset to global default"
                ><Icon name="rotate-ccw" :size="13" /> Reset</button>
              </div>
              <div class="divider"></div>
            </div>
          </div>
        </div>
      </section>

      <!-- Recent Decisions -->
      <section class="card widget-card">
        <div class="card-header">
          <span class="header-title">Recent Decisions</span>
          <span class="header-subtitle">last 20 logs</span>
        </div>
        <div class="card-body no-padding">
          <div v-if="!recentDecisions.length" class="empty-state">
            <p class="empty-text">No recent decisions stored.</p>
          </div>
          <div v-else class="decisions-list">
            <div v-for="item in recentDecisions" :key="item.id" class="decision-row">
              <span class="dec-sym-strat">
                <strong>{{ item.symbol }}</strong>
                <span class="dec-strat">{{ item.strategy_id }}</span>
              </span>
              <span class="dec-status" :style="{ color: getDecisionColor(item.status) }">
                {{ item.status }}
              </span>
              <span class="dec-time">{{ getRelativeTime(item.decided_at || item.created_at) }}</span>
            </div>
          </div>
        </div>
      </section>

      <!-- Live Activity Log -->
      <section class="card widget-card">
        <div class="card-header">
          <span class="header-title">Agent Activity Log</span>
        </div>
        <div class="card-body log-feed-body">
          <div ref="logFeedRef" class="log-feed">
            <div v-if="!state.logs.length" class="log-line loading-text">Loading log feed...</div>
            <div 
              v-for="(log, idx) in state.logs" 
              :key="idx" 
              class="log-line"
              :class="{ 
                'log-error': (log.text || '').includes('ERROR'), 
                'log-c2': (log.text || '').includes('[C2]') 
              }"
            >{{ log.text }}</div>
          </div>
        </div>
      </section>

    </div>

    <!-- Right Sidebar Settings -->
    <aside class="dashboard-sidebar card">
      <div class="tab-header-row">
        <button 
          v-for="tab in ['soul', 'strats', 'agent', 'llm', 'charts']" 
          :key="tab"
          class="sidebar-tab-btn"
          :class="{ active: activeTab === tab }"
          @click="activeTab = tab; if (tab === 'charts') loadCharts();"
        >
          <Icon v-if="tab === 'charts'" name="chart-line" :size="15" />
          <template v-else>{{ tab.toUpperCase() }}</template>
        </button>
      </div>

      <div class="tab-content">
        <!-- SOUL TAB -->
        <div v-if="activeTab === 'soul'" class="tab-panel active">
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
            <textarea 
              v-model="soulText" 
              rows="16" 
              placeholder="Define operating principles here. (e.g. 'Never trade earnings week. Close spreads at 50% profit.')"
              :class="{ 'border-error': isSoulTooLarge }"
            ></textarea>
          </div>
          <button class="btn-primary w-full" :disabled="isSoulTooLarge" @click="saveSoul(soulText)">
            Save Soul Doctrine
          </button>
        </div>

        <!-- STRATEGY ENABLE TAB -->
        <div v-if="activeTab === 'strats'" class="tab-panel active">
          <div class="tab-sec-title">Enable / Disable Strategies</div>
          <p class="tab-sec-desc">Toggled strategies apply on the next loop tick. Disabled pipelines are fully bypassed.</p>
          <div class="strategy-toggles">
            <div 
              v-for="sid in ['CS75', 'CS7', 'TT45', 'WHEEL']" 
              :key="sid" 
              class="strategy-toggle-row"
            >
              <div class="strat-info-toggle">
                <span class="strat-name-toggle">{{ sid }}</span>
                <span class="strat-desc-toggle">P{{ STRAT_DETAILS[sid].prio }} · {{ STRAT_DETAILS[sid].desc }}</span>
              </div>
              <label class="toggle">
                <input 
                  type="checkbox" 
                  :checked="state.status.strategy_enabled?.[sid] !== false"
                  @change="toggleStrategy(sid, $event.target.checked)"
                />
                <span class="slider"></span>
              </label>
            </div>
          </div>
        </div>

        <!-- AGENT STATUS & MODE TAB -->
        <div v-if="activeTab === 'agent'" class="tab-panel active">
          <div class="tab-sec-title">Agent Status Diagnostics</div>
          <div class="status-grid-diag">
            <div class="diag-row">
              <span>Daemon Loop</span>
              <span :class="state.status.hermes_running ? 'text-green' : 'text-red'">
                {{ state.status.hermes_running ? 'Online' : 'Offline' }}
              </span>
            </div>
            <div class="diag-row">
              <span>Last Heartbeat</span>
              <span>
                {{ state.status.hermes_last_seen_age_s != null ? (state.status.hermes_last_seen_age_s < 60 ? Math.round(state.status.hermes_last_seen_age_s) + 's ago' : Math.round(state.status.hermes_last_seen_age_s / 60) + 'm ago') : 'never' }}
              </span>
            </div>
            <div class="diag-row">
              <span>Loop Uptime</span>
              <span>
                {{ state.status.uptime_s != null ? Math.round(state.status.uptime_s / 60) + 'm' : '—' }}
              </span>
            </div>
            <div class="diag-row">
              <span>Tradier API</span>
              <span 
                v-if="state.status.tradier_ok" 
                class="text-green"
              >OK</span>
              <span 
                v-else-if="state.status.tradier_error" 
                class="text-red hover-truncate" 
                :title="state.status.tradier_error"
              >Error</span>
              <span v-else class="text-muted">—</span>
            </div>
            <div class="diag-row">
              <span>XGBoost ML</span>
              <span 
                v-if="state.status.ml_ok" 
                class="text-green"
              >OK</span>
              <span 
                v-else-if="state.status.ml_error" 
                class="text-red hover-truncate" 
                :title="state.status.ml_error"
              >Error</span>
              <span v-else class="text-muted">—</span>
            </div>
            <div class="diag-row">
              <span>LLM Overseer</span>
              <span 
                v-if="state.status.llm_ok" 
                class="text-green"
              >OK ({{ state.status.llm_model }})</span>
              <span 
                v-else-if="state.status.llm_error" 
                class="text-red hover-truncate" 
                :title="state.status.llm_error"
              >Error</span>
              <span v-else class="text-muted">—</span>
            </div>
            <div class="diag-row">
              <span>Version</span>
              <div class="ver-wrapper">
                <span>{{ state.status.version || '—' }}</span>
                <span 
                  v-if="state.status.update_status?.update_available" 
                  class="update-badge"
                  @click="triggerUpdateInfo"
                >UPDATE</span>
              </div>
            </div>
          </div>
          
          <div class="divider"></div>
          <div class="tab-sec-title">Trading Mode Safety</div>
          <p class="tab-sec-desc">Switching to Live trading routes option order payloads directly to the Tradier exchange.</p>
          <div class="mode-toggles">
            <button 
              class="btn-ghost w-half" 
              :class="{ 'btn-active-blue': state.status.mode === 'paper' }"
              @click="setMode('paper')"
            >Paper</button>
            <button
              class="btn-ghost w-half"
              :class="{ 'btn-active-orange': state.status.mode === 'live' }"
              @click="setMode('live')"
            ><Icon name="alert" :size="13" /> Live</button>
          </div>
        </div>

        <!-- LLM OVERSEER CONFIG TAB -->
        <div v-if="activeTab === 'llm'" class="tab-panel active">
          <div class="tab-sec-title">LLM Client Configuration</div>
          <div class="form-group">
            <label>API Provider</label>
            <select v-model="llmProvider" @change="handleProviderChange">
              <option value="mock">Mock Overseer (No LLM)</option>
              <option value="local">Local Client (LM Studio / Ollama)</option>
              <option value="ollama_cloud">Ollama Cloud REST</option>
            </select>
          </div>
          <div class="form-group" v-if="!isLlmCloud">
            <label>Base URL Endpoint</label>
            <input type="text" v-model="llmBaseUrl" placeholder="http://localhost:1234/v1" />
          </div>
          <div class="form-group">
            <label>AI Model Identifier</label>
            <input type="text" v-model="llmModel" placeholder="hermes-3-llama-3.1-8b" />
          </div>
          <div class="form-row">
            <div class="form-group">
              <label>Temperature</label>
              <input type="number" v-model.number="llmTemp" min="0" max="2" step="0.05" />
            </div>
            <div class="form-group">
              <label>Timeout (s)</label>
              <input type="number" v-model.number="llmTimeout" min="5" max="600" />
            </div>
          </div>
          <div class="form-group">
            <label>Provider Auth Token Key</label>
            <input type="password" v-model="llmApiKey" placeholder="••••••••••••••••" />
            <div v-if="state.llm?.has_api_key" class="llm-key-saved-hint">
              <span><Icon name="check" :size="12" /> Key Saved</span>
              <span v-if="state.llm.api_key_hint" class="text-muted"> (ends in {{ state.llm.api_key_hint }})</span>
            </div>
          </div>
          <button class="btn-primary w-full" @click="onSaveLLM">Save LLM Configuration</button>
        </div>

        <!-- CHART VISION TAB -->
        <div v-if="activeTab === 'charts'" class="tab-panel active">
          <div class="chart-tab-header">
            <div class="tab-sec-title">Chart Vision Analysis</div>
            <button class="btn-ghost btn-xs" @click="loadCharts"><Icon name="refresh-cw" :size="12" /> Refresh</button>
          </div>
          <p class="tab-sec-desc">Candlestick images are annotated and scanned by the model overseer every tick. View latest updates below.</p>
          
          <div v-if="!state.chartsData.watchlist?.length" class="no-charts">
            No symbols in active watchlist.
          </div>
          <div v-else class="charts-feed">
            <div 
              v-for="sym in state.chartsData.watchlist" 
              :key="sym" 
              class="chart-card"
            >
              <div class="chart-card-header">
                <span class="chart-symbol">{{ sym }}</span>
                <span 
                  class="chart-outlook"
                  :style="{ color: getOutlookColor(state.chartsData.analyses[sym]?.decision?.outlook) }"
                >
                  {{ (state.chartsData.analyses[sym]?.decision?.outlook || 'NEUTRAL').toUpperCase() }}
                </span>
                <span class="chart-ts text-muted">
                  {{ getRelativeTime(state.chartsData.analyses[sym]?.ts) }}
                </span>
              </div>
              
              <div class="chart-img-container">
                <img 
                  :src="`/api/chart/${encodeURIComponent(sym)}/image?t=${Date.now()}`" 
                  :alt="sym + ' chart'" 
                  class="chart-img"
                  @error="$event.target.style.display='none'; $event.target.nextElementSibling.style.display='block'"
                />
                <div class="chart-img-error" style="display:none">
                  Chart image unavailable. Check matplotlib package config and price bar tables.
                </div>
              </div>

              <div class="chart-details">
                <div class="detail-row"><span>Trend</span> <strong>{{ state.chartsData.analyses[sym]?.decision?.trend || '—' }}</strong></div>
                <div class="detail-row"><span>Pattern</span> <strong>{{ state.chartsData.analyses[sym]?.decision?.pattern || '—' }}</strong></div>
                <div class="detail-row"><span>RSI Regime</span> <strong>{{ state.chartsData.analyses[sym]?.decision?.rsi_regime || '—' }}</strong></div>
                <div class="detail-row">
                  <span>BB Squeeze</span>
                  <strong>
                    <template v-if="state.chartsData.analyses[sym]?.decision?.bb_squeeze === true || state.chartsData.analyses[sym]?.decision?.bb_squeeze === 'true'"><Icon name="bolt" :size="13" /> Squeeze</template>
                    <template v-else>{{ state.chartsData.analyses[sym]?.decision?.bb_squeeze || '—' }}</template>
                  </strong>
                </div>
                <div class="detail-row"><span>Support</span> <strong>{{ formatPriceList(state.chartsData.analyses[sym]?.decision?.support) }}</strong></div>
                <div class="detail-row"><span>Resistance</span> <strong>{{ formatPriceList(state.chartsData.analyses[sym]?.decision?.resistance) }}</strong></div>
              </div>

              <div class="chart-rationale">
                {{ state.chartsData.analyses[sym]?.decision?.rationale || 'No analysis completed. Awaiting the next agent loop tick.' }}
              </div>
            </div>
          </div>
        </div>
      </div>
    </aside>
  </div>
</template>

<style scoped>
.dashboard-grid {
  display: grid;
  grid-template-columns: 1fr 340px;
  gap: 20px;
  align-items: start;
}

@media (max-width: 1200px) {
  .dashboard-grid {
    grid-template-columns: 1fr;
  }
}

.dashboard-main {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.widget-card {
  width: 100%;
}

.header-title {
  display: flex;
  align-items: center;
  gap: 10px;
  font-weight: 700;
  font-size: 14px;
}

.header-subtitle {
  font-size: 11px;
  font-weight: 400;
  color: var(--text-muted);
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

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 40px;
  text-align: center;
}

.empty-icon {
  font-size: 32px;
  color: var(--color-green);
  margin-bottom: 8px;
}

.empty-text {
  color: var(--text-muted);
  font-size: 13px;
}

.queue-list {
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.trade-card {
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  overflow: hidden;
  background: rgba(6, 9, 19, 0.3);
}

.trade-card-header {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 16px;
  background: rgba(255, 255, 255, 0.03);
  border-bottom: 1px solid var(--border-color);
}

.trade-symbol {
  font-size: 16px;
  font-weight: 700;
  color: var(--color-blue);
}

.trade-strategy {
  font-size: 11px;
  color: var(--text-muted);
  background: rgba(255, 255, 255, 0.05);
  padding: 2px 6px;
  border-radius: 4px;
}

.trade-type {
  font-size: 10px;
  font-weight: 700;
  padding: 2px 6px;
  border-radius: 4px;
  letter-spacing: 0.02em;
}
.trade-type.entry {
  color: var(--color-green);
  background: rgba(16, 185, 129, 0.1);
}
.trade-type.management {
  color: var(--color-yellow);
  background: rgba(245, 158, 11, 0.1);
}

.trade-age {
  margin-left: auto;
  font-size: 11px;
  color: var(--text-muted);
}

.trade-legs {
  padding: 12px 16px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.leg-row {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 13px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.02);
  padding-bottom: 4px;
}
.leg-row:last-child {
  border-bottom: none;
  padding-bottom: 0;
}

.leg-side {
  font-weight: 700;
  font-size: 11px;
  width: 90px;
}
.leg-side.buy {
  color: var(--color-green);
}
.leg-side.sell {
  color: var(--color-red);
}

.leg-symbol {
  font-family: var(--font-mono);
  color: var(--text-muted);
  flex-grow: 1;
}

.leg-qty {
  font-weight: 600;
}

.trade-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  padding: 8px 16px;
  background: rgba(255, 255, 255, 0.01);
  border-top: 1px solid rgba(255, 255, 255, 0.02);
}

.meta-tag {
  font-size: 11px;
  color: var(--text-muted);
  background: rgba(255, 255, 255, 0.05);
  padding: 2px 6px;
  border-radius: 4px;
}
.pnl-green {
  color: var(--color-green);
  background: rgba(16, 185, 129, 0.05);
}

.trade-actions {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
  background: rgba(255, 255, 255, 0.02);
  border-top: 1px solid var(--border-color);
}

.notes-input {
  flex-grow: 1;
  padding: 6px 10px;
  font-size: 12px;
}

.btn-action-sm {
  padding: 6px 12px;
  font-size: 12px;
}

.section-desc {
  font-size: 12px;
  color: var(--text-muted);
  margin-bottom: 16px;
}

.global-default-bar {
  display: flex;
  align-items: center;
  gap: 10px;
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid var(--border-color);
  padding: 10px 14px;
  border-radius: var(--radius-md);
  margin-bottom: 20px;
  font-size: 12px;
}

.symbol-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.sym-tag {
  background: rgba(255, 255, 255, 0.06);
  border: 1px solid var(--border-color);
  padding: 2px 8px;
  border-radius: 4px;
  font-weight: 600;
  font-size: 11px;
}

.editable-tag {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding-right: 4px;
}

.btn-remove-tag {
  background: transparent;
  color: var(--text-muted);
  border: none;
  font-size: 9px;
  padding: 2px;
  border-radius: 50%;
  width: 14px;
  height: 14px;
  display: flex;
  align-items: center;
  justify-content: center;
}
.btn-remove-tag:hover {
  background: rgba(239, 68, 68, 0.2);
  color: var(--color-red);
}

.strategies-config {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.strategy-watchlist-section {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.strategy-sec-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
}

.strategy-sec-name {
  font-weight: 800;
  font-size: 14px;
}

.strategy-sec-desc {
  font-size: 11px;
  color: var(--text-muted);
}

.lots-box {
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid var(--border-color);
  padding: 10px 14px;
  border-radius: var(--radius-md);
}

.lots-box-title {
  display: block;
  font-size: 9px;
  font-weight: 700;
  color: var(--text-muted);
  letter-spacing: 0.05em;
  margin-bottom: 6px;
}

.lots-box-controls {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 14px;
}

.lot-ctrl-group {
  display: flex;
  align-items: center;
  gap: 8px;
}
.ctrl-lbl {
  font-size: 11px;
  font-weight: 600;
  color: var(--text-muted);
}

.num-adjuster {
  display: inline-flex;
  align-items: center;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  overflow: hidden;
  background: rgba(0,0,0,0.2);
}
.num-adjuster button {
  background: transparent;
  color: var(--text-primary);
  width: 24px;
  height: 24px;
  border-radius: 0;
  padding: 0;
}
.num-adjuster button:hover {
  background: rgba(255, 255, 255, 0.05);
}
.num-adjuster input {
  width: 36px;
  height: 24px;
  border: none;
  border-left: 1px solid var(--border-color);
  border-right: 1px solid var(--border-color);
  background: transparent;
  text-align: center;
  font-size: 12px;
  font-weight: 700;
  padding: 0;
}

.custom-watchlist-tags {
  margin-top: 4px;
}

.add-symbol-bar {
  display: flex;
  gap: 8px;
  align-items: center;
}

.add-symbol-input {
  width: 180px;
  padding: 6px 10px;
  font-size: 12px;
}

.btn-icon {
  color: var(--text-muted);
}

.decisions-list {
  display: flex;
  flex-direction: column;
}

.decision-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px 20px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.03);
  font-size: 13px;
}
.decision-row:last-child {
  border-bottom: none;
}

.dec-sym-strat {
  display: flex;
  align-items: baseline;
  gap: 8px;
}
.dec-strat {
  font-size: 11px;
  color: var(--text-muted);
}

.dec-status {
  font-weight: 700;
}

.dec-time {
  font-size: 11px;
  color: var(--text-muted);
}

.log-feed-body {
  padding: 10px;
}

.log-feed {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--text-muted);
  line-height: 1.6;
  max-height: 240px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-all;
  background: rgba(0, 0, 0, 0.3);
  border-radius: var(--radius-md);
  padding: 12px;
  border: 1px solid var(--border-color);
}

.log-line {
  margin-bottom: 2px;
}
.log-error {
  color: var(--color-red);
}
.log-c2 {
  color: var(--color-blue);
}

.dashboard-sidebar {
  display: flex;
  flex-direction: column;
  background: var(--surface-glass);
}

.tab-header-row {
  display: flex;
  border-bottom: 1px solid var(--border-color);
  background: rgba(0, 0, 0, 0.2);
}

.sidebar-tab-btn {
  flex-grow: 1;
  border-radius: 0;
  background: transparent;
  color: var(--text-muted);
  border-bottom: 2px solid transparent;
  padding: 12px 6px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.05em;
  transition: all 0.15s ease;
}
.sidebar-tab-btn:hover {
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.02);
}
.sidebar-tab-btn.active {
  color: var(--color-blue);
  border-bottom-color: var(--color-blue);
  background: rgba(255, 255, 255, 0.04);
}

.tab-content {
  padding: 20px;
}

.tab-sec-title {
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-primary);
  margin-bottom: 12px;
  border-left: 2px solid var(--color-blue);
  padding-left: 8px;
}

.tab-sec-desc {
  font-size: 11px;
  color: var(--text-muted);
  line-height: 1.5;
  margin-bottom: 16px;
}

.textarea-label {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
}

.byte-count {
  font-size: 10px;
  font-weight: 400;
  text-transform: none;
}

textarea {
  font-size: 12px;
  line-height: 1.5;
}

.border-error {
  border-color: var(--color-red) !important;
}

.text-red {
  color: var(--color-red) !important;
}
.text-green {
  color: var(--color-green) !important;
}

.w-full {
  width: 100%;
}
.w-half {
  width: calc(50% - 6px);
}

.strategy-toggles {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.strategy-toggle-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px;
  background: rgba(255,255,255,0.02);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
}

.strat-info-toggle {
  display: flex;
  flex-direction: column;
}
.strat-name-toggle {
  font-weight: 700;
  font-size: 13px;
}
.strat-desc-toggle {
  font-size: 10px;
  color: var(--text-muted);
  margin-top: 2px;
}

.status-grid-diag {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-bottom: 20px;
}

.diag-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 12px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.02);
  padding-bottom: 6px;
}
.diag-row:last-child {
  border-bottom: none;
  padding-bottom: 0;
}

.diag-row span:first-child {
  color: var(--text-muted);
}
.diag-row span:last-child {
  font-weight: 600;
}

.hover-truncate {
  max-width: 160px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  cursor: pointer;
}

.ver-wrapper {
  display: flex;
  align-items: center;
  gap: 6px;
}

.update-badge {
  background: var(--color-orange);
  color: #060913;
  font-size: 8px;
  font-weight: 800;
  padding: 1px 4px;
  border-radius: 3px;
  cursor: pointer;
}

.mode-toggles {
  display: flex;
  gap: 12px;
}

.btn-active-blue {
  border-color: var(--color-blue);
  background: var(--color-blue-glow);
  color: #ffffff;
}

.btn-active-orange {
  border-color: var(--color-orange);
  background: var(--color-orange-glow);
  color: #ffffff;
}

.llm-key-saved-hint {
  font-size: 11px;
  color: var(--color-green);
  margin-top: 6px;
}

.chart-tab-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.btn-xs {
  font-size: 10px;
  padding: 4px 8px;
}

.no-charts {
  text-align: center;
  color: var(--text-muted);
  padding: 40px 0;
  font-size: 13px;
}

.charts-feed {
  display: flex;
  flex-direction: column;
  gap: 20px;
  max-height: 480px;
  overflow-y: auto;
  padding-right: 4px;
}

.chart-card {
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  overflow: hidden;
  background: rgba(0, 0, 0, 0.2);
}

.chart-card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 12px;
  background: rgba(255, 255, 255, 0.03);
  border-bottom: 1px solid var(--border-color);
  font-size: 12px;
}

.chart-symbol {
  font-weight: 700;
  color: var(--color-blue);
}

.chart-outlook {
  font-weight: 700;
}

.chart-ts {
  font-size: 10px;
}

.chart-img-container {
  border-bottom: 1px solid var(--border-color);
  background: #000;
}

.chart-img {
  width: 100%;
  display: block;
}

.chart-img-error {
  padding: 20px;
  font-size: 11px;
  color: var(--text-muted);
  text-align: center;
}

.chart-details {
  padding: 10px 12px;
  font-size: 11px;
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 4px 10px;
}

.detail-row {
  display: flex;
  justify-content: space-between;
  border-bottom: 1px solid rgba(255,255,255,0.02);
  padding-bottom: 2px;
}
.detail-row span {
  color: var(--text-muted);
}

.chart-rationale {
  padding: 8px 12px;
  border-top: 1px solid var(--border-color);
  font-size: 11px;
  color: var(--text-muted);
  line-height: 1.5;
  background: rgba(255, 255, 255, 0.01);
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

.btn-sm-square {
  width: 24px;
  height: 24px;
  padding: 0;
  display: flex;
  align-items: center;
  justify-content: center;
}

.btn-sm {
  padding: 6px 12px;
  font-size: 11px;
}
</style>
