<script setup>
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from 'vue'
import {
  state,
  loadStatus,
  loadWatchlist,
  loadSoul,
  loadLLM,
  loadLogs,
  saveSoul,
  saveAutonomy,
  toggleStrategy,
  saveLLM as apiSaveLLM,
  saveLots,
  addSymbol,
  removeSymbol,
  resetWatchlist
} from '../state'
import Icon from '../components/Icon.vue'

// Local Settings tab state
const activeTab = ref('soul')
const logFeedRef = ref(null)
const autoScroll = ref(true)
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

watch(() => state.watchlistData?.strategies, (sids) => {
  (sids || []).forEach(sid => {
    if (!lotInputs.value[sid]) lotInputs.value[sid] = { target: 5, max: 5 }
  })
}, { immediate: true, deep: true })

// Auto scroll logs
watch(() => state.logs, () => {
  nextTick(() => {
    if (logFeedRef.value && autoScroll.value) {
      const el = logFeedRef.value
      // Smart scrolling: only scroll to bottom if already near the bottom
      const isNearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 50
      if (isNearBottom) {
        el.scrollTop = el.scrollHeight
      }
    }
  })
}, { deep: true })

watch(autoScroll, (val) => {
  if (val) {
    nextTick(() => {
      if (logFeedRef.value) {
        logFeedRef.value.scrollTop = logFeedRef.value.scrollHeight
      }
    })
  }
})

watch(activeTab, (newTab) => {
  if (newTab === 'diagnostics' && autoScroll.value) {
    nextTick(() => {
      if (logFeedRef.value) {
        logFeedRef.value.scrollTop = logFeedRef.value.scrollHeight
      }
    })
  }
})

// Polling for updates while Settings is open
let pollInterval = null

onMounted(async () => {
  // Load initial data
  await Promise.all([
    loadStatus(),
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
  WHEEL: { hasTarget: false, targetDefault: 5,  maxDefault: 5 },
  HermesAlpha: { hasTarget: false, targetDefault: 1, maxDefault: 1 },
  DS0: { hasTarget: false, targetDefault: 1, maxDefault: 1 }
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

const STRAT_DETAILS = {
  CS75: { name: 'CS75', prio: 1, desc: 'Credit Spreads 75 DTE' },
  CS7:  { name: 'CS7', prio: 2, desc: 'Credit Spreads 7 DTE' },
  TT45: { name: 'TT45', prio: 3, desc: 'TastyTrade 45 DTE' },
  WHEEL: { name: 'WHEEL', prio: 4, desc: 'Wheel Strategy' },
  HermesAlpha: { name: 'HermesAlpha', prio: 5, desc: 'Self-Directed Strategy' }
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
  <div class="settings-page">
    
    <!-- Left Navigation Column -->
    <nav class="settings-tabs card">
      <div class="tabs-header">System Settings</div>
      <button 
        v-for="tab in ['soul', 'strats', 'watchlists', 'llm', 'diagnostics']" 
        :key="tab" 
        class="tab-btn" 
        :class="{ active: activeTab === tab }" 
        @click="activeTab = tab"
      >
        <Icon :name="tab === 'soul' ? 'clipboard' : tab === 'strats' ? 'sliders' : tab === 'watchlists' ? 'calculator' : tab === 'llm' ? 'lock' : 'alert'" :size="16" />
        {{ tab === 'soul' ? 'Soul Doctrine' : tab === 'strats' ? 'Strategies' : tab === 'watchlists' ? 'Watchlists' : tab === 'llm' ? 'LLM Overseer' : 'Diagnostics' }}
      </button>
    </nav>
    
    <!-- Right Content Card -->
    <main class="settings-content card">
      <div class="card-body">
        
        <!-- SOUL DOCTRINE -->
        <div v-if="activeTab === 'soul'" class="tab-panel">
          <div class="tab-sec-title">Agent Operating Doctrine</div>
          <p class="tab-sec-desc">The operating principles the LLM overseer follows: prompt boundaries, trade filters, safety rules, and how much autonomy it gets.</p>
          
          <div class="form-group">
            <label>Autonomy Level</label>
            <select v-model="autonomySelect" @change="saveAutonomy(autonomySelect)">
              <option value="advisory">Advisory — AI advises, operator decides</option>
              <option value="enforcing">Enforcing — AI can veto trades</option>
              <option value="autonomous">Autonomous — reserved (reviews like enforcing)</option>
            </select>
          </div>

          <div class="form-group">
            <label class="textarea-label">
              <span>Soul Doctrine Text</span>
              <span class="byte-count" :class="{ 'text-red': isSoulTooLarge }">
                {{ soulBytes }} / 65536 bytes
              </span>
            </label>
            <textarea v-model="soulText" rows="18" placeholder="Define operating principles..." :class="{ 'border-error': isSoulTooLarge }"></textarea>
          </div>
          <button class="btn-primary" :disabled="isSoulTooLarge" @click="saveSoul(soulText)">Save Doctrine</button>
        </div>
        
        <!-- STRATEGY TOGGLES -->
        <div v-if="activeTab === 'strats'" class="tab-panel">
          <div class="tab-sec-title">Enable / Disable Strategies</div>
          <p class="tab-sec-desc">Enabled strategies are evaluated every tick in priority order; disabled strategies are skipped entirely.</p>
          
          <div class="strategy-toggles">
            <div v-for="sid in (state.watchlistData?.strategies || ['CS75', 'CS7', 'TT45', 'WHEEL', 'HermesAlpha'])" :key="sid" class="strategy-toggle-row">
              <div class="strat-info-toggle">
                <span class="strat-name-toggle">{{ sid }}</span>
                <span class="strat-desc-toggle">P{{ STRAT_DETAILS[sid]?.prio }} · {{ STRAT_DETAILS[sid]?.desc }}</span>
              </div>
              <label class="toggle">
                <input type="checkbox" :checked="state.status.strategy_enabled?.[sid] !== false" @change="toggleStrategy(sid, $event.target.checked)" />
                <span class="slider"></span>
              </label>
            </div>
          </div>
        </div>
        
        <!-- WATCHLISTS & LOTS -->
        <div v-if="activeTab === 'watchlists'" class="tab-panel">
          <div class="tab-sec-title">Watchlists &amp; Lots Configuration</div>
          <p class="tab-sec-desc">Set the symbols each strategy scans and its target / max lot sizes. Strategies without a custom list fall back to the global default.</p>
          
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
                  <button class="btn-primary btn-sm" @click="onSaveLots(sid)">Save Lots</button>
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
        
        <!-- LLM OVERSEER CONFIG -->
        <div v-if="activeTab === 'llm'" class="tab-panel">
          <div class="tab-sec-title">LLM Client Configuration</div>
          <p class="tab-sec-desc">Provider, model, API key, and timeouts for the LLM overseer that reviews and proposes trades.</p>
          
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
              <label>Temperature</label>
              <input type="number" v-model.number="llmTemp" min="0" max="2" step="0.05" />
            </div>
            <div class="form-group">
              <label>Timeout (seconds)</label>
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
          <button class="btn-primary" @click="onSaveLLM">Save LLM Configuration</button>
        </div>
        
        <!-- DIAGNOSTICS -->
        <div v-if="activeTab === 'diagnostics'" class="tab-panel">
          <div class="tab-sec-title">Diagnostics</div>
          <p class="tab-sec-desc">Connection health checks and the live agent activity log.</p>
          
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
              <span>Tradier API Connection</span>
              <span :class="state.status.tradier_ok ? 'text-green' : 'text-red'">{{ state.status.tradier_ok ? 'OK' : 'Error' }}</span>
            </div>
            <div class="diag-row">
              <span>LLM Client status</span>
              <span :class="state.status.llm_ok ? 'text-green' : 'text-red'">{{ state.status.llm_ok ? 'OK' : 'Error' }}</span>
            </div>
            <div class="diag-row">
              <span>Execution Route</span>
              <span :class="state.status.mode === 'live' ? 'text-orange font-bold' : 'text-green font-bold'" style="text-transform: uppercase;">{{ state.status.mode }}</span>
            </div>
            <div class="diag-row">
              <span>Version</span>
              <div class="ver-wrapper">
                <span>{{ state.status.version || '—' }}</span>
                <span v-if="state.status.update_status?.update_available" class="update-badge" @click="triggerUpdateInfo">UPDATE</span>
              </div>
            </div>
          </div>
          
          <div class="divider"></div>
          <div class="log-header">
            <div class="tab-sec-title">Daemon Activity Log</div>
            <label class="auto-scroll-checkbox">
              <input type="checkbox" v-model="autoScroll" />
              <span>Auto-scroll</span>
            </label>
          </div>
          <div ref="logFeedRef" class="log-feed">
            <div v-if="!state.logs.length" class="log-line">Loading logs...</div>
            <div v-for="(log, idx) in state.logs" :key="idx" class="log-line" :class="{ 'log-error': log.text?.includes('ERROR'), 'log-c2': log.text?.includes('[C2]') }">
              {{ log.text }}
            </div>
          </div>
        </div>
        
      </div>
    </main>
    
  </div>
</template>

<style scoped>
.settings-page {
  display: grid;
  grid-template-columns: 240px 1fr;
  gap: 20px;
  align-items: start;
}

@media (max-width: 900px) {
  .settings-page {
    grid-template-columns: 1fr;
  }
}

.settings-tabs {
  background: var(--surface-glass);
  display: flex;
  flex-direction: column;
  padding: 12px;
  gap: 4px;
}

.tabs-header {
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  color: var(--text-muted);
  letter-spacing: 0.05em;
  padding: 10px 12px 14px;
  border-bottom: 1px solid var(--border-color);
  margin-bottom: 8px;
}

.tab-btn {
  background: transparent;
  color: var(--text-muted);
  border: none;
  font-size: 13px;
  font-weight: 600;
  padding: 10px 16px;
  border-radius: var(--radius-md);
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 12px;
  transition: all 0.15s ease;
  width: 100%;
  justify-content: flex-start;
}
.tab-btn:hover {
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.03);
}
.tab-btn.active {
  color: #ffffff;
  background: rgba(59, 130, 246, 0.18);
  border-left: 3px solid var(--color-blue);
  padding-left: 13px;
}

.settings-content {
  background: var(--surface-glass);
}

.tab-panel {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.tab-sec-title {
  font-size: 14px;
  font-weight: 700;
  color: var(--text-primary);
  border-left: 2px solid var(--color-blue);
  padding-left: 8px;
}

.tab-sec-desc {
  font-size: 12px;
  color: var(--text-muted);
  line-height: 1.5;
  margin-bottom: 12px;
}

/* Form Styles */
.form-group {
  display: flex;
  flex-direction: column;
  gap: 6px;
  margin-bottom: 12px;
}

.textarea-label {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  font-size: 11px;
}

.byte-count {
  font-size: 10px;
  color: var(--text-muted);
}

textarea {
  font-size: 12px;
  line-height: 1.5;
}

.border-error {
  border-color: var(--color-red) !important;
}

/* Watchlists Configuration */
.lots-box {
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid var(--border-color);
  padding: 10px 14px;
  border-radius: var(--radius-md);
  margin-bottom: 8px;
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
  border: none;
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
  color: var(--text-primary);
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
  cursor: pointer;
}
.btn-remove-tag:hover {
  background: rgba(239, 68, 68, 0.2);
  color: var(--color-red);
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

/* Strategy Toggles */
.strategy-toggles {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.strategy-toggle-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px;
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

/* Diagnostics & Log Feed */
.status-grid-diag {
  display: flex;
  flex-direction: column;
  gap: 8px;
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

.log-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}

.auto-scroll-checkbox {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 11px;
  color: var(--text-muted);
  cursor: pointer;
  user-select: none;
}

.auto-scroll-checkbox input {
  cursor: pointer;
  margin: 0;
}

.log-feed {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--text-muted);
  line-height: 1.6;
  max-height: 280px;
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

.text-red {
  color: var(--color-red) !important;
}
.text-green {
  color: var(--color-green) !important;
}
.text-orange {
  color: var(--color-orange) !important;
}
.font-bold {
  font-weight: 700;
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
</style>
