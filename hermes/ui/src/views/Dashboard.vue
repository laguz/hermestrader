<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import {
  state,
  loadStatus,
  loadApprovals,
  loadLogs,
  decide,
  bulkDecide,
  requestSetMode,
  requestAutonomousLive
} from '../state'
import Icon from '../components/Icon.vue'

// Local UI state
const approvalNotes = ref({})


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

// analyticsData.performance is keyed by strategy id — aggregate across them.
const realizedPnl = computed(() => {
  const perf = state.analyticsData?.performance || {}
  const strategies = Object.values(perf)
  if (!strategies.length) return null
  return strategies.reduce((acc, s) => acc + (s.total_pnl || 0), 0)
})

const totalPnl = computed(() => {
  const pnl = realizedPnl.value
  if (pnl == null) return '—'
  const sign = pnl >= 0 ? '+' : ''
  return `${sign}$${pnl.toLocaleString(undefined, { maximumFractionDigits: 2 })}`
})

const isPnlPositive = computed(() => {
  return realizedPnl.value == null ? true : realizedPnl.value >= 0
})

const lastActionText = computed(() => {
  const list = state.logs || []
  const tradingLogs = list.filter(l => l.text && !l.text.includes('Heartbeat') && !l.text.includes('tick'))
  return tradingLogs.length > 0 ? tradingLogs[tradingLogs.length - 1].text : 'Initialized agent loop'
})
</script>

<template>
  <div class="cockpit-container">

    <!-- Trading Controls — the arming panel. Everything here changes how
         orders get placed; pure status readouts live in BotOpsBar. -->
    <section class="card arming-card" :class="{ armed: state.status.mode === 'live' || state.status.alpha_autonomous_live }">
      <div class="card-header">
        <div class="cockpit-title-group">
          <Icon name="alert" :size="16" style="color: var(--live);" />
          <span class="header-title">Trading Controls</span>
        </div>
      </div>
      <div class="card-body cockpit-body">
        <div class="cockpit-grid">
          <div class="grid-item">
            <span class="lbl">Trading Route</span>
            <div class="mode-toggles-inline">
              <button
                class="btn-inline-toggle"
                :class="{ active: state.status.mode === 'paper' }"
                @click="requestSetMode('paper')"
              >Paper</button>
              <button
                class="btn-inline-toggle btn-live-inline"
                :class="{ active: state.status.mode === 'live' }"
                @click="requestSetMode('live')"
              >Live</button>
            </div>
          </div>

          <div class="grid-item">
            <span class="lbl">Auto-Execute (no approval)</span>
            <div class="mode-toggles-inline">
              <button
                class="btn-inline-toggle"
                :class="{ active: !state.status.alpha_autonomous_live }"
                @click="requestAutonomousLive(false)"
              >Off</button>
              <button
                class="btn-inline-toggle btn-live-inline"
                :class="{ active: state.status.alpha_autonomous_live }"
                @click="requestAutonomousLive(true)"
              >Auto</button>
            </div>
          </div>

          <div class="grid-item">
            <span class="lbl">Overseer Autonomy</span>
            <span class="val mode-badge" :class="state.soul?.autonomy">{{ (state.soul?.autonomy || 'advisory').toUpperCase() }}</span>
          </div>
        </div>
      </div>
    </section>

    <!-- Bottom Row Layout -->
    <div class="primary-layout">
      
      <!-- Left Column: Bot Summary -->
      <section class="card bot-status-card">
        <div class="card-header">
          <span class="header-title">Bot Summary</span>
        </div>
        <div class="card-body bot-content">
          <div class="bot-info-table">
            <div class="bot-info-row">
              <span class="lbl">Realized P&amp;L</span>
              <span class="val font-bold" :class="isPnlPositive ? 'text-green' : 'text-red'">{{ totalPnl }}</span>
            </div>

            <div class="bot-info-row">
              <span class="lbl">Open Positions</span>
              <span class="val">{{ (state.analyticsData?.open_trades || []).length }} Trades</span>
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

    <!-- Open Positions (first-class on the dashboard; full detail in Analytics) -->
    <section class="card table-card">
      <div class="card-header">
        <span class="header-title">Open Positions</span>
        <span class="text-muted text-xs">{{ (state.analyticsData?.open_trades || []).length }} open</span>
      </div>
      <div class="card-body no-padding overflow-x">
        <div v-if="!state.analyticsData?.open_trades?.length" class="no-data">
          No open positions.
        </div>
        <table v-else class="tbl">
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Strategy</th>
              <th>Side</th>
              <th>Short Strike</th>
              <th>Long Strike</th>
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
              <td>{{ t.short_strike != null ? '$' + t.short_strike.toFixed(2) : '—' }}</td>
              <td>{{ t.long_strike != null ? '$' + t.long_strike.toFixed(2) : '—' }}</td>
              <td>{{ t.lots }}</td>
              <td class="text-green">{{ t.entry_credit != null ? '$' + t.entry_credit.toFixed(2) : '—' }}</td>
              <td class="text-xs">{{ t.expiry || '—' }}</td>
              <td class="text-xs text-muted">{{ getRelativeTime(t.opened_at) }}</td>
              <td><span v-if="t.ai_authored" class="tag ai">AI</span><span v-else>—</span></td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

  </div>
</template>

<style scoped>
.cockpit-container {
  display: flex;
  flex-direction: column;
  gap: 20px;
  width: 100%;
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

.mode-toggles-inline {
  display: inline-flex;
  background: rgba(0,0,0,0.3);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  padding: 2px;
}
.btn-inline-toggle {
  background: transparent;
  color: var(--text-muted);
  border: none;
  font-size: var(--fs-sm);
  padding: 5px 14px;
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

/* Arming panel — everything in it changes how orders are placed, so it
   carries the LIVE (orange) accent instead of the neutral card border. */
.arming-card {
  width: 100%;
  border-color: rgba(249, 115, 22, 0.22);
}
.arming-card.armed {
  border-color: rgba(249, 115, 22, 0.5);
  box-shadow: 0 0 18px rgba(249, 115, 22, 0.08);
}

.cockpit-title-group {
  display: flex;
  align-items: center;
  gap: 8px;
}

.cockpit-body {
  padding: 18px 20px;
}

.cockpit-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 16px;
  width: 100%;
}

.grid-item {
  display: flex;
  flex-direction: column;
  gap: 6px;
  background: rgba(0, 0, 0, 0.15);
  padding: 12px 16px;
  border-radius: var(--radius-md);
  border: 1px solid rgba(255, 255, 255, 0.02);
}

.grid-item .lbl {
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  color: var(--text-muted);
  letter-spacing: 0.03em;
}

.grid-item .val {
  font-size: var(--fs-md);
  font-weight: var(--fw-bold);
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
  /* Cap to the viewport so a large queue scrolls internally instead of
     growing the page and pushing the per-card Approve buttons off-screen. */
  max-height: min(72vh, 720px);
  /* min-height:0 lets this grid item honor max-height; without it the default
     min-height:auto expands the card to fit all cards (the reported bug). */
  min-height: 0;
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
  /* Required for the scroll to engage: a flex child defaults to
     min-height:auto, which would grow the card instead of scrolling. */
  min-height: 0;
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
