<script setup>
import { computed } from 'vue'
import { state } from '../state'

const openTrades = computed(() => state.analyticsData?.open_trades || [])

const openCredit = computed(() => {
  // Sum entry_credit × lots × 100 across open trades — a rough exposure proxy
  // while a live mark-to-market endpoint doesn't exist yet.
  return openTrades.value.reduce((acc, t) => {
    const lots = Number(t.lots || 0)
    const credit = Number(t.entry_credit || 0)
    return acc + lots * credit * 100
  }, 0)
})

const todayPnl = computed(() => {
  const series = state.analyticsData?.pnl_series || []
  if (!series.length) return null
  const today = new Date().toISOString().slice(0, 10)
  const row = series.find(r => (r.day || '').slice(0, 10) === today)
  return row ? Number(row.realized_pnl || 0) : 0
})

const pendingCount = computed(() => state.approvals.pending.length)

function fmtMoney(n, { signed = false } = {}) {
  if (n == null || Number.isNaN(n)) return '—'
  const abs = Math.abs(n).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  })
  const sign = signed ? (n > 0 ? '+' : n < 0 ? '−' : '') : ''
  return `${sign}$${abs}`
}

function tradeBadge(t) {
  // Compact representation: e.g. "BPS 410/405 ×2"
  const kind = (t.side_type || '').toUpperCase()
  const strikes = t.short_strike && t.long_strike
    ? `${t.short_strike}/${t.long_strike}`
    : (t.short_strike || t.long_strike || '')
  return [kind, strikes, t.lots ? `×${t.lots}` : ''].filter(Boolean).join(' ')
}
</script>

<template>
  <div v-if="!state.calmMode" class="trader-bar">
    <div class="tb-stats">
      <div class="tb-stat">
        <span class="tb-label">Open</span>
        <span class="tb-value">{{ openTrades.length }}</span>
      </div>
      <div class="tb-stat">
        <span class="tb-label">Exposure</span>
        <span class="tb-value">{{ fmtMoney(openCredit) }}</span>
      </div>
      <div class="tb-stat">
        <span class="tb-label">Today P&amp;L</span>
        <span
          class="tb-value"
          :class="todayPnl == null ? '' : todayPnl >= 0 ? 'pos' : 'neg'"
        >{{ fmtMoney(todayPnl, { signed: true }) }}</span>
      </div>
      <div class="tb-stat">
        <span class="tb-label">Pending</span>
        <span class="tb-value" :class="pendingCount ? 'warn' : ''">{{ pendingCount }}</span>
      </div>
    </div>

    <div class="tb-tape" v-if="openTrades.length">
      <div
        v-for="t in openTrades.slice(0, 18)"
        :key="t.id"
        class="tape-chip"
        :title="`${t.strategy_id} · ${tradeBadge(t)} · entry ${fmtMoney(t.entry_credit * 100)}`"
      >
        <span class="chip-sym">{{ t.symbol }}</span>
        <span class="chip-strat">{{ t.strategy_id }}</span>
        <span class="chip-meta">{{ tradeBadge(t) }}</span>
      </div>
      <span v-if="openTrades.length > 18" class="tape-more">+{{ openTrades.length - 18 }}</span>
    </div>
    <div v-else class="tb-tape tb-tape-empty">No open positions</div>
  </div>
</template>

<style scoped>
.trader-bar {
  display: flex;
  align-items: center;
  gap: 18px;
  padding: 10px 30px;
  background: rgba(8, 14, 28, 0.85);
  border-bottom: 1px solid var(--border-color);
  font-size: 12px;
  position: sticky;
  top: 0;
  z-index: 85;
  backdrop-filter: blur(10px);
}

.tb-stats {
  display: flex;
  align-items: center;
  gap: 18px;
  flex-shrink: 0;
}

.tb-stat {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 64px;
}

.tb-label {
  font-size: 9px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--text-muted);
}

.tb-value {
  font-size: 15px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
  color: var(--text-primary, #ffffff);
}
.tb-value.pos { color: var(--color-green); }
.tb-value.neg { color: var(--color-red); }
.tb-value.warn { color: var(--color-orange); }

.tb-tape {
  display: flex;
  align-items: center;
  gap: 6px;
  overflow-x: auto;
  flex-grow: 1;
  scrollbar-width: thin;
}
.tb-tape-empty {
  color: var(--text-muted);
  font-style: italic;
  font-size: 11px;
}

.tape-chip {
  display: inline-flex;
  align-items: baseline;
  gap: 6px;
  padding: 4px 8px;
  background: rgba(59, 130, 246, 0.08);
  border: 1px solid rgba(59, 130, 246, 0.18);
  border-radius: var(--radius-md, 6px);
  white-space: nowrap;
  font-variant-numeric: tabular-nums;
}
.chip-sym {
  font-weight: 700;
  color: var(--text-primary, #ffffff);
}
.chip-strat {
  font-size: 10px;
  font-weight: 600;
  color: var(--color-blue);
  letter-spacing: 0.04em;
}
.chip-meta {
  font-size: 10px;
  color: var(--text-muted);
}
.tape-more {
  font-size: 11px;
  color: var(--text-muted);
  padding: 0 6px;
}
</style>
