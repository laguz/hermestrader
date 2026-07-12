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

function fmtMoney(n, { signed = false } = {}) {
  if (n == null || Number.isNaN(n)) return '—'
  const abs = Math.abs(n).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  })
  const sign = signed ? (n > 0 ? '+' : n < 0 ? '−' : '') : ''
  return `${sign}$${abs}`
}
</script>

<template>
  <div class="trader-bar">
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
    </div>
  </div>
</template>

<style scoped>
.trader-bar {
  display: flex;
  align-items: center;
  gap: 20px;
  padding: 14px 30px;
  background: rgba(8, 14, 28, 0.85);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-lg, 12px);
  margin: 15px 30px 0 30px;
  font-size: 15px;
  position: sticky;
  top: 0;
  z-index: 85;
  backdrop-filter: blur(10px);
  width: auto;
  height: auto;
  box-sizing: border-box;
}

.tb-stats {
  display: flex;
  align-items: center;
  gap: 28px;
  flex-shrink: 0;
}

.tb-stat {
  display: flex;
  flex-direction: column;
  gap: 4px;
  min-width: 90px;
}

.tb-label {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--text-muted);
}

.tb-value {
  font-size: 22px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
  color: var(--text-primary, #ffffff);
}
.tb-value.pos { color: var(--color-green); }
.tb-value.neg { color: var(--color-red); }
</style>
