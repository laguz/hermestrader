<script setup>
import { computed } from 'vue'
import { state, decide, bulkDecide } from '../state'
import Icon from './Icon.vue'

const items = computed(() => state.approvals.pending || [])
const visible = computed(() => items.value.slice(0, 5))
const overflow = computed(() => Math.max(0, items.value.length - visible.value.length))

function shortLegs(item) {
  const legs = item.action_json?.legs
    || (item.action_json?.short_leg || item.action_json?.long_leg
        ? [item.action_json.short_leg, item.action_json.long_leg].filter(Boolean).map(s => ({ option_symbol: s }))
        : [])
  if (!legs.length) return ''
  return legs.map(l => l.option_symbol || l.symbol || '').filter(Boolean).slice(0, 2).join(' / ')
}

function priceTag(item) {
  const p = item.action_json?.price
  if (p == null) return ''
  return (p >= 0 ? '+$' : '−$') + Math.abs(p).toFixed(2)
}
</script>

<template>
  <div v-if="!state.calmMode" class="approvals-dock">
    <div class="dock-header">
      <span class="dock-title">Pending</span>
      <span v-if="items.length" class="dock-count">{{ items.length }}</span>
    </div>

    <div v-if="!items.length" class="dock-empty">All clear</div>

    <ul v-else class="dock-list">
      <li
        v-for="item in visible"
        :key="item.id"
        class="dock-item"
        :class="{ first: item === items[0] }"
      >
        <div class="dock-row">
          <span class="sym">{{ item.symbol }}</span>
          <span class="strat">{{ item.strategy_id }}</span>
          <span v-if="item.action_json?.ai_authored" class="ai">AI</span>
        </div>
        <div class="dock-row dock-sub">
          <span class="legs">{{ shortLegs(item) || (item.action_type || 'entry').toUpperCase() }}</span>
          <span class="price">{{ priceTag(item) }}</span>
        </div>
        <div class="dock-actions">
          <button
            class="dock-btn approve"
            @click="decide(item.id, 'approve')"
            :title="'Approve ' + item.symbol + ' (A acts on first)'"
          ><Icon name="check" :size="14" /></button>
          <button
            class="dock-btn reject"
            @click="decide(item.id, 'reject')"
            :title="'Reject ' + item.symbol + ' (R acts on first)'"
          ><Icon name="x" :size="14" /></button>
        </div>
      </li>
    </ul>

    <div v-if="overflow" class="dock-overflow">+{{ overflow }} more in Dashboard</div>

    <div v-if="items.length" class="dock-bulk">
      <button class="bulk-btn approve" @click="bulkDecide('approve')">Approve all</button>
      <button class="bulk-btn reject" @click="bulkDecide('reject')">Reject all</button>
    </div>
  </div>
</template>

<style scoped>
.approvals-dock {
  padding: 12px 14px;
  border-top: 1px solid var(--border-color);
  background: rgba(6, 9, 19, 0.5);
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.dock-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.dock-title {
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text-muted);
}
.dock-count {
  background: var(--color-orange);
  color: #060913;
  border-radius: 9999px;
  padding: 1px 8px;
  font-size: 10px;
  font-weight: 800;
}

.dock-empty {
  color: var(--text-muted);
  font-size: 11px;
  font-style: italic;
}

.dock-list {
  list-style: none;
  padding: 0;
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
  max-height: 320px;
  overflow-y: auto;
}

.dock-item {
  background: rgba(255, 255, 255, 0.03);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md, 6px);
  padding: 6px 8px;
  display: flex;
  flex-direction: column;
  gap: 3px;
}
.dock-item.first {
  border-color: var(--color-blue);
  box-shadow: 0 0 0 1px rgba(59, 130, 246, 0.18);
}

.dock-row {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 11px;
  font-variant-numeric: tabular-nums;
}
.dock-sub {
  font-size: 10px;
  color: var(--text-muted);
  justify-content: space-between;
}

.sym {
  font-weight: 700;
  color: var(--text-primary, #ffffff);
}
.strat {
  font-size: 9px;
  font-weight: 700;
  color: var(--color-blue);
  letter-spacing: 0.04em;
}
.ai {
  font-size: 8px;
  font-weight: 700;
  background: rgba(139, 92, 246, 0.18);
  color: #c4b5fd;
  border-radius: 4px;
  padding: 1px 4px;
}

.dock-actions {
  display: flex;
  gap: 4px;
  margin-top: 2px;
}
.dock-btn {
  flex: 1;
  padding: 3px 0;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 800;
  cursor: pointer;
  border: 1px solid transparent;
}
.dock-btn.approve {
  background: rgba(34, 197, 94, 0.15);
  color: var(--color-green);
  border-color: rgba(34, 197, 94, 0.3);
}
.dock-btn.approve:hover { background: rgba(34, 197, 94, 0.25); }
.dock-btn.reject {
  background: rgba(239, 68, 68, 0.12);
  color: var(--color-red);
  border-color: rgba(239, 68, 68, 0.28);
}
.dock-btn.reject:hover { background: rgba(239, 68, 68, 0.22); }

.dock-overflow {
  font-size: 10px;
  color: var(--text-muted);
  text-align: center;
}

.dock-bulk {
  display: flex;
  gap: 6px;
}
.bulk-btn {
  flex: 1;
  padding: 4px 0;
  border-radius: 4px;
  font-size: 10px;
  font-weight: 700;
  cursor: pointer;
  border: 1px solid var(--border-color);
  background: rgba(255, 255, 255, 0.02);
  color: var(--text-muted);
  letter-spacing: 0.04em;
  text-transform: uppercase;
}
.bulk-btn.approve:hover { color: var(--color-green); border-color: rgba(34, 197, 94, 0.4); }
.bulk-btn.reject:hover { color: var(--color-red); border-color: rgba(239, 68, 68, 0.4); }
</style>
