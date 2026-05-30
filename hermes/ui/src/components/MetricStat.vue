<script setup>
import { computed } from 'vue'

// A single labelled metric (e.g. Total Equity, Day's P&L). `trend` colors the
// value: 'pos' green, 'neg' red, 'auto' picks from the numeric sign of value.
const props = defineProps({
  label: { type: String, required: true },
  value: { type: [String, Number], default: '—' },
  sub: { type: String, default: '' },
  trend: { type: String, default: '' }, // '', 'pos', 'neg', 'auto'
})

const trendClass = computed(() => {
  let t = props.trend
  if (t === 'auto') {
    const n = parseFloat(String(props.value).replace(/[^0-9.+-]/g, ''))
    t = isNaN(n) ? '' : n >= 0 ? 'pos' : 'neg'
  }
  return t === 'pos' ? 'pos' : t === 'neg' ? 'neg' : ''
})
</script>

<template>
  <div class="metric">
    <div class="metric-label">{{ label }}</div>
    <div class="metric-value" :class="trendClass">{{ value }}</div>
    <div v-if="sub" class="metric-sub">{{ sub }}</div>
  </div>
</template>

<style scoped>
.metric {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.metric-label {
  font-size: var(--fs-2xs);
  font-weight: var(--fw-bold);
  text-transform: uppercase;
  letter-spacing: var(--tracking-wide);
  color: var(--text-muted);
}
.metric-value {
  font-size: var(--fs-2xl);
  font-weight: var(--fw-bold);
  letter-spacing: -0.01em;
  color: var(--text-primary);
}
.metric-value.pos { color: var(--positive); }
.metric-value.neg { color: var(--negative); }
.metric-sub {
  font-size: var(--fs-xs);
  color: var(--text-muted);
}
</style>
