<script setup>
import { computed } from 'vue'
import { state } from '../state'

// Algo Bot Status — the single most important readout on the bar.
const botStatus = computed(() => {
  const s = state.status
  if (!s.hermes_running) return { cls: 'red', label: '● OFFLINE' }
  if (s.paused) return { cls: 'yellow', label: '⏸ HALTED' }
  return { cls: 'green', label: '● RUNNING' }
})

const modeLive = computed(() => String(state.status.mode || '').toLowerCase() === 'live')

// Health chips — each is { ok, label, detail } driven off the status roll-up.
const health = computed(() => {
  const s = state.status
  return [
    {
      key: 'broker',
      label: 'Broker',
      ok: !!s.tradier_ok,
      detail: s.tradier_error || (s.tradier_ok ? 'Tradier connected' : 'no recent Tradier heartbeat'),
    },
    {
      key: 'overseer',
      label: 'Overseer',
      ok: !!s.llm_ok,
      detail: s.llm_error || (s.llm_ok ? `${s.llm_provider || 'LLM'} healthy` : 'no recent LLM activity'),
    },
    {
      key: 'market',
      label: 'Market',
      ok: !!s.market_is_open,
      detail: s.market_is_open ? `open (${s.market_session})` : `closed (${s.market_session || 'unknown'})`,
    },
  ]
})

const lastTick = computed(() => {
  const age = state.status.hermes_last_seen_age_s
  if (age == null) return '—'
  if (age < 1) return '<1s ago'
  if (age < 90) return `${Math.round(age)}s ago`
  return `${Math.round(age / 60)}m ago`
})
</script>

<template>
  <div class="botops-bar">
    <div class="botops-left">
      <span class="botops-title">Bot Operations</span>
      <span class="bot-status pill" :class="botStatus.cls">{{ botStatus.label }}</span>
      <span class="bot-status pill" :class="modeLive ? 'orange' : 'blue'">
        {{ modeLive ? 'LIVE' : 'PAPER' }}
      </span>
    </div>

    <div class="botops-health">
      <div
        v-for="h in health"
        :key="h.key"
        class="health-chip"
        :class="{ ok: h.ok, bad: !h.ok }"
        :title="h.detail"
      >
        <span class="dot"></span>
        <span class="h-label">{{ h.label }}</span>
      </div>
      <div class="health-chip neutral" title="Time since the agent last wrote a log line">
        <span class="h-label">Last tick</span>
        <span class="h-val">{{ lastTick }}</span>
      </div>
    </div>
  </div>
</template>

<style scoped>
.botops-bar {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 10px 30px;
  background: rgba(6, 9, 19, 0.6);
  border-bottom: 1px solid var(--border-color);
}

.botops-left {
  display: flex;
  align-items: center;
  gap: 10px;
}
.botops-title {
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text-muted);
}
.bot-status {
  font-size: 11px;
  font-weight: 800;
  letter-spacing: 0.04em;
}

.botops-health {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-grow: 1;
  flex-wrap: wrap;
}
.health-chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 3px 10px;
  border-radius: 9999px;
  border: 1px solid var(--border-color);
  font-size: 11px;
  font-weight: 600;
  color: var(--text-muted);
  cursor: default;
}
.health-chip .dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--text-muted);
}
.health-chip.ok {
  color: var(--text-primary);
  border-color: var(--positive-glow);
}
.health-chip.ok .dot {
  background: var(--positive);
  box-shadow: 0 0 6px var(--positive);
}
.health-chip.bad {
  color: var(--negative);
  border-color: var(--negative-glow);
}
.health-chip.bad .dot {
  background: var(--negative);
  box-shadow: 0 0 6px var(--negative);
}
.health-chip .h-val {
  color: var(--text-primary);
  font-weight: 700;
}

@media (max-width: 900px) {
  .botops-bar {
    flex-wrap: wrap;
    padding: 10px 16px;
  }
}
</style>
