<script setup>
import { ref, computed, watch, nextTick } from 'vue'
import { state, closeArmModal, setMode, setAutonomousLive } from '../state'
import Icon from './Icon.vue'

const typed = ref('')
const inputRef = ref(null)

const CONFIGS = {
  live: {
    word: 'LIVE',
    title: 'Switch to LIVE trading',
    lines: [
      'Orders will be placed against the real brokerage account with real money.',
      'The paper route, approval queue, and risk engine settings stay as configured.',
    ],
    confirmLabel: 'Go Live',
  },
  auto: {
    word: 'ARM',
    title: 'Arm Auto-Execute (no approval)',
    lines: [
      'EVERY strategy (CS75, CS7, TT45, Wheel, HermesAlpha) will place orders with NO human approval.',
      'Paper/Live still applies — real money is only at risk if the Live route is also on.',
    ],
    confirmLabel: 'Arm Auto-Execute',
  },
}

const cfg = computed(() => CONFIGS[state.armModal.kind] || CONFIGS.live)
const canConfirm = computed(() => typed.value.trim().toUpperCase() === cfg.value.word)

watch(() => state.armModal.open, (open) => {
  typed.value = ''
  if (open) nextTick(() => inputRef.value?.focus())
})

async function confirm() {
  if (!canConfirm.value) return
  const kind = state.armModal.kind
  closeArmModal()
  if (kind === 'live') await setMode('live')
  else if (kind === 'auto') await setAutonomousLive(true)
}
</script>

<template>
  <div
    v-if="state.armModal.open"
    class="arm-modal-backdrop"
    @click.self="closeArmModal"
  >
    <div class="arm-modal" role="alertdialog" aria-modal="true">
      <div class="arm-header">
        <Icon name="alert" :size="18" />
        <span>{{ cfg.title }}</span>
      </div>
      <p v-for="(line, i) in cfg.lines" :key="i" class="arm-line">{{ line }}</p>
      <label class="arm-type-label">
        Type <kbd>{{ cfg.word }}</kbd> to confirm
      </label>
      <input
        ref="inputRef"
        v-model="typed"
        type="text"
        class="arm-input"
        :placeholder="cfg.word"
        autocomplete="off"
        spellcheck="false"
        @keyup.enter="confirm"
        @keyup.esc="closeArmModal"
      />
      <div class="arm-actions">
        <button class="btn-ghost" @click="closeArmModal">Cancel</button>
        <button class="btn-arm" :disabled="!canConfirm" @click="confirm">
          {{ cfg.confirmLabel }}
        </button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.arm-modal-backdrop {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.6);
  z-index: 1100;
  display: flex;
  align-items: center;
  justify-content: center;
}

.arm-modal {
  background: rgba(12, 21, 39, 0.98);
  border: 1px solid rgba(249, 115, 22, 0.45);
  border-radius: var(--radius-md);
  padding: 20px 22px;
  width: 420px;
  max-width: 92vw;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.6), 0 0 24px var(--live-glow);
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.arm-header {
  display: flex;
  align-items: center;
  gap: 10px;
  font-weight: var(--fw-extrabold);
  font-size: var(--fs-md);
  color: var(--live);
  letter-spacing: 0.02em;
}

.arm-line {
  font-size: var(--fs-sm);
  color: var(--text-primary);
  line-height: 1.5;
}

.arm-type-label {
  margin-top: 4px;
  font-size: var(--fs-xs);
  color: var(--text-muted);
  text-transform: none;
  letter-spacing: normal;
}
.arm-type-label kbd {
  background: var(--live-glow);
  border: 1px solid rgba(249, 115, 22, 0.45);
  color: var(--live);
  border-radius: 4px;
  padding: 1px 6px;
  font-family: var(--font-mono);
  font-weight: var(--fw-bold);
}

.arm-input {
  font-family: var(--font-mono);
  font-weight: var(--fw-bold);
  letter-spacing: 0.1em;
  text-transform: uppercase;
}
.arm-input:focus {
  border-color: var(--live);
  box-shadow: 0 0 0 2px var(--live-glow);
}

.arm-actions {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 6px;
}

.btn-arm {
  background: var(--live);
  color: #060913;
  font-weight: var(--fw-bold);
}
.btn-arm:hover:not(:disabled) {
  filter: brightness(1.1);
  box-shadow: 0 0 12px var(--live-glow);
}
</style>
