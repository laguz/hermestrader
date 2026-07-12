<script setup>
import { computed, onMounted, onUnmounted } from 'vue'
import { useRoute } from 'vue-router'
import {
  state,
  connectSSE,
  disconnectSSE,
  togglePause,
  decideFirstPending,
  startAnalyticsAutoLoad,
  stopAnalyticsAutoLoad,
} from './state'
import BotOpsBar from './components/BotOpsBar.vue'
import TraderBar from './components/TraderBar.vue'
import ArmModal from './components/ArmModal.vue'
import Icon from './components/Icon.vue'

const route = useRoute()

const armedLive = computed(() => state.status.mode === 'live')
const armedAuto = computed(() => !!state.status.alpha_autonomous_live)
const armedText = computed(() => {
  if (armedLive.value && armedAuto.value) {
    return 'LIVE + AUTO-EXECUTE — strategies place real-money orders with no approval'
  }
  if (armedLive.value) return 'LIVE TRADING — orders use real money'
  if (armedAuto.value) return 'AUTO-EXECUTE ARMED — entries skip the approval queue (paper route)'
  return ''
})

function isTypingTarget(el) {
  if (!el) return false
  const tag = (el.tagName || '').toLowerCase()
  return tag === 'input' || tag === 'textarea' || tag === 'select' || el.isContentEditable
}

function onKeyDown(e) {
  if (isTypingTarget(e.target)) return
  if (e.metaKey || e.ctrlKey || e.altKey) return

  const k = e.key.toLowerCase()
  if (k === 'a') {
    e.preventDefault()
    decideFirstPending('approve')
  } else if (k === 'r') {
    e.preventDefault()
    decideFirstPending('reject')
  } else if (k === ' ') {
    e.preventDefault()
    togglePause()
  } else if (k === '?' || (k === '/' && e.shiftKey)) {
    e.preventDefault()
    state.hotkeysHelpOpen = !state.hotkeysHelpOpen
  } else if (k === 'escape') {
    if (state.hotkeysHelpOpen) state.hotkeysHelpOpen = false
  }
}

onMounted(() => {
  connectSSE()
  startAnalyticsAutoLoad(60000)
  window.addEventListener('keydown', onKeyDown)
})

onUnmounted(() => {
  disconnectSSE()
  stopAnalyticsAutoLoad()
  window.removeEventListener('keydown', onKeyDown)
})
</script>

<template>
  <!-- Sidebar Navigation -->
  <aside class="sidebar">
    <div class="sidebar-header">
      <div class="logo" title="HermesTrader"><Icon name="logo" :size="22" /></div>
      <span class="brand">Hermes</span>
      <div class="connection-status" :class="{ connected: state.isConnected }" :title="state.isConnected ? 'Connected' : 'Connecting...'"></div>
    </div>

    <!-- Active state keys off route.name (not router-link's param matching,
         which drops activeness on /analytics/:tab child links). -->
    <nav class="sidebar-nav">
      <router-link to="/" class="nav-item" :class="{ active: route.name === 'Dashboard' }">
        <span class="icon"><Icon name="dashboard" :size="18" /></span>
        <span class="nav-label">Dashboard</span>
      </router-link>
      <router-link to="/charts" class="nav-item" :class="{ active: route.name === 'Markets' }">
        <span class="icon"><Icon name="chart-line" :size="18" /></span>
        <span class="nav-label">Markets</span>
      </router-link>
      <router-link to="/analytics" class="nav-item" :class="{ active: route.name === 'Analytics' }">
        <span class="icon"><Icon name="chart-bar" :size="18" /></span>
        <span class="nav-label">Analytics</span>
      </router-link>
      <router-link to="/docs" class="nav-item" :class="{ active: route.name === 'Docs' }">
        <span class="icon"><Icon name="book" :size="18" /></span>
        <span class="nav-label">Docs</span>
      </router-link>
      <router-link to="/settings" class="nav-item" :class="{ active: route.name === 'Settings' }">
        <span class="icon"><Icon name="settings" :size="18" /></span>
        <span class="nav-label">Settings</span>
      </router-link>
    </nav>
  </aside>

  <!-- Main Viewport -->
  <div class="main-viewport">
    <div class="sticky-top">
      <div v-if="armedText" class="armed-banner">
        <Icon name="alert" :size="14" />
        <span>{{ armedText }}</span>
        <router-link to="/" class="armed-manage">Manage</router-link>
      </div>
      <header class="main-header">
        <h2 class="view-title">{{ route.name }}</h2>
        <div class="header-actions">
          <router-link v-if="state.status.pending_approvals > 0" to="/" class="pending-badge animate-pulse">
            {{ state.status.pending_approvals }} pending
          </router-link>
          <button
            v-if="state.status.hermes_running"
            :class="state.status.paused ? 'btn-resume' : 'btn-pause'"
            @click="togglePause"
          >
            <Icon :name="state.status.paused ? 'play' : 'pause'" :size="14" />
            {{ state.status.paused ? 'Resume Agent' : 'Pause Agent' }}
          </button>
        </div>
      </header>

      <BotOpsBar />
    </div>

    <TraderBar v-if="route.name === 'Dashboard'" />

    <main class="content-container">
      <router-view />
    </main>
  </div>

  <!-- Typed-confirmation modal for Live / Auto-Execute arming -->
  <ArmModal />

  <!-- Hotkeys Help Modal -->
  <div
    v-if="state.hotkeysHelpOpen"
    class="hotkeys-modal-backdrop"
    @click.self="state.hotkeysHelpOpen = false"
  >
    <div class="hotkeys-modal">
      <div class="hk-header">
        <span>Keyboard shortcuts</span>
        <button class="hk-close" @click="state.hotkeysHelpOpen = false">×</button>
      </div>
      <ul class="hk-list">
        <li><kbd>A</kbd><span>Approve first pending trade</span></li>
        <li><kbd>R</kbd><span>Reject first pending trade</span></li>
        <li><kbd>Space</kbd><span>Pause / resume agent</span></li>

        <li><kbd>?</kbd><span>Show / hide this help</span></li>
        <li><kbd>Esc</kbd><span>Close help</span></li>
      </ul>
      <div class="hk-note">Shortcuts ignore inputs and textareas.</div>
    </div>
  </div>

  <!-- Global Toast Notification -->
  <div id="toast" :class="{ show: state.toast.show }" :style="{ borderColor: state.toast.isError ? 'var(--color-red)' : 'var(--color-green)' }">
    {{ state.toast.message }}
  </div>
</template>

<style scoped>
.sidebar {
  background: rgba(12, 21, 39, 0.95);
  border-right: 1px solid var(--border-color);
  display: flex;
  flex-direction: column;
  height: 100vh;
  position: sticky;
  top: 0;
  z-index: 100;
}

.sidebar-header {
  padding: 20px 16px;
  display: flex;
  align-items: center;
  gap: 10px;
  border-bottom: 1px solid var(--border-color);
}

.brand {
  font-size: var(--fs-md);
  font-weight: var(--fw-extrabold);
  letter-spacing: 0.02em;
  flex-grow: 1;
}

.logo {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  color: var(--accent);
  text-shadow: 0 0 10px rgba(59, 130, 246, 0.3);
}

.connection-status {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--danger);
  position: relative;
}
.connection-status.connected {
  background: var(--positive);
  box-shadow: 0 0 8px var(--positive);
}

.sidebar-nav {
  padding: 16px 10px;
  display: flex;
  flex-direction: column;
  gap: 4px;
  flex-grow: 1;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 10px 12px;
  color: var(--text-muted);
  border-radius: var(--radius-md);
  font-size: var(--fs-base);
  font-weight: var(--fw-semibold);
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}
.nav-item:hover {
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.04);
}
.nav-item.active {
  color: #ffffff;
  background: rgba(59, 130, 246, 0.18);
  box-shadow: 0 0 12px rgba(59, 130, 246, 0.15);
}
.nav-item .icon {
  display: inline-flex;
  align-items: center;
}

.main-viewport {
  display: flex;
  flex-direction: column;
  height: 100vh;
  overflow-y: auto;
  min-width: 0;
  max-width: 100%;
}

.sticky-top {
  position: sticky;
  top: 0;
  z-index: 90;
}

.main-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 20px 30px;
  background: rgba(12, 21, 39, 0.7);
  backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--border-color);
}

.view-title {
  font-size: 20px;
  font-weight: 700;
  letter-spacing: -0.01em;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 14px;
}

.pending-badge {
  background: var(--color-orange);
  color: #060913;
  border-radius: 9999px;
  padding: 3px 10px;
  font-size: 11px;
  font-weight: 700;
  text-decoration: none;
}
.pending-badge:hover {
  color: #060913;
  filter: brightness(1.1);
}

.armed-banner {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 30px;
  background: rgba(249, 115, 22, 0.14);
  border-bottom: 1px solid rgba(249, 115, 22, 0.4);
  color: var(--live);
  font-size: var(--fs-xs);
  font-weight: var(--fw-bold);
  letter-spacing: 0.04em;
  text-transform: uppercase;
}
.armed-manage {
  margin-left: auto;
  color: var(--live);
  border: 1px solid rgba(249, 115, 22, 0.45);
  border-radius: var(--radius-sm);
  padding: 1px 8px;
  font-size: var(--fs-2xs);
  text-transform: uppercase;
}
.armed-manage:hover {
  background: var(--live-glow);
  color: var(--live);
}

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.6; }
}
.animate-pulse {
  animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
}

.content-container {
  padding: 30px;
  flex-grow: 1;
}

.hotkeys-modal-backdrop {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.55);
  z-index: 1000;
  display: flex;
  align-items: center;
  justify-content: center;
}
.hotkeys-modal {
  background: rgba(12, 21, 39, 0.98);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md, 8px);
  padding: 20px 22px;
  width: 360px;
  max-width: 92vw;
  color: var(--text-primary, #ffffff);
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
}
.hk-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: 700;
  letter-spacing: 0.02em;
  margin-bottom: 12px;
}
.hk-close {
  background: transparent;
  border: none;
  color: var(--text-muted);
  font-size: 20px;
  line-height: 1;
  cursor: pointer;
}
.hk-list {
  list-style: none;
  padding: 0;
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.hk-list li {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 13px;
}
.hk-list kbd {
  min-width: 44px;
  text-align: center;
  background: rgba(59, 130, 246, 0.15);
  border: 1px solid rgba(59, 130, 246, 0.35);
  color: #ffffff;
  border-radius: 4px;
  padding: 2px 8px;
  font-family: inherit;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.04em;
}
.hk-note {
  margin-top: 14px;
  font-size: 11px;
  color: var(--text-muted);
  font-style: italic;
}

@media (max-width: 900px) {
  .sidebar {
    height: auto;
    position: relative;
    border-right: none;
    border-bottom: 1px solid var(--border-color);
    flex-direction: row;
    justify-content: space-between;
    padding: 0 20px;
  }
  .sidebar-header {
    padding: 12px 0;
    gap: 12px;
    border-bottom: none;
  }
  .sidebar-nav {
    flex-direction: row;
    padding: 0;
    gap: 8px;
    flex-grow: 0;
  }
  .brand,
  .nav-label {
    display: none;
  }
  .main-viewport {
    height: auto;
  }
}
</style>
