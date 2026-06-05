<script setup>
import { onMounted, onUnmounted } from 'vue'
import {
  state,
  connectSSE,
  disconnectSSE,
  togglePause,
  setCalmMode,
  decideFirstPending,
  startAnalyticsAutoLoad,
  stopAnalyticsAutoLoad,
} from './state'
import TraderBar from './components/TraderBar.vue'
import BotOpsBar from './components/BotOpsBar.vue'
import Icon from './components/Icon.vue'

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
  } else if (k === 'c') {
    e.preventDefault()
    setCalmMode(!state.calmMode)
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
  <!-- Sidebar Navigation (Slim Mockup Style) -->
  <aside class="sidebar">
    <div class="sidebar-header">
      <div class="logo" title="HermesTrader"><Icon name="bolt" :size="20" /></div>
      <div class="connection-status" :class="{ connected: state.isConnected }" :title="state.isConnected ? 'Connected' : 'Connecting...'"></div>
    </div>
    
    <nav class="sidebar-nav">
      <router-link to="/" class="nav-item" exact-active-class="active" title="Dashboard">
        <span class="icon"><Icon name="dashboard" :size="20" /></span>
        <span v-if="state.status.pending_approvals > 0" class="nav-badge"></span>
      </router-link>
      <router-link to="/charts" class="nav-item" exact-active-class="active" title="Markets">
        <span class="icon"><Icon name="chart-line" :size="20" /></span>
      </router-link>
      <router-link to="/analytics" class="nav-item" exact-active-class="active" title="Bots">
        <span class="icon"><Icon name="bot" :size="20" /></span>
      </router-link>
    </nav>
  </aside>

  <!-- Main Viewport -->
  <div class="main-viewport">
    <div class="sticky-top">
      <header class="main-header">
        <h2 class="view-title">
          {{ $route.name === 'Analytics' ? 'Hermes Analytics' : $route.name === 'ChartVision' ? 'Chart Vision Analysis' : 'C2 Control Room' }}
        </h2>
        <div class="header-actions">
          <span v-if="state.status.pending_approvals > 0" class="pending-badge animate-pulse">
            {{ state.status.pending_approvals }} pending
          </span>
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

    <TraderBar />

    <main class="content-container">
      <router-view />
    </main>
  </div>

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
        <li><kbd>C</kbd><span>Toggle Calm Mode (hide tape, mute alerts)</span></li>
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
  padding: 24px 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  border-bottom: 1px solid var(--border-color);
}

.logo {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  color: var(--accent);
  text-shadow: 0 0 10px rgba(59, 130, 246, 0.3);
}

.connection-status {
  margin-top: 8px;
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
  padding: 24px 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 16px;
  flex-grow: 1;
}

.nav-item {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 44px;
  height: 44px;
  color: var(--text-muted);
  border-radius: var(--radius-md);
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
  position: relative;
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

.nav-badge {
  position: absolute;
  top: 6px;
  right: 6px;
  width: 8px;
  height: 8px;
  background-color: var(--color-orange);
  border-radius: 50%;
  box-shadow: 0 0 6px var(--color-orange);
}

.main-viewport {
  display: flex;
  flex-direction: column;
  height: 100vh;
  overflow-y: auto;
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
    flex-direction: row;
    gap: 12px;
    border-bottom: none;
  }
  .sidebar-nav {
    flex-direction: row;
    padding: 0;
    gap: 8px;
    flex-grow: 0;
  }
  .connection-status {
    margin-top: 0;
  }
  .main-viewport {
    height: auto;
  }
}
</style>
