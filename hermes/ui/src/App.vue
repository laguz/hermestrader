<script setup>
import { onMounted, onUnmounted } from 'vue'
import { state, connectSSE, disconnectSSE, togglePause } from './state'
import StatusPill from './components/StatusPill.vue'

onMounted(() => {
  connectSSE()
})

onUnmounted(() => {
  disconnectSSE()
})
</script>

<template>
  <!-- Sidebar Navigation -->
  <aside class="sidebar">
    <div class="sidebar-header">
      <div class="logo">⚡ HERMES C2</div>
      <div class="connection-status" :class="{ connected: state.isConnected }">
        {{ state.isConnected ? 'Connected' : 'Connecting...' }}
      </div>
    </div>
    
    <nav class="sidebar-nav">
      <router-link to="/" class="nav-item" exact-active-class="active">
        <span class="icon">🎛</span> Dashboard
      </router-link>
      <router-link to="/analytics" class="nav-item" exact-active-class="active">
        <span class="icon">📊</span> Analytics
      </router-link>
    </nav>

    <div class="sidebar-footer">
      <div class="status-summary">
        <div class="status-title">System Status</div>
        
        <div class="status-item">
          <span class="lbl">Agent</span>
          <StatusPill :status="state.status.hermes_running" type="agent" />
        </div>
        
        <div class="status-item">
          <span class="lbl">Mode</span>
          <StatusPill :status="state.status.mode" type="mode" />
        </div>

        <div class="status-item">
          <span class="lbl">Approval</span>
          <StatusPill :status="state.status.approval_mode" type="approval" />
        </div>

        <div class="status-item">
          <span class="lbl">Market</span>
          <StatusPill 
            :status="state.status.market_session" 
            type="market" 
            :label="state.status.market_is_open ? '● OPEN' : undefined"
          />
        </div>

        <div v-if="state.status.paused" class="status-item">
          <span class="lbl">Agent Loop</span>
          <StatusPill status="paused" type="paused" />
        </div>
      </div>
    </div>
  </aside>

  <!-- Main Viewport -->
  <div class="main-viewport">
    <header class="main-header">
      <h2 class="view-title">
        {{ $route.name === 'Analytics' ? 'Hermes Analytics' : 'C2 Control Room' }}
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
          {{ state.status.paused ? '▶ Resume Agent' : '⏸ Pause Agent' }}
        </button>
      </div>
    </header>

    <main class="content-container">
      <router-view />
    </main>
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
  padding: 24px;
  border-bottom: 1px solid var(--border-color);
}

.logo {
  font-size: 18px;
  font-weight: 800;
  letter-spacing: 0.05em;
  color: var(--color-blue);
  text-shadow: 0 0 10px rgba(59, 130, 246, 0.3);
}

.connection-status {
  font-size: 10px;
  font-weight: 600;
  color: var(--text-muted);
  margin-top: 4px;
  display: inline-flex;
  align-items: center;
  gap: 4px;
}
.connection-status::before {
  content: '';
  display: inline-block;
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--color-red);
}
.connection-status.connected::before {
  background: var(--color-green);
  box-shadow: 0 0 6px var(--color-green);
}

.sidebar-nav {
  padding: 20px 12px;
  display: flex;
  flex-direction: column;
  gap: 4px;
  flex-grow: 1;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 10px 16px;
  color: var(--text-muted);
  font-weight: 600;
  border-radius: var(--radius-md);
  transition: all 0.2s ease;
}
.nav-item:hover {
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.03);
}
.nav-item.active {
  color: #ffffff;
  background: rgba(59, 130, 246, 0.15);
  border-left: 3px solid var(--color-blue);
  padding-left: 13px;
}
.nav-item .icon {
  font-size: 16px;
}

.sidebar-footer {
  padding: 20px;
  border-top: 1px solid var(--border-color);
  background: rgba(6, 9, 19, 0.4);
}

.status-summary {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.status-title {
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  color: var(--text-muted);
  letter-spacing: 0.05em;
  margin-bottom: 4px;
}

.status-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 12px;
}
.status-item .lbl {
  color: var(--text-muted);
  font-weight: 500;
}

.main-viewport {
  display: flex;
  flex-direction: column;
  height: 100vh;
  overflow-y: auto;
}

.main-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 20px 30px;
  background: rgba(12, 21, 39, 0.7);
  backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--border-color);
  position: sticky;
  top: 0;
  z-index: 90;
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

@media (max-width: 900px) {
  .sidebar {
    height: auto;
    position: relative;
    border-right: none;
    border-bottom: 1px solid var(--border-color);
  }
  .main-viewport {
    height: auto;
  }
}
</style>
