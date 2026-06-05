<script setup>
import { onMounted, onUnmounted } from 'vue'
import { state, loadCharts } from '../state'
import Icon from '../components/Icon.vue'

let chartsTimer = null

onMounted(() => {
  loadCharts()
  // Refresh annotated charts periodically, matching the agent loop cadence.
  chartsTimer = setInterval(loadCharts, 60000)
})

onUnmounted(() => {
  if (chartsTimer) {
    clearInterval(chartsTimer)
    chartsTimer = null
  }
})

// Relative timestamp for the latest analysis of each symbol
function getRelativeTime(iso) {
  if (!iso) return '—'
  const d = Math.round((Date.now() - new Date(iso)) / 1000)
  if (d < 60) return d + 's ago'
  if (d < 3600) return Math.round(d / 60) + 'm ago'
  return Math.round(d / 3600) + 'h ago'
}

// Chart pricing formats
function formatPriceList(raw) {
  if (raw === null || raw === undefined) return '—'
  const s = String(raw).trim()
  if (!s) return '—'
  return s.replace(/\$/g, '').replace(/\d+(?:\.\d+)?/g, m => '$' + m)
}

function getOutlookColor(outlook) {
  const o = String(outlook).toUpperCase()
  if (o.includes('BULL')) return 'var(--color-green)'
  if (o.includes('BEAR')) return 'var(--color-red)'
  return 'var(--color-yellow)'
}
</script>

<template>
  <div class="chart-vision-container">
    <div class="chart-tab-header">
      <div class="tab-sec-title">Chart Vision Analysis</div>
      <button class="btn-ghost btn-xs" @click="loadCharts"><Icon name="refresh-cw" :size="12" /> Refresh</button>
    </div>
    <p class="tab-sec-desc">Candlestick images are annotated and scanned by the model overseer every tick. View latest updates below.</p>

    <div v-if="!state.chartsData.watchlist?.length" class="no-charts">
      No symbols in active watchlist.
    </div>
    <div v-else class="charts-feed">
      <div
        v-for="sym in state.chartsData.watchlist"
        :key="sym"
        class="chart-card"
      >
        <div class="chart-card-header">
          <span class="chart-symbol">{{ sym }}</span>
          <span
            class="chart-outlook"
            :style="{ color: getOutlookColor(state.chartsData.analyses[sym]?.decision?.outlook) }"
          >
            {{ (state.chartsData.analyses[sym]?.decision?.outlook || 'NEUTRAL').toUpperCase() }}
          </span>
          <span class="chart-ts text-muted">
            {{ getRelativeTime(state.chartsData.analyses[sym]?.ts) }}
          </span>
        </div>

        <div class="chart-img-container">
          <img
            :src="`/api/chart/${encodeURIComponent(sym)}/image?t=${Date.now()}`"
            :alt="sym + ' chart'"
            class="chart-img"
            @error="$event.target.style.display='none'; $event.target.nextElementSibling.style.display='block'"
          />
          <div class="chart-img-error" style="display:none">
            Chart image unavailable. Check matplotlib package config and price bar tables.
          </div>
        </div>

        <div class="chart-details">
          <div class="detail-row"><span>Trend</span> <strong>{{ state.chartsData.analyses[sym]?.decision?.trend || '—' }}</strong></div>
          <div class="detail-row"><span>Pattern</span> <strong>{{ state.chartsData.analyses[sym]?.decision?.pattern || '—' }}</strong></div>
          <div class="detail-row"><span>RSI Regime</span> <strong>{{ state.chartsData.analyses[sym]?.decision?.rsi_regime || '—' }}</strong></div>
          <div class="detail-row">
            <span>BB Squeeze</span>
            <strong>
              <template v-if="state.chartsData.analyses[sym]?.decision?.bb_squeeze === true || state.chartsData.analyses[sym]?.decision?.bb_squeeze === 'true'"><Icon name="bolt" :size="13" /> Squeeze</template>
              <template v-else>{{ state.chartsData.analyses[sym]?.decision?.bb_squeeze || '—' }}</template>
            </strong>
          </div>
          <div class="detail-row"><span>Support</span> <strong>{{ formatPriceList(state.chartsData.analyses[sym]?.decision?.support) }}</strong></div>
          <div class="detail-row"><span>Resistance</span> <strong>{{ formatPriceList(state.chartsData.analyses[sym]?.decision?.resistance) }}</strong></div>
        </div>

        <div class="chart-rationale">
          {{ state.chartsData.analyses[sym]?.decision?.rationale || 'No analysis completed. Awaiting the next agent loop tick.' }}
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.chart-vision-container {
  max-width: 1200px;
}

.tab-sec-title {
  font-size: 14px;
  font-weight: 700;
  color: var(--text-primary);
}

.tab-sec-desc {
  font-size: 12px;
  color: var(--text-muted);
  margin: 4px 0 16px;
  line-height: 1.5;
}

.chart-tab-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.btn-xs {
  font-size: 10px;
  padding: 4px 8px;
}

.no-charts {
  text-align: center;
  color: var(--text-muted);
  padding: 40px 0;
  font-size: 13px;
}

.charts-feed {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.chart-card {
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  overflow: hidden;
  background: rgba(0, 0, 0, 0.2);
}

.chart-card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 12px;
  background: rgba(255, 255, 255, 0.03);
  border-bottom: 1px solid var(--border-color);
  font-size: 12px;
}

.chart-symbol {
  font-weight: 700;
  color: var(--color-blue);
}

.chart-outlook {
  font-weight: 700;
}

.chart-ts {
  font-size: 10px;
}

.chart-img-container {
  border-bottom: 1px solid var(--border-color);
  background: #000;
}

.chart-img {
  width: 100%;
  display: block;
}

.chart-img-error {
  padding: 20px;
  font-size: 11px;
  color: var(--text-muted);
  text-align: center;
}

.chart-details {
  padding: 10px 12px;
  font-size: 11px;
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 4px 10px;
}

.detail-row {
  display: flex;
  justify-content: space-between;
  border-bottom: 1px solid rgba(255,255,255,0.02);
  padding-bottom: 2px;
}
.detail-row span {
  color: var(--text-muted);
}

.chart-rationale {
  padding: 8px 12px;
  border-top: 1px solid var(--border-color);
  font-size: 11px;
  color: var(--text-muted);
  line-height: 1.5;
  background: rgba(255, 255, 255, 0.01);
}
</style>
