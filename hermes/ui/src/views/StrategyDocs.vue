<script setup>
import { computed, onMounted } from 'vue'
import { state, loadLots } from '../state'

onMounted(() => {
  loadLots()
})

const strategyLotMeta = computed(() => {
  const d = state.lotsData || {}
  return {
    CS75: { target: d.CS75?.target, max: d.CS75?.max },
    CS7:  { target: d.CS7?.target, max: d.CS7?.max },
    TT45: { target: d.TT45?.target, max: d.TT45?.max },
    WHEEL: { max: d.WHEEL?.max },
    HermesAlpha: { max: d.HERMESALPHA?.max },
    DS0: { max: d.DS0?.max },
    DS02: { target: d.DS02?.target, max: d.DS02?.max }
  }
})
</script>

<template>
  <div class="docs-container">
    <p class="tab-sec-desc">
      Hermes ticks seven strategies in priority order every loop, sizing entries through the
      money manager. Lot numbers below are live values from Settings → Watchlists.
    </p>

    <!-- CS75 Card -->
    <div class="card logic-card">
      <h3 class="logic-title">
        <span class="p-pill pill-blue">P1</span> CS75 — Credit Spread (Institutional POP)
      </h3>
      <div class="logic-summary-row">
        <div class="logic-summary-box">
          <span class="l-lbl">Strategy Type</span>
          <span class="l-val">Iron Condor / Credit Spreads</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">DTE Target Window</span>
          <span class="l-val">39 – 45 Days</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Spread Width</span>
          <span class="l-val">$5.00 width</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Target Lots</span>
          <span class="l-val">{{ strategyLotMeta.CS75?.target ?? '—' }}</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Max Lots</span>
          <span class="l-val">{{ strategyLotMeta.CS75?.max ?? '—' }}</span>
        </div>
      </div>
      <div class="logic-steps">
        <div class="logic-step"><span class="step-num">1</span><span><strong>Mode A (Initial Entry):</strong> Find expiry date inside DTE target window (39–45 days).</span></div>
        <div class="logic-step"><span class="step-num">2</span><span><strong>Mode B (Completions):</strong> Re-scans the options chain for missing sides on partially-filled condors (14-45 DTE).</span></div>
        <div class="logic-step"><span class="step-num">3</span><span><strong>K-Means Clustering:</strong> Feeds pivot walls to model. Calculates POP. Picks strike closest to 75% POP.</span></div>
        <div class="logic-step"><span class="step-num">4</span><span><strong>Premium:</strong> Verifies credit is &gt;= $1.25 for condor entry, and &gt;= $1.00 for completions.</span></div>
        <div class="formula">TP: Close at 50% max profit (DTE &gt;= 21) or 75% profit (DTE &lt; 21). SL: 2.5× entry debit. Exit: hard close at 8 DTE.</div>
      </div>
    </div>

    <!-- CS7 Card -->
    <div class="card logic-card">
      <h3 class="logic-title">
        <span class="p-pill pill-yellow">P2</span> CS7 — 7-Day Credit Spread
      </h3>
      <div class="logic-summary-row">
        <div class="logic-summary-box">
          <span class="l-lbl">Strategy Type</span>
          <span class="l-val">Short-term Spreads</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">DTE Target Window</span>
          <span class="l-val">Fixed 7 Days</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Spread Width</span>
          <span class="l-val">$1.00 width</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Target Lots</span>
          <span class="l-val">{{ strategyLotMeta.CS7?.target ?? '—' }}</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Max Lots</span>
          <span class="l-val">{{ strategyLotMeta.CS7?.max ?? '—' }}</span>
        </div>
      </div>
      <div class="logic-steps">
        <div class="logic-step"><span class="step-num">1</span><span><strong>Mode A (Initial Entry):</strong> Find expiry at exactly 7 DTE.</span></div>
        <div class="logic-step"><span class="step-num">2</span><span><strong>Mode B (Completions):</strong> Re-scans the options chain for missing sides on partially-filled condors (4-7 DTE).</span></div>
        <div class="logic-step"><span class="step-num">3</span><span><strong>K-Means Clustering:</strong> Feeds pivot walls to model. Calculates POP based on 3-month lookback. Require &gt; 75% POP on strikes.</span></div>
        <div class="logic-step"><span class="step-num">4</span><span><strong>Premium:</strong> Verifies min_credit = width × 0.12 → $1 × 0.12 = $0.12 minimum.</span></div>
        <div class="formula">TP: credit &lt;= $0.02. SL: 3× credit debit.</div>
      </div>
    </div>

    <!-- TT45 Card -->
    <div class="card logic-card">
      <h3 class="logic-title">
        <span class="p-pill pill-orange">P3</span> TT45 — TastyTrade 16-Delta Spread
      </h3>
      <div class="logic-summary-row">
        <div class="logic-summary-box">
          <span class="l-lbl">Strategy Type</span>
          <span class="l-val">Delta directional</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">DTE Target Window</span>
          <span class="l-val">30 – 60 Days</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Spread Width</span>
          <span class="l-val">$5.00 width</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Target Lots</span>
          <span class="l-val">{{ strategyLotMeta.TT45?.target ?? '—' }}</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Max Lots</span>
          <span class="l-val">{{ strategyLotMeta.TT45?.max ?? '—' }}</span>
        </div>
      </div>
      <div class="logic-steps">
        <div class="logic-step"><span class="step-num">1</span><span>Scan 30–60 DTE. Select short strike at delta ~0.16 (±0.05). Statistically yields OTM probabilities matching 84%.</span></div>
        <div class="logic-step"><span class="step-num">2</span><span>Buy wing protection at short strike ± $5. Require net credit &gt; $0.</span></div>
        <div class="formula">Time Exit: close at 21 DTE. Challenged: exit if short contract delta exceeds 0.30 (ITM protection).</div>
      </div>
    </div>

    <!-- WHEEL Card -->
    <div class="card logic-card">
      <h3 class="logic-title">
        <span class="p-pill pill-purple">P4</span> WHEEL — Equity Rotation
      </h3>
      <div class="logic-summary-row">
        <div class="logic-summary-box">
          <span class="l-lbl">Strategy Type</span>
          <span class="l-val">Cash-Secured Puts / Covered Calls</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">DTE Target Window</span>
          <span class="l-val">30 – 45 Days</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Strike Delta</span>
          <span class="l-val">Delta ~0.30</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Max Lots</span>
          <span class="l-val">{{ strategyLotMeta.WHEEL?.max ?? '—' }}</span>
        </div>
      </div>
      <div class="logic-steps">
        <div class="logic-step"><span class="step-num">1</span><span>Check shares. If holding stock, sell Covered Calls up to shares/100 count.</span></div>
        <div class="logic-step"><span class="step-num">2</span><span>If cash available, sell Cash-Secured Puts to top off up to max lots threshold.</span></div>
        <div class="formula">Wanted Calls = min(shares/100, max_lots) − open_calls. Wanted Puts = max_lots − (open_calls + open_puts). Roll ITM if DTE &lt; 7.</div>
      </div>
    </div>

    <!-- HermesAlpha Card -->
    <div class="card logic-card">
      <h3 class="logic-title">
        <span class="p-pill pill-green">P5</span> HermesAlpha — Autonomous LLM Book
      </h3>
      <div class="logic-summary-row">
        <div class="logic-summary-box">
          <span class="l-lbl">Strategy Type</span>
          <span class="l-val">Self-Directed LLM-Driven Setups</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">DTE Target Window</span>
          <span class="l-val">Overseer Defined</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Strike Selection</span>
          <span class="l-val">LLM Discretionary</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Max Lots</span>
          <span class="l-val">{{ strategyLotMeta.HermesAlpha?.max ?? '—' }}</span>
        </div>
      </div>
      <div class="logic-steps">
        <div class="logic-step"><span class="step-num">1</span><span>Evaluates market structure, technical indicators, and option chain details through the LLM Overseer.</span></div>
        <div class="logic-step"><span class="step-num">2</span><span>Selects custom credit spread or option structures based on the LLM's high-conviction ideas.</span></div>
        <div class="formula">Runs autonomously when Autonomy is set to Autonomous. Sized within the alpha max lots threshold.</div>
      </div>
    </div>

    <!-- DS0 Card -->
    <div class="card logic-card">
      <h3 class="logic-title">
        <span class="p-pill pill-cyan">P6</span> DS0 — 0DTE Reversion Debit Spreads
      </h3>
      <div class="logic-summary-row">
        <div class="logic-summary-box">
          <span class="l-lbl">Strategy Type</span>
          <span class="l-val">0DTE Debit Spreads (Mean Reversion)</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">DTE Target Window</span>
          <span class="l-val">0 DTE (Same-Day Expiry)</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Spread Width</span>
          <span class="l-val">$1.00 width</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Max Lots</span>
          <span class="l-val">{{ strategyLotMeta.DS0?.max ?? '—' }}</span>
        </div>
      </div>
      <div class="logic-steps">
        <div class="logic-step"><span class="step-num">1</span><span><strong>Range Qualification:</strong> A support level inside [open − ATR14, open] arms a <strong>put</strong> debit spread; a resistance level inside [open, open + ATR14] arms a <strong>call</strong> debit spread — reversion toward the level, not a touch-fade.</span></div>
        <div class="logic-step"><span class="step-num">2</span><span><strong>POP Gate:</strong> The qualifying level's 3-month POP (that it holds) must be ≥ 75% — same engine and number CS7 uses.</span></div>
        <div class="logic-step"><span class="step-num">3</span><span><strong>Entry:</strong> Day-limit buy at max $0.08 debit. Never repriced or chased; dies unfilled at day end. One shot per side per symbol per day.</span></div>
        <div class="logic-step"><span class="step-num">4</span><span><strong>Exit:</strong> Resting day-limit sell at $0.50 placed on fill. 3:01 PM ET sweep closes anything marked ≥ $0.11; below that rides to expiry. 3:50 PM assignment guard force-closes near-the-money spreads (QQQ is American-style / physically settled).</span></div>
        <div class="formula">No stop loss — the $0.08 debit is the entire accepted risk per side. Risk $0.08 to net $0.42 → 16% breakeven hit rate vs. the 75% POP-holds gate.</div>
      </div>
    </div>

    <!-- DS02 Card -->
    <div class="card logic-card">
      <h3 class="logic-title">
        <span class="p-pill pill-pink">P7</span> DS02 — 0DTE Implied-Move Iron Condors
        <span class="p-pill pill-muted">Default-Disabled</span>
      </h3>
      <div class="logic-summary-row">
        <div class="logic-summary-box">
          <span class="l-lbl">Strategy Type</span>
          <span class="l-val">0DTE Iron Condor (Implied-Move)</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">DTE Target Window</span>
          <span class="l-val">0 DTE (Same-Day Expiry)</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Spread Width</span>
          <span class="l-val">$1.00 width</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Target Lots</span>
          <span class="l-val">{{ strategyLotMeta.DS02?.target ?? '—' }}</span>
        </div>
        <div class="logic-summary-box">
          <span class="l-lbl">Max Lots</span>
          <span class="l-val">{{ strategyLotMeta.DS02?.max ?? '—' }}</span>
        </div>
      </div>
      <div class="logic-steps">
        <div class="logic-step"><span class="step-num">1</span><span><strong>Entry Window:</strong> ET 10:00–13:30 — skips unreliable opening quotes and stops early enough that residual credit still compensates for the resting size.</span></div>
        <div class="logic-step"><span class="step-num">2</span><span><strong>Implied-Move Signal:</strong> ATM straddle price (nearest-strike call mid + put mid) sets synthetic levels: spot − 1.0× straddle (support → put side) and spot + 1.0× straddle (resistance → call side). Self-adjusting as theta bleeds the straddle down through the day.</span></div>
        <div class="logic-step"><span class="step-num">3</span><span><strong>Shared Credit Engine:</strong> Same strike-selection stack as CS75/CS7/HermesAlpha — honest chain-delta POP ≥ 80%, short Δ in 0.05–0.20, credit ≥ 10% of width, highest-EV candidate wins. Sits out FOMC/CPI (macro blackout).</span></div>
        <div class="logic-step"><span class="step-num">4</span><span><strong>Management:</strong> TP at 50% of entry credit. SL at 2.5× credit (width-capped). Blanket EOD flatten at 15:45 ET — no per-trade proximity judgment.</span></div>
        <div class="formula">Ships default-disabled — must be armed by the operator, never enabled by a deploy. Independent design from DS0: reuses the shared credit-spread engine, not DS0's debit machinery.</div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.docs-container {
  display: flex;
  flex-direction: column;
  gap: 4px;
  max-width: 1100px;
}

.tab-sec-desc {
  font-size: 12px;
  color: var(--text-muted);
  line-height: 1.5;
  margin-bottom: 12px;
}

.logic-card {
  background: var(--surface-glass);
  padding: 20px;
  margin-bottom: 16px;
}

.logic-title {
  font-size: 14px;
  font-weight: 800;
  margin-bottom: 14px;
  display: flex;
  align-items: center;
  gap: 10px;
}

.p-pill {
  font-size: 10px;
  font-weight: 700;
  padding: 2px 8px;
  border-radius: 4px;
  color: #060913;
}
.pill-blue { background: var(--color-blue); }
.pill-yellow { background: var(--color-yellow); }
.pill-orange { background: var(--color-orange); }
.pill-purple { background: var(--color-purple); }
.pill-green { background: var(--color-green); }
.pill-cyan { background: var(--color-cyan); }
.pill-pink { background: var(--color-pink); }
.pill-muted { background: rgba(255, 255, 255, 0.08); color: var(--text-muted); font-weight: 600; }

.logic-summary-row {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  margin-bottom: 16px;
}

.logic-summary-box {
  flex-grow: 1;
  background: rgba(255, 255, 255, 0.02);
  border: 1px solid var(--border-color);
  padding: 10px 12px;
  border-radius: var(--radius-md);
  min-width: 120px;
}
.logic-summary-box .l-lbl {
  display: block;
  font-size: 9px;
  font-weight: 700;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.03em;
  margin-bottom: 4px;
}
.logic-summary-box .l-val {
  font-size: 13px;
  font-weight: 700;
}

.logic-steps {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.logic-step {
  display: flex;
  gap: 10px;
  font-size: 13px;
  align-items: flex-start;
}

.step-num {
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid var(--border-color);
  width: 20px;
  height: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 50%;
  font-size: 10px;
  font-weight: 700;
  flex-shrink: 0;
  margin-top: 1px;
}

.formula {
  background: rgba(0, 0, 0, 0.3);
  border: 1px solid var(--border-color);
  padding: 8px 12px;
  border-radius: var(--radius-md);
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--color-yellow);
  margin-top: 8px;
}
</style>
