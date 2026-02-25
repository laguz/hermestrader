document.addEventListener('DOMContentLoaded', () => {

    // --- Backtester Logic ---
    const form = document.getElementById('backtest-form');
    if (form) {
        initBacktester(form);
    }

    // --- AI Logic ---
    const btnTrain = document.getElementById('btn-train');
    if (btnTrain) {
        initAI();
    }

});

function initBacktester(form) {
    const loading = document.getElementById('loading');
    const chartContainer = document.getElementById('chart-container');
    const metricsContainer = document.getElementById('metrics-container');

    // Set default start date to 6 months ago
    const startDateInput = document.getElementById('start_date');
    if (startDateInput) {
        const today = new Date();
        const oneYearAgo = new Date(today.getTime() - (365 * 24 * 60 * 60 * 1000));
        startDateInput.valueAsDate = oneYearAgo;
    }

    form.addEventListener('submit', async (e) => {
        e.preventDefault();

        loading.classList.remove('hidden');
        chartContainer.innerHTML = '';
        metricsContainer.innerHTML = '';

        const formData = new FormData(form);
        const data = Object.fromEntries(formData.entries());

        try {
            console.log("Submitting backtest for:", data);

            const response = await fetch('/api/backtest', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });

            console.log("Response status:", response.status);

            if (!response.ok) {
                const errorData = await response.json();
                console.error("Backtest failed:", errorData);
                throw new Error(errorData.error || 'Backtest failed');
            }

            const result = await response.json();
            console.log("Backtest Result Received:", result);

            if (!result.dates || result.dates.length === 0) {
                metricsContainer.innerHTML = `<p style="color: #f59e0b">No data returned for this period.</p>`;
                loading.classList.add('hidden');
                return;
            }

            chartContainer.innerHTML = '';
            
            const chart = LightweightCharts.createChart(chartContainer, {
                layout: {
                    background: { type: 'solid', color: 'transparent' },
                    textColor: document.documentElement.classList.contains('dark') ? '#9ca3af' : '#4b5563',
                },
                grid: {
                    vertLines: { color: document.documentElement.classList.contains('dark') ? '#374151' : '#f3f4f6' },
                    horzLines: { color: document.documentElement.classList.contains('dark') ? '#374151' : '#f3f4f6' },
                },
                rightPriceScale: {
                    borderVisible: false,
                },
                timeScale: {
                    borderVisible: false,
                    timeVisible: true,
                },
            });

            const lineSeries = chart.addLineSeries({
                color: '#3b82f6',
                lineWidth: 2,
            });

            const chartData = [];
            result.dates.forEach((dateStr, index) => {
                let tStr = String(dateStr);
                if (tStr.length === 10 && tStr.includes('-')) {
                    chartData.push({ time: tStr, value: result.values[index] });
                } else {
                    chartData.push({ time: Math.floor(new Date(dateStr).getTime() / 1000), value: result.values[index] });
                }
            });

            chartData.sort((a, b) => {
                let timeA = typeof a.time === 'string' ? new Date(a.time).getTime() : a.time;
                let timeB = typeof b.time === 'string' ? new Date(b.time).getTime() : b.time;
                return timeA - timeB;
            });

            const uniqueChartData = chartData.filter((item, pos, ary) => {
                return !pos || item.time != ary[pos - 1].time;
            });

            if (uniqueChartData.length > 0) {
                lineSeries.setData(uniqueChartData);
                chart.timeScale().fitContent();
            }

            new ResizeObserver(entries => {
                if (entries.length === 0 || entries[0].target !== chartContainer) { return; }
                const newRect = entries[0].contentRect;
                chart.applyOptions({ height: newRect.height, width: newRect.width });
            }).observe(chartContainer);

            metricsContainer.innerHTML = `
                <div class="metrics-grid">
                    <div class="metric-card">
                        <h3>Total Return</h3>
                        <p style="color: ${result.metrics.total_return.includes('-') ? '#ef4444' : '#4ade80'}">${result.metrics.total_return}</p>
                    </div>
                    <div class="metric-card">
                        <h3>Final Value</h3>
                        <p>${result.metrics.final_value}</p>
                    </div>
                    <div class="metric-card">
                        <h3>Total Trades</h3>
                        <p>${result.metrics.trade_count}</p>
                    </div>
                </div>
            `;

            // Render Trades Table
            const tradesContainer = document.getElementById('trades-container');
            const tradesTableBody = document.querySelector('#trades-table tbody');
            tradesTableBody.innerHTML = '';

            if (result.trades && result.trades.length > 0) {
                tradesContainer.classList.remove('hidden');
                result.trades.forEach(trade => {
                    const row = document.createElement('tr');

                    let pnlHtml = '-';
                    if (trade.pnl !== undefined) {
                        const color = trade.pnl >= 0 ? '#4ade80' : '#ef4444';
                        pnlHtml = `<span style="color: ${color}">$${trade.pnl.toFixed(2)}</span>`;
                    }

                    let valueHtml = '';
                    if (trade.credit) valueHtml = `Credit: $${trade.credit.toFixed(2)}`;
                    else if (trade.price) valueHtml = `Price: $${trade.price.toFixed(2)}`;

                    row.innerHTML = `
                        <td>${trade.date}</td>
                        <td class="trade-action">${trade.action.replace(/_/g, ' ')}</td>
                        <td>${valueHtml}</td>
                        <td>${pnlHtml}</td>
                    `;
                    tradesTableBody.appendChild(row);
                });
            } else {
                tradesContainer.classList.add('hidden');
            }

        } catch (error) {
            console.error('Error:', error);
            metricsContainer.innerHTML = `<p style="color: #ef4444">Error: ${error.message}</p>`;
        } finally {
            loading.classList.add('hidden');
        }
    });
}

function initAI() {
    const btnTrain = document.getElementById('btn-train');
    const btnPredict = document.getElementById('btn-predict');
    const aiLoading = document.getElementById('ai-loading');
    const aiResult = document.getElementById('ai-result');

    btnTrain.addEventListener('click', async () => {
        const symbol = document.getElementById('symbol').value;
        if (!symbol) return alert('Please enter a symbol');

        aiLoading.classList.remove('hidden');
        aiLoading.innerText = `Training Random Forest for ${symbol}...`;
        aiResult.classList.add('hidden');

        try {
            const response = await fetch('/api/train', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ symbol })
            });
            const res = await response.json();

            if (res.error) throw new Error(res.error);

            aiResult.classList.remove('hidden');
            aiResult.innerHTML = `<p>✅ Model trained successfully! (MSE: ${res.mse})</p>`;

        } catch (e) {
            alert('Training Error: ' + e.message);
        } finally {
            aiLoading.classList.add('hidden');
        }
    });

    btnPredict.addEventListener('click', async () => {
        const symbol = document.getElementById('symbol').value;
        if (!symbol) return alert('Please enter a symbol');

        aiLoading.classList.remove('hidden');
        aiLoading.innerText = `Predicting next day price for ${symbol}...`;
        aiResult.classList.add('hidden');

        try {
            const response = await fetch('/api/predict', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ symbol })
            });
            const res = await response.json();

            if (res.error) throw new Error(res.error);

            const color = res.change >= 0 ? '#4ade80' : '#ef4444';
            const arrow = res.change >= 0 ? '▲' : '▼';

            aiResult.classList.remove('hidden');
            aiResult.innerHTML = `
                <div style="text-align: center;">
                    <h3>Prediction for ${res.prediction_date}</h3>
                    <div style="font-size: 2rem; font-weight: bold; margin: 10px 0;">
                        $${res.predicted_price.toFixed(2)}
                    </div>
                    <div style="color: ${color}; font-size: 1.1rem;">
                        ${arrow} ${res.change.toFixed(2)} (${res.percent_change_str})
                    </div>
                    <small>Previous Close: $${res.last_close}</small>
                </div>
            `;

        } catch (e) {
            alert('Prediction Error: ' + e.message);
        } finally {
            aiLoading.classList.add('hidden');
        }
    });
}
