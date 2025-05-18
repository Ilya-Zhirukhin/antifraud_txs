const ws = new WebSocket(`ws://${location.host}/ws`);

// DOM elements
const totalT = document.getElementById('total-txs');
const fraudP = document.getElementById('fraud-pct');
const currAUC = document.getElementById('current-auc');
const timeBox = document.getElementById('time');
const statP99 = document.getElementById('stat-p99');
const statAvg = document.getElementById('stat-avg');
const statMAcc = document.getElementById('stat-model-acc');
const statAAcc = document.getElementById('stat-arf-acc');
const arfPct = document.getElementById('arf-pct');
const legitList = document.getElementById('legit-list');
const suspList = document.getElementById('susp-list');
const arfList = document.getElementById('arf-list');
const alertText = document.getElementById('alert-text');

// Chart.js
const ctx = document.getElementById('aucChart').getContext('2d');
const chartData = {
  labels: [],
  datasets: [{
    label: 'ROC-AUC',
    data: [],
    borderColor: '#F9C74F',
    backgroundColor: 'rgba(249,199,79,0.3)',
    fill: true,
    tension: 0.3
  }]
};
const aucChart = new Chart(ctx, {
  type: 'line',
  data: chartData,
  options: {
    responsive: true,
    scales: {
      x: { display: false },
      y: {
        min: 0, max: 1,
        grid: { color: 'rgba(255,255,255,0.1)' },
        ticks: { color: '#ECECEC' }
      }
    },
    plugins: { legend: { labels: { color: '#ECECEC' } } }
  }
});

// State
let totalCount = 0, fraudCount = 0, driftAlerts = 0, arfFraudCount = 0;
const latencies = [], maxLat = 200;
const modelAccArr = [], arfAccArr = [], maxAcc = 200;
const pendingPoints = [];

// Обновление графика каждые 100ms
setInterval(() => {
  if (!pendingPoints.length) return;
  pendingPoints.forEach(pt => {
    chartData.labels.push(pt.t);
    chartData.datasets[0].data.push(pt.auc);
  });
  if (chartData.labels.length > 50) {
    chartData.labels = chartData.labels.slice(-50);
    chartData.datasets[0].data = chartData.datasets[0].data.slice(-50);
  }
  aucChart.update();
  pendingPoints.length = 0;
}, 100);

// WebSocket handler
ws.onmessage = evt => {
  const msg = JSON.parse(evt.data);
  const t = new Date(msg.created_at).toLocaleTimeString();

  // — KPI Updates —
  totalCount++;
  if (msg.fraud_flag) fraudCount++;
  if (msg.arf_fraud_flag) arfFraudCount++;
  totalT.textContent = totalCount;
  fraudP.textContent = ((fraudCount / totalCount) * 100).toFixed(1) + '%';
  arfPct.textContent = ((arfFraudCount / totalCount) * 100).toFixed(1) + '%';
  currAUC.textContent = msg.drift_auc.toFixed(3);
  timeBox.textContent = t;

  // — Latency —
  if (typeof msg.response_time_ms === 'number') {
    latencies.push(msg.response_time_ms);
    if (latencies.length > maxLat) latencies.shift();
    const sorted = [...latencies].sort((a,b)=>a-b);
    const idx99 = Math.floor(sorted.length * 0.99);
    statP99.textContent = sorted[idx99]?.toFixed(0) || '0';
    statAvg.textContent = (latencies.reduce((a,v)=>a+v,0) / latencies.length).toFixed(1);
  }

  // — Accuracy —
  if (typeof msg.model_correct === 'number') {
    modelAccArr.push(msg.model_correct);
    if (modelAccArr.length > maxAcc) modelAccArr.shift();
    statMAcc.textContent = ((modelAccArr.reduce((a,v)=>a+v,0) / modelAccArr.length) * 100).toFixed(1) + '%';
  }
  if (typeof msg.arf_correct === 'number') {
    arfAccArr.push(msg.arf_correct);
    if (arfAccArr.length > maxAcc) arfAccArr.shift();
    statAAcc.textContent = ((arfAccArr.reduce((a,v)=>a+v,0) / arfAccArr.length) * 100).toFixed(1) + '%';
  }

  // — AUC Drift Points —
  pendingPoints.push({ t, auc: msg.drift_auc });

  // — Transaction rendering —
  const li = document.createElement('li');
  li.className = 'txn ' + (msg.fraud_flag ? 'red' : 'green');
  li.innerHTML = `
    <span class="icon">${msg.fraud_flag ? '🔴' : '🟢'}</span>
    <div class="details">
      <strong>${t}</strong><br>${msg.amount} ${msg.currency}
    </div>`;

  if (msg.fraud_flag) {
    suspList.prepend(li);
    if (suspList.children.length > 40) suspList.removeChild(suspList.lastChild);
  } else {
    legitList.prepend(li);
    if (legitList.children.length > 40) legitList.removeChild(legitList.lastChild);
  }

  // — ARF prediction rendering —
  if (msg.arf_fraud_flag) {
    const liArf = document.createElement('li');
    liArf.className = 'txn blue';
    liArf.innerHTML = `
      <span class="icon">🧠</span>
      <div class="details">
        <strong>${t}</strong><br>${msg.amount} ${msg.currency}
      </div>`;
    arfList.prepend(liArf);
    if (arfList.children.length > 40) arfList.removeChild(arfList.lastChild);
  }

  // — Alerts —
  if (msg.drift_alerts > driftAlerts) {
    driftAlerts = msg.drift_alerts;
    alertText.textContent = `⚡ Drift #${driftAlerts} detected!`;
    setTimeout(() => {
      alertText.textContent = `Total drifts: ${driftAlerts}`;
    }, 5000);
  }
};
