/**
 * NIDS Intelligence Dashboard - Plotly.js Visualization Engine
 */

const PLOTLY_CONFIG = {
  responsive: true,
  displayModeBar: true,
  displaylogo: false,
  modeBarButtonsToRemove: ['lasso2d', 'select2d']
};

const DARK_LAYOUT_BASE = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: {
    family: 'Inter, -apple-system, sans-serif',
    color: '#94a3b8',
    size: 12
  },
  margin: { t: 30, r: 25, b: 50, l: 60 }
};

async function fetchJSON(url) {
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    console.error(`Failed to fetch ${url}:`, err);
    return null;
  }
}

function formatNumber(num) {
  if (num === null || num === undefined) return '-';
  return Number(num).toLocaleString();
}

async function initDashboard() {
  const statusDot = document.querySelector('.status-dot');
  const statusText = document.getElementById('statusText');
  const emptyBanner = document.getElementById('emptyStateBanner');

  // Check health and db existence
  const health = await fetchJSON('/api/health');
  if (health && health.db_exists) {
    statusDot.className = 'status-dot online';
    statusText.textContent = 'ONLINE (DEPLOY DB)';
    emptyBanner.style.display = 'none';
  } else {
    statusDot.className = 'status-dot offline';
    statusText.textContent = 'NO DB CONNECTED';
    emptyBanner.style.display = 'block';
  }

  // Load KPI metrics
  const kpis = await fetchJSON('/api/kpis');
  if (kpis && kpis.ready) {
    document.getElementById('valTotalFlows').textContent = formatNumber(kpis.total_flows);
    document.getElementById('valBenignFlows').textContent = formatNumber(kpis.benign_count);
    document.getElementById('valAttackFlows').textContent = formatNumber(kpis.attack_count);
    document.getElementById('valAttackTypes').textContent = formatNumber(kpis.unique_attack_types);

    const benignPct = (100 - kpis.attack_percentage).toFixed(2);
    document.getElementById('valBenignPct').textContent = `${benignPct}% of all evaluated traffic`;
    document.getElementById('valAttackPct').textContent = `${kpis.attack_percentage}% Threat Ratio`;
  }

  // Render Charts
  await renderClassDistribution();
  await renderCorrelationHeatmap();
  await renderVolumetricScatter();
}

/**
 * 1. Class Imbalance Analysis (Donut + Attack Categories Breakdown)
 */
async function renderClassDistribution() {
  const data = await fetchJSON('/api/class-distribution');
  if (!data || data.length === 0) return;

  // Split benign vs malicious
  let benignCount = 0;
  let attackCount = 0;
  const attackBreakdown = [];

  data.forEach(item => {
    if (item.is_attack === 0 || item.label.toUpperCase() === 'BENIGN') {
      benignCount += item.count;
    } else {
      attackCount += item.count;
      attackBreakdown.push({
        label: item.label,
        count: item.count,
        percentage: item.percentage
      });
    }
  });

  // Donut Chart: Binary Class Distribution
  const donutTrace = {
    labels: ['Benign Traffic', 'Malicious (Attack)'],
    values: [benignCount, attackCount],
    type: 'pie',
    hole: 0.65,
    marker: {
      colors: ['#10b981', '#f43f5e']
    },
    textinfo: 'label+percent',
    textposition: 'outside',
    insidetextorientation: 'radial',
    hoverinfo: 'label+value+percent'
  };

  const donutLayout = {
    ...DARK_LAYOUT_BASE,
    showlegend: false,
    margin: { t: 20, r: 40, b: 20, l: 40 }
  };

  Plotly.newPlot('chartBinaryDist', [donutTrace], donutLayout, PLOTLY_CONFIG);

  // Horizontal Bar Chart: Attack Types
  attackBreakdown.sort((a, b) => a.count - b.count);
  const barTrace = {
    y: attackBreakdown.map(a => a.label),
    x: attackBreakdown.map(a => a.count),
    type: 'bar',
    orientation: 'h',
    marker: {
      color: attackBreakdown.map(a => a.label.toLowerCase().includes('ddos') ? '#f59e0b' : '#a855f7'),
      line: { color: 'rgba(255,255,255,0.2)', width: 1 }
    },
    hovertemplate: '<b>%{y}</b><br>Flows: %{x:,}<extra></extra>'
  };

  const barLayout = {
    ...DARK_LAYOUT_BASE,
    margin: { t: 20, r: 25, b: 40, l: 120 },
    xaxis: {
      title: 'Flow Count',
      gridcolor: 'rgba(255,255,255,0.06)',
      zerolinecolor: 'rgba(255,255,255,0.1)'
    },
    yaxis: {
      gridcolor: 'rgba(255,255,255,0.06)'
    }
  };

  Plotly.newPlot('chartAttackBreakdown', [barTrace], barLayout, PLOTLY_CONFIG);
}

/**
 * 2. Feature Correlation Heatmap (Real Precomputed Matrix of 10-15 Features + is_attack)
 */
async function renderCorrelationHeatmap() {
  const data = await fetchJSON('/api/correlation-matrix');
  if (!data || !data.features || data.features.length === 0) return;

  const features = data.features;
  const matrix = data.matrix;

  // Format feature names nicely for display
  const cleanFeatureNames = features.map(f => {
    return f.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
  });

  const heatmapTrace = {
    z: matrix,
    x: cleanFeatureNames,
    y: cleanFeatureNames,
    type: 'heatmap',
    colorscale: [
      [0.0, '#312e81'],   // Dark Indigo
      [0.25, '#1e40af'],  // Blue
      [0.5, '#0f172a'],   // Slate Neutral (zero correlation)
      [0.75, '#0284c7'],  // Cyan-blue
      [1.0, '#06b6d4']    // Electric Cyan
    ],
    zmin: -1.0,
    zmax: 1.0,
    colorbar: {
      title: 'Pearson (r)',
      titleside: 'top',
      tickfont: { color: '#94a3b8' },
      titlefont: { color: '#ffffff' }
    },
    hovertemplate: '<b>%{y}</b> × <b>%{x}</b><br>Correlation: %{z:.3f}<extra></extra>'
  };

  const heatmapLayout = {
    ...DARK_LAYOUT_BASE,
    margin: { t: 30, r: 40, b: 120, l: 140 },
    xaxis: {
      tickangle: -40,
      gridcolor: 'rgba(255,255,255,0.04)'
    },
    yaxis: {
      autorange: 'reversed',
      gridcolor: 'rgba(255,255,255,0.04)'
    }
  };

  Plotly.newPlot('chartCorrelationHeatmap', [heatmapTrace], heatmapLayout, PLOTLY_CONFIG);
}

/**
 * 3. Volumetric Profiling: Flow Packet Rate vs. Flow Duration (Stratified Scatter)
 */
async function renderVolumetricScatter() {
  const records = await fetchJSON('/api/scatter-sample');
  if (!records || records.length === 0) return;

  // Group records by label so user can toggle categories in legend
  const groups = {};
  records.forEach(r => {
    const lbl = r.label || 'Unknown';
    if (!groups[lbl]) groups[lbl] = [];
    groups[lbl].push(r);
  });

  // Color palette for classes
  const colorMap = {
    'BENIGN': '#10b981',
    'DDoS': '#f59e0b',
    'PortScan': '#06b6d4',
    'Bot': '#ec4899',
    'Infiltration': '#a855f7',
    'Web Attack': '#e11d48'
  };

  const traces = [];

  // Add Benign first
  if (groups['BENIGN']) {
    const benignRows = groups['BENIGN'];
    traces.push({
      name: 'BENIGN',
      x: benignRows.map(r => Math.max(1, r.flow_duration || 0)),
      y: benignRows.map(r => Math.max(0.01, r.flow_packets_s || 0.01)),
      mode: 'markers',
      type: 'scatter',
      marker: {
        color: colorMap['BENIGN'],
        size: 5,
        opacity: 0.5
      },
      text: benignRows.map(r => `Port: ${r.destination_port || 'N/A'}<br>Mean Pkt Len: ${r.packet_length_mean ? r.packet_length_mean.toFixed(1) : 'N/A'}`),
      hovertemplate: '<b>BENIGN Flow</b><br>Duration: %{x} µs<br>Packet Rate: %{y:.2f} pkts/s<br>%{text}<extra></extra>'
    });
  }

  // Add Attack groups
  for (const [lbl, rows] of Object.entries(groups)) {
    if (lbl.toUpperCase() === 'BENIGN') continue;

    const isDDoS = lbl.toLowerCase().includes('ddos');
    const color = isDDoS ? '#f59e0b' : (colorMap[lbl] || '#a855f7');

    traces.push({
      name: lbl,
      x: rows.map(r => Math.max(1, r.flow_duration || 0)),
      y: rows.map(r => Math.max(0.01, r.flow_packets_s || 0.01)),
      mode: 'markers',
      type: 'scatter',
      marker: {
        color: color,
        size: isDDoS ? 7 : 6,
        opacity: isDDoS ? 0.8 : 0.65,
        line: isDDoS ? { color: '#ffffff', width: 0.8 } : undefined
      },
      text: rows.map(r => `Port: ${r.destination_port || 'N/A'}<br>Mean Pkt Len: ${r.packet_length_mean ? r.packet_length_mean.toFixed(1) : 'N/A'}`),
      hovertemplate: `<b>${lbl} Attack</b><br>Duration: %{x} µs<br>Packet Rate: %{y:.2f} pkts/s<br>%{text}<extra></extra>`
    });
  }

  const scatterLayout = {
    ...DARK_LAYOUT_BASE,
    xaxis: {
      title: 'Flow Duration (microseconds, log scale)',
      type: 'log',
      gridcolor: 'rgba(255,255,255,0.06)',
      zerolinecolor: 'rgba(255,255,255,0.1)'
    },
    yaxis: {
      title: 'Flow Packet Rate (Packets / Second, log scale)',
      type: 'log',
      gridcolor: 'rgba(255,255,255,0.06)',
      zerolinecolor: 'rgba(255,255,255,0.1)'
    },
    legend: {
      orientation: 'h',
      x: 0,
      y: 1.12,
      font: { color: '#cbd5e1', size: 11 }
    }
  };

  Plotly.newPlot('chartVolumetricScatter', traces, scatterLayout, PLOTLY_CONFIG);
}

// Initialize on DOM Ready
document.addEventListener('DOMContentLoaded', initDashboard);
