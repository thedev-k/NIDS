/**
 * NIDS Intelligence Dashboard — Bespoke Chart Engine
 * Plotly.js Visualizations + GSAP Entrance Animations
 *
 * Implements:
 * - displayModeBar: false (clean, bespoke chart styling with no default toolbar)
 * - Deep custom Plotly theming for Flight Tracker Dark Material
 * - Custom center cutout & annotations for Class Imbalance Donut
 * - Wide margins and rounded breathing room for Attack Vector Bar Chart
 * - Dynamic update of HorizonX telemetry progress indicators
 */

/* ── Plotly Bespoke Config: Toolbar Completely Hidden ──────────────────── */
const PLOTLY_CONFIG = {
  responsive: true,
  displayModeBar: false, // Strict requirement: remove default hover toolbar
  displaylogo: false
};

/* ── Dark Base Theme Layout ────────────────────────────────────────────── */
const DARK_LAYOUT = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor:  'rgba(0,0,0,0)',
  font: {
    family: 'Inter, -apple-system, BlinkMacSystemFont, sans-serif',
    color:  '#8c96a8',
    size:   11
  }
};

const GRID_COLOR = 'rgba(255, 255, 255, 0.05)';
const ZERO_COLOR = 'rgba(255, 255, 255, 0.09)';

/* ── Utility Helpers ───────────────────────────────────────────────────── */
async function fetchJSON(url) {
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status} — ${url}`);
    return await res.json();
  } catch (err) {
    console.error('[NIDS API Error]', err.message);
    return null;
  }
}

function fmt(n) {
  if (n === null || n === undefined) return '—';
  return Number(n).toLocaleString();
}

function el(id) {
  return document.getElementById(id);
}

/* ── Main Dashboard Initialization ─────────────────────────────────────── */
async function initDashboard() {

  /* 1. GSAP Entrance Animation (with reduced-motion guard) */
  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (window.gsap && !prefersReducedMotion) {
    gsap.fromTo(
      '.animate-card',
      { opacity: 0, y: 16 },
      {
        opacity: 1,
        y: 0,
        duration: 0.5,
        stagger: 0.06,
        ease: 'power2.out',
        clearProps: 'all' // Prevents inline styles from ever trapping the DOM
      }
    );
  }

  /* 2. Health Check & Live Beacon */
  const statusDot   = el('statusDot');
  const statusText  = el('statusText');
  const emptyBanner = el('emptyStateBanner');

  const health = await fetchJSON('/api/health');
  if (health && health.db_exists) {
    if (statusDot)  statusDot.className = 'status-dot online';
    if (statusText) statusText.textContent = 'ONLINE (DEPLOY DB)';
    if (emptyBanner) emptyBanner.style.display = 'none';
  } else {
    if (statusDot)  statusDot.className = 'status-dot offline';
    if (statusText) statusText.textContent = 'NO DB DETECTED';
    if (emptyBanner) emptyBanner.style.display = 'flex';
  }

  /* 3. KPI Telemetry Metrics */
  const kpis = await fetchJSON('/api/kpis');
  if (kpis && kpis.ready) {
    const total = fmt(kpis.total_flows);

    if (el('valTotalFlows'))   el('valTotalFlows').textContent   = total;
    if (el('valBenignFlows'))  el('valBenignFlows').textContent  = fmt(kpis.benign_count);
    if (el('valAttackFlows'))  el('valAttackFlows').textContent  = fmt(kpis.attack_count);
    if (el('valAttackTypes'))  el('valAttackTypes').textContent  = fmt(kpis.unique_attack_types);

    const benignPct = (100 - kpis.attack_percentage).toFixed(1);
    const attackPct = Number(kpis.attack_percentage).toFixed(1);

    if (el('valBenignPct')) el('valBenignPct').textContent = `${benignPct}% Normal Operational Traffic`;
    if (el('valAttackPct')) el('valAttackPct').textContent = `${attackPct}% Threat Ratio (Class 1)`;

    /* Update Dynamic Progress Bar in Hero KPI Card */
    if (el('kpiFlowBarBenign')) el('kpiFlowBarBenign').style.width = `${benignPct}%`;
    if (el('kpiFlowBarAttack')) el('kpiFlowBarAttack').style.width = `${attackPct}%`;
    if (el('kpiFlowTextBenign')) el('kpiFlowTextBenign').textContent = `${benignPct}% Benign`;
    if (el('kpiFlowTextAttack')) el('kpiFlowTextAttack').textContent = `${attackPct}% Malicious`;

    /* Dynamic Sample Size Callouts in Navbar */
    if (el('sampleFlowsCount'))  el('sampleFlowsCount').textContent  = total;
    if (el('sampleCountBanner')) el('sampleCountBanner').textContent = total;
  }

  /* 4. Render All Visualizations in Parallel */
  await Promise.all([
    renderClassDistribution(),
    renderCorrelationHeatmap(),
    renderVolumetricScatter()
  ]);
}

/* ══════════════════════════════════════════════════════════════════════
   VISUALIZATION 1: Binary Class Imbalance Donut + Attack Breakdown Bar
   ══════════════════════════════════════════════════════════════════════ */
async function renderClassDistribution() {
  const data = await fetchJSON('/api/class-distribution');
  if (!data || data.length === 0) return;

  let benignCount = 0;
  let attackCount = 0;
  const breakdown = [];

  data.forEach(item => {
    if (item.is_attack === 0 || item.label.toUpperCase() === 'BENIGN') {
      benignCount += item.count;
    } else {
      attackCount += item.count;
      breakdown.push({ label: item.label, count: item.count });
    }
  });

  const totalEvaluated = benignCount + attackCount;
  const benignRatio = totalEvaluated > 0 ? Math.round((benignCount / totalEvaluated) * 100) : 80;
  const attackRatio = totalEvaluated > 0 ? Math.round((attackCount / totalEvaluated) * 100) : 20;

  /* ── A) Donut Chart: Clean Center Cutout & Bespoke Center Readout ── */
  Plotly.newPlot('chartBinaryDist', [{
    labels: ['Benign Traffic', 'Malicious (Attack)'],
    values: [benignCount, attackCount],
    type: 'pie',
    hole: 0.72, // Clean deep center cutout
    marker: {
      colors: ['#10b981', '#f43f5e'],
      line: { color: '#14161c', width: 3 } // Matching dark card background gap
    },
    textinfo: 'label+percent',
    textposition: 'outside',
    outsidetextfont: { color: '#8c96a8', size: 11, family: 'Inter, sans-serif' },
    hoverinfo: 'label+value+percent'
  }], {
    ...DARK_LAYOUT,
    showlegend: false,
    margin: { t: 30, r: 45, b: 30, l: 45 },
    annotations: [
      {
        text: 'RATIO',
        x: 0.5,
        y: 0.58,
        xref: 'paper',
        yref: 'paper',
        showarrow: false,
        font: {
          family: 'Inter, sans-serif',
          size: 10,
          color: '#64748b'
        }
      },
      {
        text: `<b>${benignRatio} / ${attackRatio}</b>`,
        x: 0.5,
        y: 0.44,
        xref: 'paper',
        yref: 'paper',
        showarrow: false,
        font: {
          family: 'JetBrains Mono, monospace',
          size: 20,
          color: '#f1f3f8'
        }
      }
    ]
  }, PLOTLY_CONFIG);

  /* ── B) Horizontal Bar Chart: Bespoke Breathing Room & Margins ── */
  breakdown.sort((a, b) => a.count - b.count);

  const barColors = breakdown.map(item => {
    const lbl = item.label.toLowerCase();
    if (lbl.includes('ddos'))         return '#f59e0b'; // Neon amber
    if (lbl.includes('portscan'))     return '#06b6d4'; // Electric cyan
    if (lbl.includes('web'))          return '#f43f5e'; // Crimson
    if (lbl.includes('bot'))          return '#a855f7'; // Purple
    if (lbl.includes('patator'))      return '#38bdf8'; // Sky blue
    if (lbl.includes('hulk'))         return '#f97316'; // Orange
    if (lbl.includes('infiltration')) return '#ec4899'; // Pink
    return '#818cf8';                                   // Indigo
  });

  Plotly.newPlot('chartAttackBreakdown', [{
    y: breakdown.map(item => item.label),
    x: breakdown.map(item => item.count),
    type: 'bar',
    orientation: 'h',
    marker: {
      color: barColors,
      line: { color: 'rgba(255, 255, 255, 0.12)', width: 1 }
    },
    hovertemplate: '<b>%{y}</b><br>Detected flows: %{x:,}<extra></extra>'
  }], {
    ...DARK_LAYOUT,
    bargap: 0.35, // Generous breathing room between bars
    /* Left margin expanded to 220px so long labels like "Web Attack - Brute Force" are 100% visible */
    margin: { t: 15, r: 25, b: 40, l: 220 },
    xaxis: {
      title: { text: 'Flow Count', font: { size: 10, color: '#8c96a8' } },
      gridcolor: GRID_COLOR,
      zerolinecolor: ZERO_COLOR,
      tickfont: { color: '#8c96a8', size: 10 }
    },
    yaxis: {
      automargin: true,
      tickfont: { size: 11, color: '#e2e8f0', family: 'Inter, sans-serif' },
      gridcolor: GRID_COLOR
    }
  }, PLOTLY_CONFIG);
}

/* ══════════════════════════════════════════════════════════════════════
   VISUALIZATION 2: 13×13 Pearson Correlation Heatmap
   ══════════════════════════════════════════════════════════════════════ */
async function renderCorrelationHeatmap() {
  const data = await fetchJSON('/api/correlation-matrix');
  if (!data || !data.features || data.features.length === 0) return;

  const names = data.features.map(f =>
    f.replace(/_/g, ' ').replace(/\b\w/g, char => char.toUpperCase())
  );

  const flatVals = data.matrix.flat().filter(v => v !== null && v !== undefined);
  const zMin = flatVals.length > 0 ? Math.max(0, Math.floor(Math.min(...flatVals) * 10) / 10 - 0.05) : 0.3;

  Plotly.newPlot('chartCorrelationHeatmap', [{
    z: data.matrix,
    x: names,
    y: names,
    type: 'heatmap',
    colorscale: [
      [0.0,  '#14161c'],  // Deep card neutral (minimum correlation)
      [0.25, '#1e293b'],  // Slate blue
      [0.5,  '#1d4ed8'],  // Rich royal blue
      [0.75, '#06b6d4'],  // Electric cyan
      [1.0,  '#a5f3fc']   // Bright specular cyan (perfect 1.0 correlation)
    ],
    zmin: zMin,
    zmax: 1.0,
    colorbar: {
      title: { text: 'Pearson (r)', font: { color: '#f1f3f8', size: 11 } },
      titleside: 'top',
      tickfont: { color: '#8c96a8', size: 10 },
      thickness: 12,
      len: 0.85
    },
    hovertemplate: '<b>%{y}</b> × <b>%{x}</b><br>r = %{z:.3f}<extra></extra>'
  }], {
    ...DARK_LAYOUT,
    margin: { t: 25, r: 40, b: 140, l: 160 },
    xaxis: {
      tickangle: -40,
      tickfont: { color: '#8c96a8', size: 10 },
      gridcolor: 'rgba(255, 255, 255, 0.03)'
    },
    yaxis: {
      autorange: 'reversed',
      tickfont: { color: '#8c96a8', size: 10 },
      gridcolor: 'rgba(255, 255, 255, 0.03)'
    }
  }, PLOTLY_CONFIG);
}

/* ══════════════════════════════════════════════════════════════════════
   VISUALIZATION 3: Volumetric Profiling — Log-Log Scatter Plot
   ══════════════════════════════════════════════════════════════════════ */
async function renderVolumetricScatter() {
  const records = await fetchJSON('/api/scatter-sample');
  if (!records || records.length === 0) return;

  const COLOR_MAP = {
    BENIGN:       '#10b981',
    DDoS:         '#f59e0b',
    PortScan:     '#06b6d4',
    Bot:          '#ec4899',
    Infiltration: '#a855f7'
  };

  /* Group points by attack label */
  const groups = {};
  records.forEach(row => {
    const lbl = row.label || 'Unknown';
    if (!groups[lbl]) groups[lbl] = [];
    groups[lbl].push(row);
  });

  const traces = [];

  /* Benign flows (baseline background points) */
  if (groups['BENIGN']) {
    const rows = groups['BENIGN'];
    traces.push({
      name: 'BENIGN',
      x: rows.map(r => Math.max(1, r.flow_duration || 0)),
      y: rows.map(r => Math.max(0.01, r.flow_packets_s || 0.01)),
      mode: 'markers',
      type: 'scatter',
      marker: { color: COLOR_MAP.BENIGN, size: 5, opacity: 0.38 },
      text: rows.map(r => `Port: ${r.destination_port ?? 'N/A'}<br>Mean Pkt Len: ${r.packet_length_mean?.toFixed(1) ?? 'N/A'}`),
      hovertemplate: '<b>BENIGN</b><br>Duration: %{x:,} µs<br>Rate: %{y:,.2f} pkt/s<br>%{text}<extra></extra>'
    });
  }

  /* Malicious attack traces */
  for (const [lbl, rows] of Object.entries(groups)) {
    if (lbl.toUpperCase() === 'BENIGN') continue;
    const isDDoS = lbl.toLowerCase().includes('ddos');
    const color = isDDoS ? COLOR_MAP.DDoS : (COLOR_MAP[lbl] || '#818cf8');
    traces.push({
      name: lbl,
      x: rows.map(r => Math.max(1, r.flow_duration || 0)),
      y: rows.map(r => Math.max(0.01, r.flow_packets_s || 0.01)),
      mode: 'markers',
      type: 'scatter',
      marker: {
        color,
        size:    isDDoS ? 7 : 6,
        opacity: isDDoS ? 0.88 : 0.72,
        line:    isDDoS ? { color: '#ffffff', width: 0.7 } : undefined
      },
      text: rows.map(r => `Port: ${r.destination_port ?? 'N/A'}<br>Mean Pkt Len: ${r.packet_length_mean?.toFixed(1) ?? 'N/A'}`),
      hovertemplate: `<b>${lbl}</b><br>Duration: %{x:,} µs<br>Rate: %{y:,.2f} pkt/s<br>%{text}<extra></extra>`
    });
  }

  Plotly.newPlot('chartVolumetricScatter', traces, {
    ...DARK_LAYOUT,
    margin: { t: 30, r: 30, b: 60, l: 70 },
    xaxis: {
      title: { text: 'Flow Duration (µs, log scale)', font: { size: 11, color: '#8c96a8' } },
      type: 'log',
      gridcolor: GRID_COLOR,
      zerolinecolor: ZERO_COLOR,
      tickfont: { color: '#8c96a8', size: 10 }
    },
    yaxis: {
      title: { text: 'Packet Rate (pkt/s, log scale)', font: { size: 11, color: '#8c96a8' } },
      type: 'log',
      gridcolor: GRID_COLOR,
      zerolinecolor: ZERO_COLOR,
      tickfont: { color: '#8c96a8', size: 10 }
    },
    legend: {
      orientation: 'h',
      x: 0,
      y: 1.12,
      font: { color: '#cbd5e1', size: 11 }
    }
  }, PLOTLY_CONFIG);
}

/* ── Responsive Reflow on Resize ────────────────────────────────────────── */
const CHART_CONTAINERS = [
  'chartBinaryDist',
  'chartAttackBreakdown',
  'chartCorrelationHeatmap',
  'chartVolumetricScatter'
];

window.addEventListener('resize', () => {
  CHART_CONTAINERS.forEach(id => {
    const node = document.getElementById(id);
    if (node && node.data) Plotly.Plots.resize(node);
  });
});

/* ── Boot ───────────────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', initDashboard);
