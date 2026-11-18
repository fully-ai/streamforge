(() => {
  const api = async (path, opts = {}) => {
    const r = await fetch(path, opts);
    return r.json();
  };
  const renderTable = (elId, cols, rows) => {
    const host = document.getElementById(elId);
    host.textContent = '';
    const t = document.createElement('table');
    const tr = document.createElement('tr');
    cols.forEach((c) => { const th = document.createElement('th'); th.textContent = c; tr.appendChild(th); });
    t.appendChild(tr);
    rows.forEach((row) => {
      const r = document.createElement('tr');
      row.forEach((cell) => { const td = document.createElement('td'); td.textContent = String(cell); r.appendChild(td); });
      t.appendChild(r);
    });
    host.appendChild(t);
  };
  const load = async () => {
    const [tRes, gRes, sRes] = await Promise.all([api('/api/v1/topics'), api('/api/v1/groups'), api('/api/v1/schemas')]);
    const m = tRes.metrics || {};
    const kpiHost = document.getElementById('kpis');
    kpiHost.textContent = '';
    [['Topics', m.topics || 0], ['Consumer Groups', m.groups || 0], ['Produced Records', m.produced_total || 0], ['Consumed Records', m.consumed_total || 0], ['DLQ Routed', m.dlq_routed_total || 0]].forEach(([l, v]) => {
      const c = document.createElement('div'); c.className = 'card';
      const d1 = document.createElement('div'); d1.textContent = l; d1.style.color = '#94a3b8';
      const d2 = document.createElement('div'); d2.className = 'kpi'; d2.textContent = String(v);
      c.append(d1, d2); kpiHost.appendChild(c);
    });
    renderTable('topics-tbl', ['Topic', 'Partitions', 'LEO per Partition', 'Segments'], (tRes.topics || []).map((t) => [
      t.config.name, t.config.partitions, JSON.stringify(t.partition_leo), JSON.stringify(t.partition_segments)
    ]));
    renderTable('groups-tbl', ['Group', 'Topic', 'Committed Offsets', 'Lag'], (gRes.groups || []).map((g) => [
      g.group_id, g.topic, JSON.stringify(g.offsets), JSON.stringify((gRes.lags || {})[g.group_id] || {})
    ]));
    document.getElementById('schemas-pre').textContent = JSON.stringify(sRes.schemas || {}, null, 2);
  };
  document.getElementById('create-topic').addEventListener('click', async () => {
    const name = document.getElementById('tname').value.trim();
    const parts = parseInt(document.getElementById('tparts').value || '3', 10);
    await api('/api/v1/topics', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, partitions: parts }) });
    await load();
  });
  document.getElementById('produce').addEventListener('click', async () => {
    const topic = document.getElementById('tname').value.trim();
    await api(`/api/v1/topics/${encodeURIComponent(topic)}/produce`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key: document.getElementById('pkey').value, value: document.getElementById('pval').value }),
    });
    await load();
  });
  document.getElementById('refresh').addEventListener('click', load);
  load();
})();
