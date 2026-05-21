const express = require('express');
const cors = require('cors');
const path = require('path');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');
const fetch = require('node-fetch');
const db = require('./database');
const metaApi = require('./meta');

const app = express();
const PORT = process.env.PORT || 3000;
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

// ─── AUTH ─────────────────────────────────────────────────────────────────────
async function auth(req, res, next) {
  try {
    const token = req.headers['x-auth-token'];
    const cfg = await db.getConfig();
    if (!cfg || !cfg.auth_token) return res.status(401).json({ error: 'Nao autenticado' });
    if (token !== cfg.auth_token) return res.status(401).json({ error: 'Token invalido' });
    next();
  } catch(e) { res.status(500).json({ error: e.message }); }
}

// ─── LOGIN / CONFIG ───────────────────────────────────────────────────────────
app.post('/api/login', async function(req, res) {
  try {
    const password = req.body.password;
    const cfg = await db.getConfig();
    if (!cfg) {
      const token = uuidv4();
      const webhook = uuidv4().replace(/-/g, '');
      await db.saveConfig({ password: password, auth_token: token, webhook_token: webhook });
      return res.json({ token: token });
    }
    if (password !== cfg.password) return res.status(401).json({ error: 'Senha incorreta' });
    res.json({ token: cfg.auth_token });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/reset', async function(req, res) {
  try {
    const secret = req.body.secret;
    if (secret !== 'infinity2026reset') return res.status(403).json({ error: 'Nao autorizado' });
    const { Pool } = require('pg');
    const pool2 = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
    await pool2.query('DELETE FROM config WHERE id = 1');
    await pool2.end();
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/change-password', auth, async function(req, res) {
  try {
    const new_password = req.body.new_password;
    if (!new_password || new_password.length < 4) return res.status(400).json({ error: 'Senha muito curta' });
    await db.saveConfig({ password: new_password });
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── PIXELS (META) ────────────────────────────────────────────────────────────
app.get('/api/pixels', auth, async function(req, res) {
  try {
    const pixels = await db.getPixels();
    res.json(pixels.map(function(p) { return { id: p.id, name: p.name, pixel_id: p.pixel_id, created_at: p.created_at }; }));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/pixels', auth, async function(req, res) {
  try {
    const id = req.body.id, name = req.body.name, pixel_id = req.body.pixel_id, access_token = req.body.access_token;
    if (!name || !pixel_id || !access_token) return res.status(400).json({ error: 'Todos os campos sao obrigatorios' });
    const pixels = await db.savePixel({ id, name, pixel_id, access_token });
    res.json({ ok: true, pixels: pixels.map(function(p) { return { id: p.id, name: p.name, pixel_id: p.pixel_id }; }) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/pixels/:id', auth, async function(req, res) {
  try {
    await db.deletePixel(req.params.id);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── WEBHOOK (META) ───────────────────────────────────────────────────────────
app.get('/api/webhook-url', auth, async function(req, res) {
  try {
    const cfg = await db.getConfig();
    const base = process.env.BASE_URL || ('http://localhost:' + PORT);
    res.json({ url: base + '/webhook/' + cfg.webhook_token });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/webhook/:token', async function(req, res) {
  try {
    const cfg = await db.getConfig();
    if (!cfg || req.params.token !== cfg.webhook_token) return res.status(403).json({ error: 'Webhook invalido' });
    const { name, phone, email, value, gender, cep, pixel_id } = req.body;
    if (!phone || !value) return res.status(400).json({ error: 'phone e value sao obrigatorios' });
    let pixelCfg = pixel_id ? await db.getPixelById(pixel_id) : (await db.getPixels())[0];
    if (!pixelCfg) return res.status(400).json({ error: 'Nenhum pixel configurado' });
    const result = await metaApi.sendPurchase(pixelCfg, { name, phone, email, value, gender, cep });
    await db.insertEvent({ pixel_id: pixelCfg.id, pixel_name: pixelCfg.name, name, phone, email, value, gender, cep, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'webhook' });
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── ENVIO MANUAL / BULK (META) ───────────────────────────────────────────────
app.post('/api/send', auth, async function(req, res) {
  try {
    const { name, phone, email, value, gender, cep, pixel_id } = req.body;
    if (!phone || !value) return res.status(400).json({ error: 'Telefone e valor sao obrigatorios' });
    if (!pixel_id) return res.status(400).json({ error: 'Selecione um pixel' });
    const pixelCfg = await db.getPixelById(pixel_id);
    if (!pixelCfg) return res.status(400).json({ error: 'Pixel nao encontrado' });
    const result = await metaApi.sendPurchase(pixelCfg, { name, phone, email, value, gender, cep });
    await db.insertEvent({ pixel_id: pixelCfg.id, pixel_name: pixelCfg.name, name, phone, email, value, gender, cep, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'manual' });
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/preview-bulk', auth, upload.single('file'), async function(req, res) {
  try {
    if (!req.file) return res.status(400).json({ error: 'Nenhum arquivo enviado' });
    const ext = req.file.originalname.split('.').pop().toLowerCase();
    let rows = [];
    if (ext === 'csv') {
      const csvParse = require('csv-parse/sync');
      rows = csvParse.parse(req.file.buffer.toString('utf8'), { columns: true, skip_empty_lines: true, trim: true });
    } else if (ext === 'xlsx' || ext === 'xls') {
      const XLSX = require('xlsx');
      const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
      rows = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);
    } else { return res.status(400).json({ error: 'Use CSV ou XLSX.' }); }
    const normalize = function(row) {
      const k = {};
      Object.keys(row).forEach(function(key) { k[key.toLowerCase().trim()] = row[key]; });
      return { name: k.nome||k.name||'', phone: k.telefone||k.phone||k.fone||'', email: k.email||'', value: parseFloat(k.valor||k.value||0)||0, gender: k.genero||k.gender||k.sexo||'', cep: k.cep||k.zip||'' };
    };
    res.json({ rows: rows.map(normalize), total: rows.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/send-bulk', auth, upload.single('file'), async function(req, res) {
  try {
    const pixel_id = req.body.pixel_id;
    if (!pixel_id) return res.status(400).json({ error: 'Selecione um pixel' });
    const pixelCfg = await db.getPixelById(pixel_id);
    if (!pixelCfg) return res.status(400).json({ error: 'Pixel nao encontrado' });
    if (!req.file) return res.status(400).json({ error: 'Nenhum arquivo enviado' });
    const ext = req.file.originalname.split('.').pop().toLowerCase();
    let rows = [];
    if (ext === 'csv') {
      const csvParse = require('csv-parse/sync');
      rows = csvParse.parse(req.file.buffer.toString('utf8'), { columns: true, skip_empty_lines: true, trim: true });
    } else if (ext === 'xlsx' || ext === 'xls') {
      const XLSX = require('xlsx');
      const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
      rows = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);
    } else { return res.status(400).json({ error: 'Formato invalido.' }); }
    const normalize = function(row) {
      const k = {};
      Object.keys(row).forEach(function(key) { k[key.toLowerCase().trim()] = row[key]; });
      return { name: k.nome||k.name||'', phone: k.telefone||k.phone||k.fone||'', email: k.email||'', value: parseFloat(k.valor||k.value||0)||0, gender: k.genero||k.gender||k.sexo||'', cep: k.cep||k.zip||'' };
    };
    const results = [];
    for (let i = 0; i < rows.length; i++) {
      const lead = normalize(rows[i]);
      if (!lead.phone || !lead.value) { results.push({ success: false, error: 'Telefone ou valor ausente' }); continue; }
      const result = await metaApi.sendPurchase(pixelCfg, lead);
      await db.insertEvent({ pixel_id: pixelCfg.id, pixel_name: pixelCfg.name, name: lead.name, phone: lead.phone, email: lead.email, value: lead.value, gender: lead.gender, cep: lead.cep, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'bulk' });
      results.push({ success: result.success });
      await new Promise(function(r) { setTimeout(r, 200); });
    }
    res.json({ total: results.length, sent: results.filter(function(r) { return r.success; }).length, errors: results.filter(function(r) { return !r.success; }).length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── EVENTS / STATS (META) ────────────────────────────────────────────────────
app.get('/api/events', auth, async function(req, res) {
  try {
    res.json(await db.getEvents({ page: parseInt(req.query.page || 1), status: req.query.status, pixel_id: req.query.pixel_id }));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/stats', auth, async function(req, res) {
  try {
    res.json(await db.getStats(req.query.pixel_id));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// =============================================================================
// MÓDULO KWAI TRACKER
// =============================================================================

const KWAI_PIXELS    = ['304883249383318', '308803017463304'];
const KWAI_WPP_TEXT  = 'Olá! Gostaria de saber mais sobre o tratamento.';
const DEFAULT_VALUE  = 497;
const MAX_NUMBERS    = 20;

// Gera Lead ID curto e único — ex: A3K9F2
function gerarLeadId() {
  var chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  var id = '';
  for (var i = 0; i < 6; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }
  return id;
}

function kwaiPool() {
  const { Pool } = require('pg');
  return new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
}

async function initKwaiDB() {
  const pool = kwaiPool();
  // Cria tabela base se nao existir
  await pool.query(`
    CREATE TABLE IF NOT EXISTS kwai_leads (
      id         SERIAL PRIMARY KEY,
      phone      VARCHAR(30),
      status     VARCHAR(20) DEFAULT 'visitou',
      created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS kwai_numbers (
      id         SERIAL PRIMARY KEY,
      label      VARCHAR(100),
      number     VARCHAR(30) NOT NULL,
      active     BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Migrations — adiciona colunas novas com seguranca
  var migrations = [
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS lead_id VARCHAR(20)",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS kwai_click_id VARCHAR(255)",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS utm_source VARCHAR(100)",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS utm_campaign VARCHAR(100)",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS utm_adset VARCHAR(100)",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS utm_ad VARCHAR(100)",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS wpp_number VARCHAR(30)",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS user_agent TEXT",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS ip VARCHAR(50)",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS session_id VARCHAR(100)",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS clicks INT DEFAULT 0",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS clicked_at TIMESTAMP",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS purchased_at TIMESTAMP",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS purchase_value DECIMAL(10,2)",
    "ALTER TABLE kwai_leads ADD COLUMN IF NOT EXISTS kwai_results JSONB",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_kwai_lead_id ON kwai_leads(lead_id)",
    "CREATE INDEX IF NOT EXISTS idx_kwai_phone ON kwai_leads(phone)",
    "CREATE INDEX IF NOT EXISTS idx_kwai_click_id ON kwai_leads(kwai_click_id)",
    "CREATE INDEX IF NOT EXISTS idx_kwai_status ON kwai_leads(status)",
    "CREATE INDEX IF NOT EXISTS idx_kwai_created ON kwai_leads(created_at DESC)"
  ];
  for (var i = 0; i < migrations.length; i++) {
    try { await pool.query(migrations[i]); } catch(e) {}
  }
  await pool.end();
  console.log('[Kwai] Tabelas prontas');
}

async function kwaiDispararEvento(eventName, clickId, pixelId, extra) {
  const eventId  = uuidv4();
  const userData = {};
  if (clickId)              userData.click_id = clickId;
  if (extra && extra.phone) userData.ph = extra.phone.replace(/\D/g, '');
  const payload = {
    pixel_id: pixelId,
    events: [{
      event_name:  eventName,
      event_id:    eventId,
      event_time:  Math.floor(Date.now() / 1000),
      user_data:   userData,
      custom_data: {
        value:        (extra && extra.value)    || DEFAULT_VALUE,
        currency:     'BRL',
        content_name: 'Glivia',
        content_id:   'glivia'
      }
    }]
  };
  try {
    const res = await fetch(
      'https://s21-def.ap4r.com/rest/n/v1/pixel/batch?sdkid=' + pixelId,
      { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }
    );
    const body = await res.text();
    console.log('[Kwai ' + pixelId + '] ' + eventName + ' → ' + res.status + ': ' + body);
    return { ok: res.ok, status: res.status, body: body, event_id: eventId };
  } catch(err) {
    console.error('[Kwai ' + pixelId + '] Erro:', err.message);
    return { ok: false, error: err.message, event_id: eventId };
  }
}

async function kwaiDispararTodos(eventName, clickId, extra) {
  return Promise.all(KWAI_PIXELS.map(function(pid) {
    return kwaiDispararEvento(eventName, clickId, pid, extra);
  }));
}

// ── Gerar Lead ID — chamado pela pretzel ao carregar ─────────────────────────
app.post('/kwai/lead/criar', async function(req, res) {
  const pool = kwaiPool();
  try {
    const {
      kwai_click_id, utm_source, utm_campaign, utm_adset, utm_ad, session_id
    } = req.body;

    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress || '';
    const user_agent = req.headers['user-agent'] || '';

    // Gera Lead ID único com retry em caso de colisão
    let lead_id, attempts = 0;
    while (attempts < 10) {
      lead_id = gerarLeadId();
      const exists = await pool.query('SELECT id FROM kwai_leads WHERE lead_id = $1', [lead_id]);
      if (!exists.rows.length) break;
      attempts++;
    }

    const result = await pool.query(
      `INSERT INTO kwai_leads 
        (lead_id, kwai_click_id, utm_source, utm_campaign, utm_adset, utm_ad, ip, user_agent, session_id, status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'visitou') RETURNING id, lead_id`,
      [lead_id, kwai_click_id||null, utm_source||null, utm_campaign||null, utm_adset||null, utm_ad||null, ip, user_agent, session_id||null]
    );

    console.log('[Kwai] Lead criado: ' + lead_id + ' click_id=' + kwai_click_id);
    res.json({ ok: true, lead_id: lead_id });
  } catch(err) {
    console.error('[Kwai] Erro ao criar lead:', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    await pool.end();
  }
});

// ── Redirect do botão WhatsApp ────────────────────────────────────────────────
app.get('/kwai/ir', async function(req, res) {
  const lead_id     = req.query.lid         || null;
  const phonesParam = req.query.phones      || null;
  const phoneParam  = req.query.phone       || null;
  const campaign_id = req.query.campaign_id || null;

  // Determina número — randomiza se múltiplos
  let number = null;
  if (phonesParam) {
    const list = phonesParam.split(',').map(function(n) { return n.trim().replace(/\D/g,''); }).filter(Boolean);
    if (list.length) number = list[Math.floor(Math.random() * list.length)];
  } else if (phoneParam) {
    number = phoneParam.replace(/\D/g,'');
  } else {
    const pool2 = kwaiPool();
    try {
      const r = await pool2.query("SELECT number FROM kwai_numbers WHERE active = TRUE ORDER BY id LIMIT 1");
      if (r.rows.length) number = r.rows[0].number;
    } catch(e) {} finally { await pool2.end(); }
  }

  if (!number) return res.status(400).send('Nenhum número configurado.');

  // Atualiza o lead com o clique e número de destino
  if (lead_id) {
    const pool = kwaiPool();
    try {
      await pool.query(
        `UPDATE kwai_leads SET 
          status = 'clicou', 
          wpp_number = $1, 
          clicked_at = NOW(),
          clicks = COALESCE(clicks, 0) + 1
         WHERE lead_id = $2`,
        [number, lead_id]
      );
      console.log('[Kwai] Clique registrado: lid=' + lead_id + ' wpp=' + number);
    } catch(err) {
      console.error('[Kwai] Erro ao registrar clique:', err.message);
    } finally {
      await pool.end();
    }
  }

  // Monta mensagem com o Lead ID
  var msg = KWAI_WPP_TEXT + (lead_id ? ' REF:' + lead_id : '');
  res.redirect(302,
    'https://api.whatsapp.com/send/?phone=' + number +
    '&text=' + encodeURIComponent(msg) + '&type=phone_number&app_absent=0'
  );
});

// ── CRUD Números ──────────────────────────────────────────────────────────────
app.get('/api/kwai/numbers', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const r = await pool.query('SELECT * FROM kwai_numbers ORDER BY id');
    res.json({ numbers: r.rows });
  } catch(err) { res.status(500).json({ error: err.message }); }
  finally { await pool.end(); }
});

app.post('/api/kwai/numbers', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const { label, number } = req.body;
    if (!number) return res.status(400).json({ error: 'Número obrigatório' });
    const clean = number.replace(/\D/g,'');
    const count = await pool.query('SELECT COUNT(*) FROM kwai_numbers');
    if (parseInt(count.rows[0].count) >= MAX_NUMBERS) return res.status(400).json({ error: 'Limite de ' + MAX_NUMBERS + ' números atingido' });
    const r = await pool.query('INSERT INTO kwai_numbers (label, number) VALUES ($1,$2) RETURNING *', [label||clean, clean]);
    res.json({ ok: true, number: r.rows[0] });
  } catch(err) { res.status(500).json({ error: err.message }); }
  finally { await pool.end(); }
});

app.delete('/api/kwai/numbers/:id', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    await pool.query('DELETE FROM kwai_numbers WHERE id = $1', [req.params.id]);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
  finally { await pool.end(); }
});

// ── Buscar lead por REF ou telefone ──────────────────────────────────────────
app.get('/api/kwai/lead/:query', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const q = req.params.query.replace(/\s/g,'').toUpperCase();
    const result = await pool.query(
      `SELECT * FROM kwai_leads 
       WHERE lead_id = $1 OR phone LIKE $2 OR lead_id LIKE $2
       ORDER BY created_at DESC LIMIT 10`,
      [q, '%' + q + '%']
    );
    res.json({ leads: result.rows });
  } catch(err) { res.status(500).json({ error: err.message }); }
  finally { await pool.end(); }
});

// ── Associar telefone do lead ─────────────────────────────────────────────────
app.post('/api/kwai/lead/:lead_id/phone', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    await pool.query('UPDATE kwai_leads SET phone = $1 WHERE lead_id = $2', [req.body.phone.replace(/\D/g,''), req.params.lead_id]);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
  finally { await pool.end(); }
});

// ── Disparar Purchase ─────────────────────────────────────────────────────────
app.post('/api/kwai/purchase', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const { lead_id, ref, phone, value } = req.body;
    let lead;

    const searchId = (ref || lead_id || '').toUpperCase().replace('REF:','').trim();
    if (searchId) {
      const r = await pool.query('SELECT * FROM kwai_leads WHERE lead_id = $1', [searchId]);
      lead = r.rows[0];
    }
    if (!lead && phone) {
      const clean = phone.replace(/\D/g,'');
      const r = await pool.query('SELECT * FROM kwai_leads WHERE phone LIKE $1 ORDER BY created_at DESC LIMIT 1', ['%' + clean + '%']);
      lead = r.rows[0];
    }

    if (!lead) return res.status(404).json({ error: 'Lead não encontrado. Verifique o REF ou telefone.' });
    if (lead.status === 'purchased') return res.status(400).json({ error: 'Purchase já disparado para este lead' });

    const purchaseValue = parseFloat(value) || DEFAULT_VALUE;
    const results = await kwaiDispararTodos('PURCHASE', lead.kwai_click_id, { phone: lead.phone, value: purchaseValue });

    await pool.query(
      'UPDATE kwai_leads SET status=$1, purchased_at=NOW(), purchase_value=$2, kwai_results=$3 WHERE id=$4',
      ['purchased', purchaseValue, JSON.stringify(results), lead.id]
    );

    res.json({ success: true, lead: lead, results: results });
  } catch(err) { res.status(500).json({ error: err.message }); }
  finally { await pool.end(); }
});

// ── Listar leads ──────────────────────────────────────────────────────────────
app.get('/api/kwai/leads', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const status = req.query.status;
    const limit  = parseInt(req.query.limit) || 50;
    const page   = parseInt(req.query.page)  || 1;
    const offset = (page - 1) * limit;
    let query = 'SELECT * FROM kwai_leads';
    const params = [];
    if (status) { query += ' WHERE status = $1'; params.push(status); }
    query += ' ORDER BY created_at DESC LIMIT ' + limit + ' OFFSET ' + offset;
    const rows  = await pool.query(query, params);
    const count = await pool.query(
      status ? 'SELECT COUNT(*) FROM kwai_leads WHERE status=$1' : 'SELECT COUNT(*) FROM kwai_leads',
      status ? [status] : []
    );
    res.json({ leads: rows.rows, total: parseInt(count.rows[0].count), page: page });
  } catch(err) { res.status(500).json({ error: err.message }); }
  finally { await pool.end(); }
});

// ── Stats ─────────────────────────────────────────────────────────────────────
app.get('/api/kwai/stats', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const total     = await pool.query('SELECT COUNT(*) FROM kwai_leads');
    const clicou    = await pool.query("SELECT COUNT(*) FROM kwai_leads WHERE status IN ('clicou','purchased')");
    const purchases = await pool.query("SELECT COUNT(*), COALESCE(SUM(purchase_value),0) FROM kwai_leads WHERE status='purchased'");
    const hoje      = await pool.query("SELECT COUNT(*) FROM kwai_leads WHERE created_at >= CURRENT_DATE");
    const vendasHj  = await pool.query("SELECT COUNT(*) FROM kwai_leads WHERE status='purchased' AND purchased_at >= CURRENT_DATE");
    res.json({
      total_leads:     parseInt(total.rows[0].count),
      total_cliques:   parseInt(clicou.rows[0].count),
      total_purchases: parseInt(purchases.rows[0].count),
      total_revenue:   parseFloat(purchases.rows[0].coalesce),
      leads_hoje:      parseInt(hoje.rows[0].count),
      vendas_hoje:     parseInt(vendasHj.rows[0].count),
      taxa_conversao:  clicou.rows[0].count > 0
        ? ((purchases.rows[0].count / clicou.rows[0].count) * 100).toFixed(1) + '%'
        : '0%'
    });
  } catch(err) { res.status(500).json({ error: err.message }); }
  finally { await pool.end(); }
});

initKwaiDB().catch(console.error);

// =============================================================================
// ROTAS DE PÁGINAS
// =============================================================================
app.get('/kwai', function(req, res) { res.sendFile(path.join(__dirname, '../public/kwai.html')); });
app.get('/kwai.html', function(req, res) { res.sendFile(path.join(__dirname, '../public/kwai.html')); });

// CATCH-ALL — sempre no final
app.get('*', function(req, res) { res.sendFile(path.join(__dirname, '../public/index.html')); });

app.listen(PORT, function() { console.log('Servidor rodando na porta ' + PORT); });
