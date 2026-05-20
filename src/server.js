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

// ═════════════════════════════════════════════════════════════════════════════
// MÓDULO KWAI TRACKER
// ═════════════════════════════════════════════════════════════════════════════

const KWAI_PIXELS = ['304883249383318', '308803017463304'];
const KWAI_WPP_NUMBER = '5562981585394';
const KWAI_WPP_TEXT   = encodeURIComponent('Olá! Gostaria de saber mais sobre o tratamento.');

// Inicializa tabela kwai_leads
async function initKwaiDB() {
  const { Pool } = require('pg');
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
  await pool.query(`
    CREATE TABLE IF NOT EXISTS kwai_leads (
      id             SERIAL PRIMARY KEY,
      phone          VARCHAR(30),
      click_id       VARCHAR(255),
      campaign_id    VARCHAR(100),
      status         VARCHAR(20)   DEFAULT 'lead',
      created_at     TIMESTAMP     DEFAULT NOW(),
      purchased_at   TIMESTAMP,
      purchase_value DECIMAL(10,2),
      kwai_results   JSONB
    );
    CREATE INDEX IF NOT EXISTS idx_kwai_phone    ON kwai_leads(phone);
    CREATE INDEX IF NOT EXISTS idx_kwai_click_id ON kwai_leads(click_id);
    CREATE INDEX IF NOT EXISTS idx_kwai_status   ON kwai_leads(status);
  `);
  await pool.end();
  console.log('[Kwai] Tabela kwai_leads pronta');
}

// Disparo server-side para um pixel
async function kwaiDispararEvento(eventName, clickId, pixelId, extra) {
  const eventId  = uuidv4();
  const userData = {};
  if (clickId)      userData.click_id = clickId;
  if (extra && extra.phone) userData.ph = extra.phone.replace(/\D/g, '');

  const payload = {
    pixel_id: pixelId,
    events: [{
      event_name:  eventName,
      event_id:    eventId,
      event_time:  Math.floor(Date.now() / 1000),
      user_data:   userData,
      custom_data: {
        value:        (extra && extra.value)    || 297,
        currency:     (extra && extra.currency) || 'BRL',
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

// Pool dedicado para queries do Kwai
function kwaiPool() {
  const { Pool } = require('pg');
  return new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
}

// ── Redirect do botão WhatsApp — captura lead ─────────────────────────────────
app.get('/kwai/ir', async function(req, res) {
  const cid         = req.query.cid || null;
  const campaign_id = req.query.campaign_id || null;

  if (cid) {
    const pool = kwaiPool();
    try {
      await pool.query(
        'INSERT INTO kwai_leads (click_id, campaign_id, status) VALUES ($1, $2, $3)',
        [cid, campaign_id, 'lead']
      );
      console.log('[Kwai Lead] click_id=' + cid);
    } catch(err) {
      console.error('[Kwai Lead] Erro:', err.message);
    } finally {
      await pool.end();
    }
  }

  res.redirect(302,
    'https://api.whatsapp.com/send/?phone=' + KWAI_WPP_NUMBER +
    '&text=' + KWAI_WPP_TEXT + '&type=phone_number&app_absent=0'
  );
});

// ── Buscar lead por número ────────────────────────────────────────────────────
app.get('/api/kwai/lead/:phone', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const phone = req.params.phone.replace(/\D/g, '');
    const result = await pool.query(
      'SELECT * FROM kwai_leads WHERE phone LIKE $1 OR click_id LIKE $1 ORDER BY created_at DESC LIMIT 10',
      ['%' + phone + '%']
    );
    res.json({ leads: result.rows });
  } catch(err) {
    res.status(500).json({ error: err.message });
  } finally {
    await pool.end();
  }
});

// ── Atualizar número do lead ──────────────────────────────────────────────────
app.post('/api/kwai/lead/:id/phone', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    await pool.query(
      'UPDATE kwai_leads SET phone = $1 WHERE id = $2',
      [req.body.phone.replace(/\D/g, ''), req.params.id]
    );
    res.json({ ok: true });
  } catch(err) {
    res.status(500).json({ error: err.message });
  } finally {
    await pool.end();
  }
});

// ── Disparar Purchase ─────────────────────────────────────────────────────────
app.post('/api/kwai/purchase', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const { lead_id, phone, value } = req.body;
    let lead;

    if (lead_id) {
      const r = await pool.query('SELECT * FROM kwai_leads WHERE id = $1', [lead_id]);
      lead = r.rows[0];
    } else if (phone) {
      const clean = phone.replace(/\D/g, '');
      const r = await pool.query(
        'SELECT * FROM kwai_leads WHERE phone LIKE $1 ORDER BY created_at DESC LIMIT 1',
        ['%' + clean + '%']
      );
      lead = r.rows[0];
    }

    if (!lead) return res.status(404).json({ error: 'Lead não encontrado' });
    if (lead.status === 'purchased') return res.status(400).json({ error: 'Purchase já disparado para este lead' });

    const purchaseValue = parseFloat(value) || 297;
    const results = await kwaiDispararTodos('PURCHASE', lead.click_id, {
      phone: lead.phone,
      value: purchaseValue
    });

    await pool.query(
      'UPDATE kwai_leads SET status = $1, purchased_at = NOW(), purchase_value = $2, kwai_results = $3 WHERE id = $4',
      ['purchased', purchaseValue, JSON.stringify(results), lead.id]
    );

    res.json({ success: true, lead: lead, results: results });
  } catch(err) {
    res.status(500).json({ error: err.message });
  } finally {
    await pool.end();
  }
});

// ── Listar leads ──────────────────────────────────────────────────────────────
app.get('/api/kwai/leads', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const status = req.query.status;
    const limit  = parseInt(req.query.limit) || 50;
    const page   = parseInt(req.query.page)  || 1;
    const offset = (page - 1) * limit;

    let query  = 'SELECT * FROM kwai_leads';
    const params = [];
    if (status) { query += ' WHERE status = $1'; params.push(status); }
    query += ' ORDER BY created_at DESC LIMIT ' + limit + ' OFFSET ' + offset;

    const rows  = await pool.query(query, params);
    const count = await pool.query(
      status ? 'SELECT COUNT(*) FROM kwai_leads WHERE status = $1' : 'SELECT COUNT(*) FROM kwai_leads',
      status ? [status] : []
    );
    res.json({ leads: rows.rows, total: parseInt(count.rows[0].count), page: page });
  } catch(err) {
    res.status(500).json({ error: err.message });
  } finally {
    await pool.end();
  }
});

// ── Stats do Kwai ─────────────────────────────────────────────────────────────
app.get('/api/kwai/stats', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const total     = await pool.query('SELECT COUNT(*) FROM kwai_leads');
    const purchases = await pool.query("SELECT COUNT(*), COALESCE(SUM(purchase_value),0) FROM kwai_leads WHERE status = 'purchased'");
    const hoje      = await pool.query("SELECT COUNT(*) FROM kwai_leads WHERE created_at >= CURRENT_DATE");
    const vendasHj  = await pool.query("SELECT COUNT(*) FROM kwai_leads WHERE status = 'purchased' AND purchased_at >= CURRENT_DATE");

    res.json({
      total_leads:     parseInt(total.rows[0].count),
      total_purchases: parseInt(purchases.rows[0].count),
      total_revenue:   parseFloat(purchases.rows[0].coalesce),
      leads_hoje:      parseInt(hoje.rows[0].count),
      vendas_hoje:     parseInt(vendasHj.rows[0].count),
      taxa_conversao:  total.rows[0].count > 0
        ? ((purchases.rows[0].count / total.rows[0].count) * 100).toFixed(1) + '%'
        : '0%'
    });
  } catch(err) {
    res.status(500).json({ error: err.message });
  } finally {
    await pool.end();
  }
});

// ── Inicializa banco do Kwai ao subir o servidor ──────────────────────────────
initKwaiDB().catch(console.error);

// ═════════════════════════════════════════════════════════════════════════════
// CATCH-ALL — deve ficar SEMPRE no final
// ═════════════════════════════════════════════════════════════════════════════
app.get('*', function(req, res) {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

app.listen(PORT, function() {
  console.log('Servidor rodando na porta ' + PORT);
});
