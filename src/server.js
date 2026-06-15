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
    res.json(pixels.map(function(p) { return { id: p.id, name: p.name, pixel_id: p.pixel_id, page_id: p.page_id || '', created_at: p.created_at }; }));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/pixels', auth, async function(req, res) {
  try {
    const id = req.body.id, name = req.body.name, pixel_id = req.body.pixel_id, access_token = req.body.access_token, page_id = req.body.page_id || '';
    if (!name || !pixel_id || !access_token) return res.status(400).json({ error: 'Todos os campos sao obrigatorios' });
    const pixels = await db.savePixel({ id, name, pixel_id, access_token, page_id });
    res.json({ ok: true, pixels: pixels.map(function(p) { return { id: p.id, name: p.name, pixel_id: p.pixel_id, page_id: p.page_id || '' }; }) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/pixels/:id', auth, async function(req, res) {
  try {
    await db.deletePixel(req.params.id);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// =============================================================================
// WEBHOOK SPY — deve vir ANTES de /webhook/:token para não ser interceptado
// =============================================================================

function spyPool() {
  const { Pool } = require('pg');
  return new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
}

async function initSpyDB() {
  const pool = spyPool();
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS webhook_spy_logs (
        id          SERIAL PRIMARY KEY,
        received_at TIMESTAMP DEFAULT NOW(),
        headers     TEXT,
        body        TEXT,
        ip          VARCHAR(100)
      )
    `);
    console.log('[Spy] Tabela pronta');
  } catch(e) {
    console.error('[Spy] Erro ao criar tabela:', e.message);
  } finally {
    await pool.end();
  }
}

// POST /webhook/spy — captura payload bruto da Five Delivery (para debug)
app.post('/webhook/spy', async function(req, res) {
  const pool = spyPool();
  try {
    const headers = JSON.stringify(req.headers, null, 2);
    const body    = JSON.stringify(req.body,    null, 2);
    const ip      = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
    await pool.query(
      'INSERT INTO webhook_spy_logs (headers, body, ip) VALUES ($1, $2, $3)',
      [headers, body, ip]
    );
    console.log('[Spy] Webhook recebido de ' + ip);
    res.status(200).json({ ok: true });
  } catch(err) {
    console.error('[Spy] Erro:', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    await pool.end();
  }
});

// GET /api/spy/logs
app.get('/api/spy/logs', async function(req, res) {
  const pool = spyPool();
  try {
    const token = req.query.token || req.headers['x-auth-token'];
    const cfg = await db.getConfig();
    if (!cfg || token !== cfg.auth_token) return res.status(401).json({ error: 'Token inválido' });
    const result = await pool.query('SELECT * FROM webhook_spy_logs ORDER BY id DESC LIMIT 30');
    res.json({ total: result.rows.length, logs: result.rows });
  } catch(err) {
    res.status(500).json({ error: err.message });
  } finally {
    await pool.end();
  }
});

// DELETE /api/spy/logs
app.delete('/api/spy/logs', async function(req, res) {
  const pool = spyPool();
  try {
    const token = req.query.token || req.headers['x-auth-token'];
    const cfg = await db.getConfig();
    if (!cfg || token !== cfg.auth_token) return res.status(401).json({ error: 'Token inválido' });
    await pool.query('DELETE FROM webhook_spy_logs');
    res.json({ ok: true });
  } catch(err) {
    res.status(500).json({ error: err.message });
  } finally {
    await pool.end();
  }
});

initSpyDB().catch(console.error);

// =============================================================================
// WEBHOOK FIVE DELIVERY — disparo automático de Purchase na Meta CAPI
// Deve vir ANTES de /webhook/:token
// =============================================================================

// Extrai campos do payload da Five Delivery e normaliza para o formato CAPI
function parseFiveDelivery(body) {
  const c = body.customer || {};
  const addr = c.address || {};
  const offer = (body.product || {}).offer || {};

  // Telefone: remove +55, espaços e caracteres não numéricos
  let phone = String(c.phoneNumber || '').replace(/\D/g, '');
  if (phone.startsWith('55') && phone.length >= 12) phone = phone.slice(2);

  // CEP: apenas dígitos
  const cep = String(addr.zipCode || '').replace(/\D/g, '');

  // Valor: usa product.offer.price (já em reais conforme payload)
  const value = parseFloat(offer.price || 0) || 0;

  // Gênero: não vem no payload, deixa vazio
  return {
    name:   String(c.name  || '').trim(),
    phone:  phone,
    email:  String(c.mail  || '').trim(),
    cep:    cep,
    value:  value,
    gender: ''
  };
}

// POST /webhook/five-delivery/:token
app.post('/webhook/five-delivery/:token', async function(req, res) {
  try {
    const cfg = await db.getConfig();
    if (!cfg || req.params.token !== cfg.webhook_token) {
      return res.status(403).json({ error: 'Token inválido' });
    }

    const body = req.body;

    // Só processa evento de criação de pedido
    const evento = String(body.event || body.eventStatus || '').toUpperCase();
    if (!evento.includes('ORDER_CREATE') && !evento.includes('PEDIDO CRIADO') && !evento.includes('CREATE')) {
      console.log('[FiveDelivery] Evento ignorado:', body.event || body.eventStatus);
      return res.status(200).json({ ok: true, ignored: true, event: body.event || body.eventStatus });
    }

    const lead = parseFiveDelivery(body);

    if (!lead.phone) {
      console.warn('[FiveDelivery] Pedido sem telefone, ignorado. orderId:', body.orderId);
      return res.status(200).json({ ok: true, ignored: true, reason: 'sem telefone' });
    }

    // Dispara para todos os pixels cadastrados simultaneamente
    const pixels = await db.getPixels();
    if (!pixels || !pixels.length) {
      return res.status(400).json({ error: 'Nenhum pixel configurado no Infinity Track' });
    }

    console.log('[FiveDelivery] Disparando Purchase → ' + lead.phone + ' | R$' + lead.value + ' | pixels: ' + pixels.length + ' | orderId: ' + body.orderId);

    const results = await Promise.all(pixels.map(async function(pixelCfg) {
      const result = await metaApi.sendPurchase(pixelCfg, lead);
      await db.insertEvent({
        pixel_id:   pixelCfg.id,
        pixel_name: pixelCfg.name,
        name:       lead.name,
        phone:      lead.phone,
        email:      lead.email,
        value:      lead.value,
        gender:     lead.gender,
        cep:        lead.cep,
        status:     result.success ? 'sent' : 'error',
        error_msg:  result.error || null,
        source:     'five_delivery'
      });
      console.log('[FiveDelivery] Pixel ' + pixelCfg.name + ': ' + (result.success ? '✅ enviado' : '❌ ' + result.error));
      return { pixel: pixelCfg.name, success: result.success };
    }));

    const sent = results.filter(function(r) { return r.success; }).length;
    res.status(200).json({ ok: true, sent: sent, total: pixels.length, phone: lead.phone, value: lead.value, results: results });

  } catch(e) {
    console.error('[FiveDelivery] Erro:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/five-delivery/webhook-url — retorna a URL formatada para colar na Five Delivery
app.get('/api/five-delivery/webhook-url', auth, async function(req, res) {
  try {
    const cfg = await db.getConfig();
    const base = process.env.BASE_URL || ('http://localhost:' + PORT);
    res.json({ url: base + '/webhook/five-delivery/' + cfg.webhook_token });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── WEBHOOK (META) — genérico, deve vir DEPOIS dos webhooks específicos ──────
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

    // Lê de body (JSON) OU de query params (Datacrazy envia como parâmetros)
    const src = (req.body && Object.keys(req.body).length > 0) ? req.body : req.query;
    const name          = src.name       || '';
    const phone         = src.phone      || '';
    const email         = src.email      || '';
    const value         = src.value      || '';
    const gender        = src.gender     || '';
    const cep           = src.cep        || '';
    const city          = src.city       || '';
    const state         = src.state      || '';
    const pixel_id      = src.pixel_id   || '';
    const ctwa_incoming = src.ctwa_clid  || null;

    // Se tem ctwa_clid mas não tem value — apenas salva o ctwa_clid (Datacrazy)
    if (ctwa_incoming && phone && !value) {
      await db.saveCtwaClid(phone, ctwa_incoming);
      console.log('[Webhook] ctwa_clid salvo para ' + phone + ': ' + ctwa_incoming.substring(0, 20) + '...');
      return res.status(200).json({ ok: true, saved: 'ctwa_clid' });
    }

    if (!phone || !value) return res.status(400).json({ error: 'phone e value sao obrigatorios' });
    let pixelCfg = pixel_id ? await db.getPixelById(pixel_id) : (await db.getPixels())[0];
    if (!pixelCfg) return res.status(400).json({ error: 'Nenhum pixel configurado' });

    // Busca ctwa_clid pelo número se não veio no payload
    const ctwa_clid = ctwa_incoming || await db.getCtwaClid(phone);
    console.log('[Webhook] phone=' + phone + ' ctwa=' + (ctwa_clid ? 'SIM' : 'NAO') + ' pixel=' + pixelCfg.name);

    const result = await metaApi.sendPurchase(pixelCfg, { name, phone, email, value, gender, cep, city, state, ctwa_clid: ctwa_clid||null });
    await db.insertEvent({ pixel_id: pixelCfg.id, pixel_name: pixelCfg.name, name, phone, email, value, gender, cep, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'webhook' });
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── ENVIO MANUAL / BULK (META) ───────────────────────────────────────────────
app.post('/api/send', auth, async function(req, res) {
  try {
    const { name, phone, email, value, gender, cep, city, state, pixel_id } = req.body;
    if (!phone || !value) return res.status(400).json({ error: 'Telefone e valor sao obrigatorios' });
    if (!pixel_id) return res.status(400).json({ error: 'Selecione um pixel' });
    const pixelCfg = await db.getPixelById(pixel_id);
    if (!pixelCfg) return res.status(400).json({ error: 'Pixel nao encontrado' });
    // Busca ctwa_clid pelo número do cliente
    const ctwa_clid = await db.getCtwaClid(phone);
    if (ctwa_clid) {
      console.log('[Send] ctwa_clid encontrado para ' + phone + ': ' + ctwa_clid.substring(0, 20) + '...');
    } else {
      console.log('[Send] Nenhum ctwa_clid encontrado para ' + phone);
    }
    const lead = { name, phone, email, value, gender, cep, city: city || '', state: state || '', ctwa_clid: ctwa_clid || null };
    console.log('[Send] Disparando Purchase → pixel: ' + pixelCfg.name + ' | phone: ' + phone + ' | value: ' + value + ' | ctwa: ' + (ctwa_clid ? 'SIM' : 'NAO') + ' | page_id: ' + (pixelCfg.page_id || 'NAO'));
    const result = await metaApi.sendPurchase(pixelCfg, lead);
    console.log('[Send] Resultado: ' + (result.success ? 'SUCESSO fbtrace:' + result.fbtrace_id : 'ERRO: ' + result.error));
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
      return { name: k.nome||k.name||'', phone: k.telefone||k.phone||k.fone||'', email: k.email||'', value: parseFloat(k.valor||k.value||0)||0, gender: k.genero||k.gender||k.sexo||'', cep: k.cep||k.zip||'', city: k.cidade||k.city||'', state: k.estado||k.state||'' };
    };
    const results = [];
    for (let i = 0; i < rows.length; i++) {
      const lead = normalize(rows[i]);
      if (!lead.phone || !lead.value) { results.push({ success: false, error: 'Telefone ou valor ausente' }); continue; }
      // Busca ctwa_clid pelo número
      const ctwa_clid = await db.getCtwaClid(lead.phone);
      lead.ctwa_clid = ctwa_clid || null;
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

app.get('/api/ctwa-leads', auth, async function(req, res) {
  try {
    var limit  = parseInt(req.query.limit  || 50);
    var page   = parseInt(req.query.page   || 1);
    var offset = (page - 1) * limit;
    var phone  = req.query.phone || null;
    res.json(await db.getCtwaLeads({ limit: limit, offset: offset, phone: phone }));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/ctwa-leads — lista leads com ctwa_clid capturado
app.get('/api/ctwa-leads', auth, async function(req, res) {
  try {
    var limit  = parseInt(req.query.limit  || 50);
    var page   = parseInt(req.query.page   || 1);
    var offset = (page - 1) * limit;
    var phone  = req.query.phone || null;
    var result = await db.getCtwaLeads({ limit: limit, offset: offset, phone: phone });
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// =============================================================================
// MÓDULO KWAI TRACKER
// =============================================================================

const KWAI_PIXELS    = ['304883249383318', '308803017463304'];
const KWAI_EVENT_IDS = { '304883249383318': '689521911', '308803017463304': '687782634' };
const KWAI_WPP_TEXT  = 'Olá! Gostaria de saber mais sobre o tratamento.';
const DEFAULT_VALUE  = 497;
const MAX_NUMBERS    = 20;

function gerarLeadId() {
  var chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  var id = '';
  for (var i = 0; i < 6; i++) id += chars[Math.floor(Math.random() * chars.length)];
  return id;
}

function kwaiPool() {
  const { Pool } = require('pg');
  return new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
}

async function initKwaiDB() {
  const pool = kwaiPool();
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
  const eventId     = uuidv4();
  const kwaiEventId = KWAI_EVENT_IDS[pixelId] || null;
  const userData = {};
  if (clickId)              userData.click_id = clickId;
  if (extra && extra.phone) userData.ph = extra.phone.replace(/\D/g, '');
  const eventObj = {
    event_name:  eventName,
    event_id:    eventId,
    event_time:  Math.floor(Date.now() / 1000),
    user_data:   userData,
    custom_data: { value: (extra && extra.value) || DEFAULT_VALUE, currency: 'BRL', content_name: 'Glivia', content_id: 'glivia' }
  };
  if (kwaiEventId) eventObj.event_type_id = kwaiEventId;
  var results = [];

  try {
    var activateParams = new URLSearchParams();
    if (clickId) activateParams.append('callback', clickId);
    activateParams.append('pixel_id', pixelId);
    activateParams.append('event_type', '3');
    activateParams.append('event_time', String(Date.now()));
    activateParams.append('purchase_amount', String(((extra && extra.value) || DEFAULT_VALUE).toFixed(2)));
    activateParams.append('event_name', 'EVENT_PURCHASE');
    if (extra && extra.phone) activateParams.append('phone', (extra.phone || '').replace(/\D/g, ''));
    var r1 = await fetch('http://ad.partner.gifshow.com/track/activate?' + activateParams.toString(), {
      method: 'GET',
      headers: { 'User-Agent': 'Mozilla/5.0 (Linux; Android 11) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36' }
    });
    var b1 = await r1.text();
    console.log('[Kwai Activate ' + pixelId + '] → ' + r1.status + ': ' + b1);
    results.push({ method: 'activate', ok: r1.ok, status: r1.status, body: b1 });
  } catch(e1) {
    results.push({ method: 'activate', ok: false, error: e1.message });
  }

  try {
    var mobilePayload = {
      pixel_id: pixelId,
      events: [{
        event_name: eventName, event_id: eventId, event_time: Math.floor(Date.now() / 1000),
        user_data: Object.assign({}, clickId ? { click_id: clickId } : {}, (extra && extra.phone) ? { ph: extra.phone.replace(/\D/g, '') } : {}),
        custom_data: { value: (extra && extra.value) || DEFAULT_VALUE, currency: 'BRL', content_name: 'Glivia', content_id: 'glivia', content_type: 'product' },
        page: { url: 'https://oficialvitalife.shop/obrigado', referrer: 'https://oficialvitalife.shop/life/' }
      }]
    };
    var ip1 = '177.' + Math.floor(Math.random()*255) + '.' + Math.floor(Math.random()*255) + '.' + Math.floor(Math.random()*255);
    var r2 = await fetch('https://s21-def.ap4r.com/rest/n/v1/pixel/batch?sdkid=' + pixelId + '&lib=kwaiq', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json', 'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pt-BR,pt;q=0.9', 'Origin': 'https://oficialvitalife.shop',
        'Referer': 'https://oficialvitalife.shop/obrigado',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 13; SM-A536B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36',
        'sec-ch-ua': '"Chromium";v="112","Google Chrome";v="112","Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?1', 'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'empty', 'sec-fetch-mode': 'cors', 'sec-fetch-site': 'cross-site',
        'x-forwarded-for': ip1, 'x-real-ip': ip1
      },
      body: JSON.stringify(mobilePayload)
    });
    var b2 = await r2.text();
    console.log('[Kwai Mobile ' + pixelId + '] ' + eventName + ' → ' + r2.status + ': ' + b2);
    results.push({ method: 'mobile', ok: r2.ok, status: r2.status, body: b2, event_id: eventId });
  } catch(err) {
    results.push({ method: 'mobile', ok: false, error: err.message, event_id: eventId });
  }

  return { ok: results.some(function(r) { return r.ok; }), results: results, event_id: eventId };
}

async function kwaiDispararTodos(eventName, clickId, extra) {
  return Promise.all(KWAI_PIXELS.map(function(pid) { return kwaiDispararEvento(eventName, clickId, pid, extra); }));
}

app.post('/kwai/lead/criar', async function(req, res) {
  const pool = kwaiPool();
  try {
    const { kwai_click_id, utm_source, utm_campaign, utm_adset, utm_ad, session_id } = req.body;
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress || '';
    const user_agent = req.headers['user-agent'] || '';
    let lead_id, attempts = 0;
    while (attempts < 10) {
      lead_id = gerarLeadId();
      const exists = await pool.query('SELECT id FROM kwai_leads WHERE lead_id = $1', [lead_id]);
      if (!exists.rows.length) break;
      attempts++;
    }
    const result = await pool.query(
      `INSERT INTO kwai_leads (lead_id, kwai_click_id, utm_source, utm_campaign, utm_adset, utm_ad, ip, user_agent, session_id, status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'visitou') RETURNING id, lead_id`,
      [lead_id, kwai_click_id||null, utm_source||null, utm_campaign||null, utm_adset||null, utm_ad||null, ip, user_agent, session_id||null]
    );
    console.log('[Kwai] Lead criado: ' + lead_id);
    res.json({ ok: true, lead_id: lead_id });
  } catch(err) {
    res.status(500).json({ error: err.message });
  } finally { await pool.end(); }
});

app.get('/kwai/ir', async function(req, res) {
  const lead_id = req.query.lid || null, phonesParam = req.query.phones || null, phoneParam = req.query.phone || null;
  let number = null;
  if (phonesParam) {
    const list = phonesParam.split(',').map(function(n) { return n.trim().replace(/\D/g,''); }).filter(Boolean);
    if (list.length) number = list[Math.floor(Math.random() * list.length)];
  } else if (phoneParam) {
    number = phoneParam.replace(/\D/g,'');
  } else {
    const pool2 = kwaiPool();
    try { const r = await pool2.query("SELECT number FROM kwai_numbers WHERE active = TRUE ORDER BY id LIMIT 1"); if (r.rows.length) number = r.rows[0].number; }
    catch(e) {} finally { await pool2.end(); }
  }
  if (!number) return res.status(400).send('Nenhum número configurado.');
  if (lead_id) {
    const pool = kwaiPool();
    try { await pool.query(`UPDATE kwai_leads SET status='clicou', wpp_number=$1, clicked_at=NOW(), clicks=COALESCE(clicks,0)+1 WHERE lead_id=$2`, [number, lead_id]); }
    catch(err) {} finally { await pool.end(); }
  }
  res.redirect(302, 'https://api.whatsapp.com/send/?phone=' + number + '&text=' + encodeURIComponent(KWAI_WPP_TEXT + (lead_id ? ' REF:' + lead_id : '')) + '&type=phone_number&app_absent=0');
});

app.get('/api/kwai/numbers', auth, async function(req, res) {
  const pool = kwaiPool();
  try { const r = await pool.query('SELECT * FROM kwai_numbers ORDER BY id'); res.json({ numbers: r.rows }); }
  catch(err) { res.status(500).json({ error: err.message }); } finally { await pool.end(); }
});

app.post('/api/kwai/numbers', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const { label, number } = req.body;
    if (!number) return res.status(400).json({ error: 'Número obrigatório' });
    const clean = number.replace(/\D/g,'');
    const count = await pool.query('SELECT COUNT(*) FROM kwai_numbers');
    if (parseInt(count.rows[0].count) >= MAX_NUMBERS) return res.status(400).json({ error: 'Limite atingido' });
    const r = await pool.query('INSERT INTO kwai_numbers (label, number) VALUES ($1,$2) RETURNING *', [label||clean, clean]);
    res.json({ ok: true, number: r.rows[0] });
  } catch(err) { res.status(500).json({ error: err.message }); } finally { await pool.end(); }
});

app.delete('/api/kwai/numbers/:id', auth, async function(req, res) {
  const pool = kwaiPool();
  try { await pool.query('DELETE FROM kwai_numbers WHERE id = $1', [req.params.id]); res.json({ ok: true }); }
  catch(err) { res.status(500).json({ error: err.message }); } finally { await pool.end(); }
});

app.get('/api/kwai/lead/:query', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const q = req.params.query.replace(/\s/g,'').toUpperCase();
    const result = await pool.query(`SELECT * FROM kwai_leads WHERE lead_id=$1 OR phone LIKE $2 ORDER BY created_at DESC LIMIT 10`, [q, '%'+q+'%']);
    res.json({ leads: result.rows });
  } catch(err) { res.status(500).json({ error: err.message }); } finally { await pool.end(); }
});

app.post('/api/kwai/lead/:lead_id/phone', auth, async function(req, res) {
  const pool = kwaiPool();
  try { await pool.query('UPDATE kwai_leads SET phone=$1 WHERE lead_id=$2', [req.body.phone.replace(/\D/g,''), req.params.lead_id]); res.json({ ok: true }); }
  catch(err) { res.status(500).json({ error: err.message }); } finally { await pool.end(); }
});

app.post('/api/kwai/purchase', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const { lead_id, ref, phone, value } = req.body;
    let lead;
    const searchId = (ref || lead_id || '').toUpperCase().replace('REF:','').trim();
    if (searchId) { const r = await pool.query('SELECT * FROM kwai_leads WHERE lead_id=$1', [searchId]); lead = r.rows[0]; }
    if (!lead && phone) { const r = await pool.query('SELECT * FROM kwai_leads WHERE phone LIKE $1 ORDER BY created_at DESC LIMIT 1', ['%'+phone.replace(/\D/g,'')+'%']); lead = r.rows[0]; }
    if (!lead) return res.status(404).json({ error: 'Lead não encontrado.' });
    if (lead.status === 'purchased') return res.status(400).json({ error: 'Purchase já disparado' });
    const purchaseValue = parseFloat(value) || DEFAULT_VALUE;
    const results = await kwaiDispararTodos('PURCHASE', lead.kwai_click_id, { phone: lead.phone, value: purchaseValue });
    await pool.query('UPDATE kwai_leads SET status=$1, purchased_at=NOW(), purchase_value=$2, kwai_results=$3 WHERE id=$4', ['purchased', purchaseValue, JSON.stringify(results), lead.id]);
    res.json({ success: true, lead: lead, results: results });
  } catch(err) { res.status(500).json({ error: err.message }); } finally { await pool.end(); }
});

app.get('/api/kwai/leads', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const status = req.query.status, limit = parseInt(req.query.limit)||50, page = parseInt(req.query.page)||1, offset = (page-1)*limit;
    let query = 'SELECT * FROM kwai_leads'; const params = [];
    if (status) { query += ' WHERE status=$1'; params.push(status); }
    query += ' ORDER BY created_at DESC LIMIT ' + limit + ' OFFSET ' + offset;
    const rows = await pool.query(query, params);
    const count = await pool.query(status ? 'SELECT COUNT(*) FROM kwai_leads WHERE status=$1' : 'SELECT COUNT(*) FROM kwai_leads', status ? [status] : []);
    res.json({ leads: rows.rows, total: parseInt(count.rows[0].count), page: page });
  } catch(err) { res.status(500).json({ error: err.message }); } finally { await pool.end(); }
});

app.get('/api/kwai/stats', auth, async function(req, res) {
  const pool = kwaiPool();
  try {
    const total     = await pool.query('SELECT COUNT(*) FROM kwai_leads');
    const clicou    = await pool.query("SELECT COUNT(*) FROM kwai_leads WHERE status IN ('clicou','purchased')");
    const purchases = await pool.query("SELECT COUNT(*), COALESCE(SUM(purchase_value),0) FROM kwai_leads WHERE status='purchased'");
    const hoje      = await pool.query("SELECT COUNT(*) FROM kwai_leads WHERE created_at >= CURRENT_DATE");
    const vendasHj  = await pool.query("SELECT COUNT(*) FROM kwai_leads WHERE status='purchased' AND purchased_at >= CURRENT_DATE");
    res.json({
      total_leads: parseInt(total.rows[0].count), total_cliques: parseInt(clicou.rows[0].count),
      total_purchases: parseInt(purchases.rows[0].count), total_revenue: parseFloat(purchases.rows[0].coalesce),
      leads_hoje: parseInt(hoje.rows[0].count), vendas_hoje: parseInt(vendasHj.rows[0].count),
      taxa_conversao: clicou.rows[0].count > 0 ? ((purchases.rows[0].count / clicou.rows[0].count)*100).toFixed(1)+'%' : '0%'
    });
  } catch(err) { res.status(500).json({ error: err.message }); } finally { await pool.end(); }
});

initKwaiDB().catch(console.error);

// =============================================================================
// ROTAS DE PÁGINAS
// =============================================================================
app.get('/kwai', function(req, res) { res.sendFile(path.join(__dirname, '../public/kwai.html')); });
app.get('/kwai.html', function(req, res) { res.sendFile(path.join(__dirname, '../public/kwai.html')); });
app.get('/spy', function(req, res) { res.sendFile(path.join(__dirname, '../public/spy.html')); });
app.get('/spy.html', function(req, res) { res.sendFile(path.join(__dirname, '../public/spy.html')); });

// CATCH-ALL — sempre no final
app.get('*', function(req, res) { res.sendFile(path.join(__dirname, '../public/index.html')); });

app.listen(PORT, function() { console.log('Servidor rodando na porta ' + PORT); });
