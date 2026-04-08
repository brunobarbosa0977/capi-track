const express = require('express');
const cors = require('cors');
const path = require('path');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');
const db = require('./database');
const metaApi = require('./meta');

const app = express();
const PORT = process.env.PORT || 3000;
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

async function auth(req, res, next) {
  try {
    const token = req.headers['x-auth-token'];
    const cfg = await db.getConfig();
    if (!cfg || !cfg.auth_token) return res.status(401).json({ error: 'Nao autenticado' });
    if (token !== cfg.auth_token) return res.status(401).json({ error: 'Token invalido' });
    next();
  } catch(e) { res.status(500).json({ error: e.message }); }
}

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

app.get('/api/pixels', auth, async function(req, res) {
  try {
    const pixels = await db.getPixels();
    res.json(pixels.map(function(p) { return { id: p.id, name: p.name, pixel_id: p.pixel_id, created_at: p.created_at }; }));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/pixels', auth, async function(req, res) {
  try {
    const id = req.body.id;
    const name = req.body.name;
    const pixel_id = req.body.pixel_id;
    const access_token = req.body.access_token;
    if (!name || !pixel_id || !access_token) return res.status(400).json({ error: 'Todos os campos sao obrigatorios' });
    const pixels = await db.savePixel({ id: id, name: name, pixel_id: pixel_id, access_token: access_token });
    res.json({ ok: true, pixels: pixels.map(function(p) { return { id: p.id, name: p.name, pixel_id: p.pixel_id }; }) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/pixels/:id', auth, async function(req, res) {
  try {
    await db.deletePixel(req.params.id);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

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
    const name = req.body.name, phone = req.body.phone, email = req.body.email;
    const value = req.body.value, gender = req.body.gender, cep = req.body.cep;
    const pixel_id = req.body.pixel_id;
    if (!phone || !value) return res.status(400).json({ error: 'phone e value sao obrigatorios' });
    let pixelCfg = pixel_id ? await db.getPixelById(pixel_id) : (await db.getPixels())[0];
    if (!pixelCfg) return res.status(400).json({ error: 'Nenhum pixel configurado' });
    const result = await metaApi.sendPurchase(pixelCfg, { name, phone, email, value, gender, cep });
    await db.insertEvent({ pixel_id: pixelCfg.id, pixel_name: pixelCfg.name, name, phone, email, value, gender, cep, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'webhook' });
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/send', auth, async function(req, res) {
  try {
    const name = req.body.name, phone = req.body.phone, email = req.body.email;
    const value = req.body.value, gender = req.body.gender, cep = req.body.cep;
    const pixel_id = req.body.pixel_id;
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

app.get('*', function(req, res) {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

app.listen(PORT, function() { console.log('Servidor rodando na porta ' + PORT); });
