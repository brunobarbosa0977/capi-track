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

function auth(req, res, next) {
  const token = req.headers['x-auth-token'];
  const cfg = db.getConfig();
  if (!cfg || !cfg.auth_token) return res.status(401).json({ error: 'Nao autenticado' });
  if (token !== cfg.auth_token) return res.status(401).json({ error: 'Token invalido' });
  next();
}

// LOGIN - primeiro acesso cadastra qualquer senha
app.post('/api/login', function(req, res) {
  const password = req.body.password;
  const cfg = db.getConfig();
  if (!cfg) {
    const token = uuidv4();
    const webhook = uuidv4().replace(/-/g, '');
    db.saveConfig({ password: password, auth_token: token, webhook_token: webhook });
    return res.json({ token: token });
  }
  if (password !== cfg.password) return res.status(401).json({ error: 'Senha incorreta' });
  res.json({ token: cfg.auth_token });
});

// RESET DE EMERGENCIA - deleta config e permite novo cadastro
app.post('/api/reset', function(req, res) {
  const secret = req.body.secret;
  if (secret !== 'infinity2026reset') return res.status(403).json({ error: 'Nao autorizado' });
  const fs = require('fs');
  const configPath = require('path').join(__dirname, '../data/config.json');
  if (fs.existsSync(configPath)) fs.writeFileSync(configPath, 'null');
  res.json({ ok: true, msg: 'Config resetada. Acesse a plataforma e cadastre nova senha.' });
});

app.post('/api/change-password', auth, function(req, res) {
  const new_password = req.body.new_password;
  if (!new_password || new_password.length < 4) return res.status(400).json({ error: 'Senha muito curta' });
  db.saveConfig({ password: new_password });
  res.json({ ok: true });
});

// PIXELS
app.get('/api/pixels', auth, function(req, res) {
  const pixels = db.getPixels().map(function(p) {
    return { id: p.id, name: p.name, pixel_id: p.pixel_id, created_at: p.created_at };
  });
  res.json(pixels);
});

app.post('/api/pixels', auth, function(req, res) {
  const id = req.body.id;
  const name = req.body.name;
  const pixel_id = req.body.pixel_id;
  const access_token = req.body.access_token;
  if (!name || !pixel_id || !access_token) return res.status(400).json({ error: 'Nome, Pixel ID e Access Token sao obrigatorios' });
  const pixels = db.savePixel({ id: id, name: name, pixel_id: pixel_id, access_token: access_token });
  res.json({ ok: true, pixels: pixels.map(function(p) { return { id: p.id, name: p.name, pixel_id: p.pixel_id }; }) });
});

app.delete('/api/pixels/:id', auth, function(req, res) {
  db.deletePixel(req.params.id);
  res.json({ ok: true });
});

// WEBHOOK URL
app.get('/api/webhook-url', auth, function(req, res) {
  const cfg = db.getConfig();
  const base = process.env.BASE_URL || ('http://localhost:' + PORT);
  res.json({ url: base + '/webhook/' + cfg.webhook_token });
});

// WEBHOOK RECEIVER
app.post('/webhook/:token', async function(req, res) {
  const cfg = db.getConfig();
  if (!cfg || req.params.token !== cfg.webhook_token) return res.status(403).json({ error: 'Webhook invalido' });
  const name = req.body.name;
  const phone = req.body.phone;
  const email = req.body.email;
  const value = req.body.value;
  const gender = req.body.gender;
  const cep = req.body.cep;
  const pixel_id = req.body.pixel_id;
  if (!phone || !value) return res.status(400).json({ error: 'phone e value sao obrigatorios' });
  let pixelCfg = pixel_id ? db.getPixelById(pixel_id) : db.getPixels()[0];
  if (!pixelCfg) return res.status(400).json({ error: 'Nenhum pixel configurado' });
  const result = await metaApi.sendPurchase(pixelCfg, { name: name, phone: phone, email: email, value: value, gender: gender, cep: cep });
  db.insertEvent({ pixel_id: pixelCfg.id, pixel_name: pixelCfg.name, name: name, phone: phone, email: email, value: value, gender: gender, cep: cep, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'webhook' });
  res.json(result);
});

// ENVIO MANUAL
app.post('/api/send', auth, async function(req, res) {
  const name = req.body.name;
  const phone = req.body.phone;
  const email = req.body.email;
  const value = req.body.value;
  const gender = req.body.gender;
  const cep = req.body.cep;
  const pixel_id = req.body.pixel_id;
  if (!phone || !value) return res.status(400).json({ error: 'Telefone e valor sao obrigatorios' });
  if (!pixel_id) return res.status(400).json({ error: 'Selecione um pixel' });
  const pixelCfg = db.getPixelById(pixel_id);
  if (!pixelCfg) return res.status(400).json({ error: 'Pixel nao encontrado' });
  const result = await metaApi.sendPurchase(pixelCfg, { name: name, phone: phone, email: email, value: value, gender: gender, cep: cep });
  db.insertEvent({ pixel_id: pixelCfg.id, pixel_name: pixelCfg.name, name: name, phone: phone, email: email, value: value, gender: gender, cep: cep, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'manual' });
  res.json(result);
});

// PREVIEW BULK
app.post('/api/preview-bulk', auth, upload.single('file'), function(req, res) {
  if (!req.file) return res.status(400).json({ error: 'Nenhum arquivo enviado' });
  const ext = req.file.originalname.split('.').pop().toLowerCase();
  let rows = [];
  try {
    if (ext === 'csv') {
      const csvParse = require('csv-parse/sync');
      rows = csvParse.parse(req.file.buffer.toString('utf8'), { columns: true, skip_empty_lines: true, trim: true });
    } else if (ext === 'xlsx' || ext === 'xls') {
      const XLSX = require('xlsx');
      const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
      rows = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);
    } else {
      return res.status(400).json({ error: 'Formato invalido. Use CSV ou XLSX.' });
    }
  } catch(e) { return res.status(400).json({ error: 'Erro ao ler arquivo: ' + e.message }); }
  const normalize = function(row) {
    const k = {};
    Object.keys(row).forEach(function(key) { k[key.toLowerCase().trim()] = row[key]; });
    return {
      name: k.nome || k.name || '',
      phone: k.telefone || k.phone || k.fone || '',
      email: k.email || '',
      value: parseFloat(k.valor || k.value || 0) || 0,
      gender: k.genero || k.gender || k.sexo || '',
      cep: k.cep || k.zip || ''
    };
  };
  res.json({ rows: rows.map(normalize), total: rows.length });
});

// DISPARO EM MASSA
app.post('/api/send-bulk', auth, upload.single('file'), async function(req, res) {
  const pixel_id = req.body.pixel_id;
  if (!pixel_id) return res.status(400).json({ error: 'Selecione um pixel' });
  const pixelCfg = db.getPixelById(pixel_id);
  if (!pixelCfg) return res.status(400).json({ error: 'Pixel nao encontrado' });
  if (!req.file) return res.status(400).json({ error: 'Nenhum arquivo enviado' });
  const ext = req.file.originalname.split('.').pop().toLowerCase();
  let rows = [];
  try {
    if (ext === 'csv') {
      const csvParse = require('csv-parse/sync');
      rows = csvParse.parse(req.file.buffer.toString('utf8'), { columns: true, skip_empty_lines: true, trim: true });
    } else if (ext === 'xlsx' || ext === 'xls') {
      const XLSX = require('xlsx');
      const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
      rows = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);
    } else {
      return res.status(400).json({ error: 'Formato invalido.' });
    }
  } catch(e) { return res.status(400).json({ error: 'Erro ao ler: ' + e.message }); }
  const normalize = function(row) {
    const k = {};
    Object.keys(row).forEach(function(key) { k[key.toLowerCase().trim()] = row[key]; });
    return {
      name: k.nome || k.name || '',
      phone: k.telefone || k.phone || k.fone || '',
      email: k.email || '',
      value: parseFloat(k.valor || k.value || 0) || 0,
      gender: k.genero || k.gender || k.sexo || '',
      cep: k.cep || k.zip || ''
    };
  };
  const results = [];
  for (let i = 0; i < rows.length; i++) {
    const lead = normalize(rows[i]);
    if (!lead.phone || !lead.value) {
      results.push({ success: false, error: 'Telefone ou valor ausente', name: lead.name, phone: lead.phone });
      continue;
    }
    const result = await metaApi.sendPurchase(pixelCfg, lead);
    db.insertEvent({ pixel_id: pixelCfg.id, pixel_name: pixelCfg.name, name: lead.name, phone: lead.phone, email: lead.email, value: lead.value, gender: lead.gender, cep: lead.cep, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'bulk' });
    results.push({ success: result.success, name: lead.name, phone: lead.phone });
    await new Promise(function(resolve) { setTimeout(resolve, 200); });
  }
  const sent = results.filter(function(r) { return r.success; }).length;
  const errors = results.filter(function(r) { return !r.success; }).length;
  res.json({ total: results.length, sent: sent, errors: errors });
});

// EVENTOS
app.get('/api/events', auth, function(req, res) {
  res.json(db.getEvents({ page: parseInt(req.query.page || 1), status: req.query.status, pixel_id: req.query.pixel_id }));
});

// STATS
app.get('/api/stats', auth, function(req, res) {
  res.json(db.getStats(req.query.pixel_id));
});

// SPA fallback
app.get('*', function(req, res) {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

app.listen(PORT, function() {
  console.log('Servidor rodando na porta ' + PORT);
});
