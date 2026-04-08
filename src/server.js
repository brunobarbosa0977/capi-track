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

app.post('/api/login', (req, res) => {
  const { password } = req.body;
  const cfg = db.getConfig();
  if (!cfg) {
    const token = uuidv4();
    db.saveConfig({ password, auth_token: token, webhook_token: uuidv4().replace(/-/g,'') });
    return res.json({ token });
  }
  if (password !== cfg.password) return res.status(401).json({ error: 'Senha incorreta' });
  res.json({ token: cfg.auth_token });
});

app.post('/api/change-password', auth, (req, res) => {
  const { new_password } = req.body;
  if (!new_password || new_password.length < 4) return res.status(400).json({ error: 'Senha muito curta' });
  db.saveConfig({ password: new_password });
  res.json({ ok: true });
});

app.get('/api/pixels', auth, (req, res) => {
  const pixels = db.getPixels().map(function(p) { return { id: p.id, name: p.name, pixel_id: p.pixel_id, created_at: p.created_at }; });
  res.json(pixels);
});

app.post('/api/pixels', auth, (req, res) => {
  const { id, name, pixel_id, access_token } = req.body;
  if (!name || !pixel_id || !access_token) return res.status(400).json({ error: 'Nome, Pixel ID e Access Token sao obrigatorios' });
  const pixels = db.savePixel({ id, name, pixel_id, access_token });
  res.json({ ok: true, pixels: pixels.map(function(p) { return { id: p.id, name: p.name, pixel_id: p.pixel_id }; }) });
});

app.delete('/api/pixels/:id', auth, (req, res) => {
  db.deletePixel(req.params.id);
  res.json({ ok: true });
});

app.get('/api/webhook-url', auth, (req, res) => {
  const cfg = db.getConfig();
  const base = process.env.BASE_URL || ('http://localhost:' + PORT);
  res.json({ url: base + '/webhook/' + cfg.webhook_token });
});

app.post('/webhook/:token', async (req, res) => {
  const cfg = db.getConfig();
  if (!cfg || req.params.token !== cfg.webhook_token) return res.status(403).json({ error: 'Webhook invalido' });
  const { name, phone, email, value, gender, cep, pixel_id } = req.body;
  if (!phone || !value) return res.status(400).json({ error: 'phone e value sao obrigatorios' });
  let pixelCfg = pixel_id ? db.getPixelById(pixel_id) : db.getPixels()[0];
  if (!pixelCfg) return res.status(400).json({ error: 'Nenhum pixel configurado' });
  const result = await metaApi.sendPurchase(pixelCfg, { name, phone, email, value, gender, cep });
  db.insertEvent({ pixel_id: pixelCfg.id, pixel_name: pixelCfg.name, name, phone, email, value, gender, cep, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'webhook' });
  res.json(result);
});

app.post('/api/send', auth, async (req, res) => {
  const { name, phone, email, value, gender, cep, pixel_id } = req.body;
  if (!phone || !value) return res.status(400).json({ error: 'Telefone e valor sao obrigatorios' });
  if (!pixel_id) return res.status(400).json({ error: 'Selecione um pixel' });
  const pixelCfg = db.getPixelById(pixel_id);
  if (!pixelCfg) return res.status(400).json({ error: 'Pixel nao encontrado' });
  const result = await metaApi.sendPurchase(pixelCfg, { name, phone, email, value, gender, cep });
  db.insertEvent({ pixel_id: pixelCfg.id, pixel_name: pixelCfg.name, name, phone, email, value, gender, cep, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'manual' });
  res.json(result);
});

app.post('/api/preview-bulk', auth, upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'Nenhum arquivo enviado' });
  const ext = req.file.originalname.split('.').pop().toLowerCase();
  let rows = [];
  try {
    if (ext === 'csv') {
      const { parse } = require('csv-parse/sync');
      rows = parse(req.file.buffer.toString('utf8'), { columns: true, skip_empty_lines: true, trim: true });
    } else if (ext === 'xlsx' || ext === 'xls') {
      const XLSX = require('xlsx');
      const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
      rows = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);
    } else { return res.status(400).json({ error: 'Formato invalido. Use CSV ou XLSX.' }); }
  } catch (e) { return res.status(400).json({ error: 'Erro ao ler arquivo: ' + e.message }); }
  const normalize = function(row) {
    const k = Object.keys(row).reduce(function(acc, key) { acc[key.toLowerCase().trim()] = row[key]; return acc; }, {});
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

app.post('/api/send-bulk', auth, upload.single('file'), async (req, res) => {
  const pixel_id = req.body.pixel_id;
  if (!pixel_id) return res.status(400).json({ error: 'Selecione um pixel' });
  const pixelCfg = db.getPixelById(pixel_id);
  if (!pixelCfg) return res.status(400).json({ error: 'Pixel nao encontrado' });
  if (!req.file) return res.status(400).json({ error: 'Nenhum arquivo enviado' });
  const ext = req.file.originalname.split('.').pop().toLowerCase();
  let rows = [];
  try {
    if (ext === 'csv') {
      const { parse } = require('csv-parse/sync');
      rows = parse(req.file.buffer.toString('utf8'), { columns: true, skip_empty_lines: true, trim: true });
    } else if (ext === 'xlsx' || ext === 'xls') {
      const XLSX = require('xlsx');
      const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
      rows = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);
    } else { return res.status(400).json({ error: 'Formato invalido.' }); }
  } catch (e) { return res.status(400).json({ error: 'Erro ao ler: ' + e.message }); }
  const normalize = function(row) {
    const k = Object.keys(row).reduce(function(acc, key) { acc[key.toLowerCase().trim()] = row[key]; return acc; }, {});
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
  for (const raw of rows) {
    const lead = normalize(raw);
    if (!lead.phone || !lead.value) { results.push(Object.assign({}, lead, { success: false, error: 'Telefone ou valor ausente' })); continue; }
    const result = await metaApi.sendPurchase(pixelCfg, lead);
    db.insertEvent(Object.assign({ pixel_id: pixelCfg.id, pixel_name: pixelCfg.name }, lead, { status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'bulk' }));
    results.push(Object.assign({}, lead, result));
    await new Promise(function(r) { setTimeout(r, 200); });
  }
  res.json({ total: results.length, sent: results.filter(function(r) { return r.success; }).length, errors: results.filter(function(r) { return !r.success; }).length });
});

app.get('/api/events', auth, (req, res) => {
  res.json(db.getEvents({ page: parseInt(req.query.page || 1), status: req.query.status, pixel_id: req.query.pixel_id }));
});

app.get('/api/stats', auth, (req, res) => res.json(db.getStats(req.query.pixel_id)));

app.get('*', (req, res) => res.sendFile(path.join(__dirname, '../public/index.html')));

app.listen(PORT, function() { console.log('Servidor rodando na porta ' + PORT); });
