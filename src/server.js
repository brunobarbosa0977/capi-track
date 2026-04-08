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

// LOGIN
app.post('/api/login', (req, res) => {
  const { password } = req.body;
  const cfg = db.getConfig();
  if (!cfg) {
    const token = uuidv4();
    db.saveConfig({ password, auth_token: token, webhook_token: uuidv4().replace(/-/g,''), pixel_id: '', access_token: '', pixel_name: '' });
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

// CONFIG
app.get('/api/config', auth, (req, res) => {
  const cfg = db.getConfig();
  if (!cfg) return res.json({});
  const { password, auth_token, ...safe } = cfg;
  res.json(safe);
});

app.post('/api/config', auth, (req, res) => {
  const { pixel_id, access_token, pixel_name } = req.body;
  if (!pixel_id || !access_token) return res.status(400).json({ error: 'Pixel ID e Access Token sao obrigatorios' });
  db.saveConfig({ pixel_id, access_token, pixel_name: pixel_name || '' });
  res.json({ ok: true });
});

// WEBHOOK
app.post('/webhook/:token', async (req, res) => {
  const cfg = db.getConfig();
  if (!cfg || req.params.token !== cfg.webhook_token) return res.status(403).json({ error: 'Webhook invalido' });
  const { name, phone, email, value } = req.body;
  if (!phone || !value) return res.status(400).json({ error: 'phone e value sao obrigatorios' });
  const result = await metaApi.sendPurchase(cfg, { name, phone, email, value });
  db.insertEvent({ name, phone, email, value, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'webhook' });
  res.json(result);
});

// SEND MANUAL
app.post('/api/send', auth, async (req, res) => {
  const cfg = db.getConfig();
  if (!cfg || !cfg.pixel_id || !cfg.access_token) return res.status(400).json({ error: 'Configure o Pixel ID e Access Token primeiro' });
  const { name, phone, email, value } = req.body;
  if (!phone || !value) return res.status(400).json({ error: 'Telefone e valor sao obrigatorios' });
  const result = await metaApi.sendPurchase(cfg, { name, phone, email, value });
  db.insertEvent({ name, phone, email, value, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'manual' });
  res.json(result);
});

// PREVIEW BULK
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
    } else {
      return res.status(400).json({ error: 'Formato invalido. Use CSV ou XLSX.' });
    }
  } catch (e) { return res.status(400).json({ error: 'Erro ao ler arquivo: ' + e.message }); }

  const normalize = (row) => {
    const k = Object.keys(row).reduce((acc, key) => { acc[key.toLowerCase().trim()] = row[key]; return acc; }, {});
    return { name: k.nome || k.name || '', phone: k.telefone || k.phone || k.fone || '', email: k.email || '', value: parseFloat(k.valor || k.value || 0) || 0 };
  };
  res.json({ rows: rows.map(normalize), total: rows.length });
});

// SEND BULK
app.post('/api/send-bulk', auth, upload.single('file'), async (req, res) => {
  const cfg = db.getConfig();
  if (!cfg || !cfg.pixel_id || !cfg.access_token) return res.status(400).json({ error: 'Configure o Pixel ID e Access Token primeiro' });
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

  const normalize = (row) => {
    const k = Object.keys(row).reduce((acc, key) => { acc[key.toLowerCase().trim()] = row[key]; return acc; }, {});
    return { name: k.nome || k.name || '', phone: k.telefone || k.phone || k.fone || '', email: k.email || '', value: parseFloat(k.valor || k.value || 0) || 0 };
  };

  const results = [];
  for (const raw of rows) {
    const lead = normalize(raw);
    if (!lead.phone || !lead.value) { results.push({ ...lead, success: false, error: 'Telefone ou valor ausente' }); continue; }
    const result = await metaApi.sendPurchase(cfg, lead);
    db.insertEvent({ ...lead, status: result.success ? 'sent' : 'error', error_msg: result.error || null, source: 'bulk' });
    results.push({ ...lead, ...result });
    await new Promise(r => setTimeout(r, 200));
  }
  res.json({ total: results.length, sent: results.filter(r => r.success).length, errors: results.filter(r => !r.success).length, results });
});

// EVENTS
app.get('/api/events', auth, (req, res) => {
  res.json(db.getEvents({ page: parseInt(req.query.page || 1), status: req.query.status }));
});

// STATS
app.get('/api/stats', auth, (req, res) => res.json(db.getStats()));

// WEBHOOK URL
app.get('/api/webhook-url', auth, (req, res) => {
  const cfg = db.getConfig();
  const base = process.env.BASE_URL || `http://localhost:${PORT}`;
  res.json({ url: `${base}/webhook/${cfg.webhook_token}` });
});

app.get('*', (req, res) => res.sendFile(path.join(__dirname, '../public/index.html')));

app.listen(PORT, () => console.log(`Servidor rodando na porta ${PORT}`));
