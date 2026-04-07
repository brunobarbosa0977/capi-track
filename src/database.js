const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '../data');
const CONFIG_FILE = path.join(DATA_DIR, 'config.json');
const EVENTS_FILE = path.join(DATA_DIR, 'events.json');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(CONFIG_FILE)) fs.writeFileSync(CONFIG_FILE, 'null');
if (!fs.existsSync(EVENTS_FILE)) fs.writeFileSync(EVENTS_FILE, '[]');

function readConfig() {
  try { return JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8')); } catch { return null; }
}
function writeConfig(cfg) { fs.writeFileSync(CONFIG_FILE, JSON.stringify(cfg, null, 2)); }
function readEvents() {
  try { return JSON.parse(fs.readFileSync(EVENTS_FILE, 'utf8')); } catch { return []; }
}
function writeEvents(events) { fs.writeFileSync(EVENTS_FILE, JSON.stringify(events, null, 2)); }

function getConfig() { return readConfig(); }
function saveConfig(cfg) { writeConfig({ ...(readConfig() || {}), ...cfg }); }

function insertEvent({ name, phone, email, value, status, error_msg, source }) {
  const events = readEvents();
  events.unshift({
    id: Date.now(),
    created_at: new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' }),
    name: name || '', phone: phone || '', email: email || '',
    value: parseFloat(value) || 0, status,
    error_msg: error_msg || null, source: source || 'manual'
  });
  if (events.length > 5000) events.splice(5000);
  writeEvents(events);
}

function getEvents({ page = 1, status } = {}) {
  const limit = 50;
  let events = readEvents();
  if (status && status !== 'all') events = events.filter(e => e.status === status);
  return { total: events.length, page, rows: events.slice((page - 1) * limit, page * limit) };
}

function parseDate(str) {
  try {
    const parts = str.split(',')[0].trim().split('/');
    return new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, parseInt(parts[0]));
  } catch { return null; }
}

function sameDay(d1, d2) {
  return d1 && d2 && d1.getDate() === d2.getDate() && d1.getMonth() === d2.getMonth() && d1.getFullYear() === d2.getFullYear();
}

function getStats() {
  const events = readEvents();
  const today = new Date();
  const todayAll = events.filter(e => sameDay(parseDate(e.created_at), today));
  const last30All = events.filter(e => {
    const d = parseDate(e.created_at);
    return d && (Date.now() - d.getTime()) <= 30 * 24 * 60 * 60 * 1000;
  });
  const sent30 = last30All.filter(e => e.status === 'sent').length;
  const chart = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    const dayEvents = events.filter(e => sameDay(parseDate(e.created_at), d));
    chart.push({
      day: `${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}`,
      sent: dayEvents.filter(e => e.status === 'sent').length,
      errors: dayEvents.filter(e => e.status === 'error').length
    });
  }
  return {
    eventsToday: todayAll.length,
    valueToday: todayAll.filter(e => e.status === 'sent').reduce((s, e) => s + (e.value || 0), 0),
    errorsToday: todayAll.filter(e => e.status === 'error').length,
    matchRate: last30All.length > 0 ? ((sent30 / last30All.length) * 100).toFixed(1) : '0.0',
    sent30, last30: last30All.length, chart
  };
}

module.exports = { getConfig, saveConfig, insertEvent, getEvents, getStats };
