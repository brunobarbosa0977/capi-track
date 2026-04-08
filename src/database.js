const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '../data');
const CONFIG_FILE = path.join(DATA_DIR, 'config.json');
const EVENTS_FILE = path.join(DATA_DIR, 'events.json');
const PIXELS_FILE = path.join(DATA_DIR, 'pixels.json');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(CONFIG_FILE)) fs.writeFileSync(CONFIG_FILE, 'null');
if (!fs.existsSync(EVENTS_FILE)) fs.writeFileSync(EVENTS_FILE, '[]');
if (!fs.existsSync(PIXELS_FILE)) fs.writeFileSync(PIXELS_FILE, '[]');

function readConfig() { try { return JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8')); } catch(e) { return null; } }
function writeConfig(cfg) { fs.writeFileSync(CONFIG_FILE, JSON.stringify(cfg, null, 2)); }
function readEvents() { try { return JSON.parse(fs.readFileSync(EVENTS_FILE, 'utf8')); } catch(e) { return []; } }
function writeEvents(events) { fs.writeFileSync(EVENTS_FILE, JSON.stringify(events, null, 2)); }
function readPixels() { try { return JSON.parse(fs.readFileSync(PIXELS_FILE, 'utf8')); } catch(e) { return []; } }
function writePixels(pixels) { fs.writeFileSync(PIXELS_FILE, JSON.stringify(pixels, null, 2)); }

function getConfig() { return readConfig(); }

function saveConfig(cfg) {
  const existing = readConfig() || {};
  writeConfig(Object.assign({}, existing, cfg));
}

function getPixels() { return readPixels(); }

function savePixel(data) {
  const pixels = readPixels();
  if (data.id) {
    const idx = pixels.findIndex(function(p) { return p.id === data.id; });
    if (idx >= 0) {
      pixels[idx].name = data.name;
      pixels[idx].pixel_id = data.pixel_id;
      pixels[idx].access_token = data.access_token;
    } else {
      pixels.push({ id: data.id, name: data.name, pixel_id: data.pixel_id, access_token: data.access_token, created_at: new Date().toISOString() });
    }
  } else {
    pixels.push({ id: Date.now().toString(), name: data.name, pixel_id: data.pixel_id, access_token: data.access_token, created_at: new Date().toISOString() });
  }
  writePixels(pixels);
  return pixels;
}

function deletePixel(id) {
  const pixels = readPixels().filter(function(p) { return p.id !== id; });
  writePixels(pixels);
  return pixels;
}

function getPixelById(id) {
  return readPixels().find(function(p) { return p.id === id; }) || null;
}

function insertEvent(data) {
  const events = readEvents();
  events.unshift({
    id: Date.now(),
    created_at: new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' }),
    pixel_id: data.pixel_id || '',
    pixel_name: data.pixel_name || '',
    name: data.name || '',
    phone: data.phone || '',
    email: data.email || '',
    gender: data.gender || '',
    cep: data.cep || '',
    value: parseFloat(data.value) || 0,
    status: data.status,
    error_msg: data.error_msg || null,
    source: data.source || 'manual'
  });
  if (events.length > 5000) events.splice(5000);
  writeEvents(events);
}

function getEvents(opts) {
  const page = opts.page || 1;
  const status = opts.status;
  const pixel_id = opts.pixel_id;
  const limit = 50;
  let events = readEvents();
  if (status && status !== 'all') events = events.filter(function(e) { return e.status === status; });
  if (pixel_id && pixel_id !== 'all') events = events.filter(function(e) { return e.pixel_id === pixel_id; });
  return { total: events.length, page: page, rows: events.slice((page - 1) * limit, page * limit) };
}

function parseDate(str) {
  try {
    const parts = str.split(',')[0].trim().split('/');
    return new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, parseInt(parts[0]));
  } catch(e) { return null; }
}

function sameDay(d1, d2) {
  return d1 && d2 && d1.getDate() === d2.getDate() && d1.getMonth() === d2.getMonth() && d1.getFullYear() === d2.getFullYear();
}

function getStats(pixel_id) {
  let events = readEvents();
  if (pixel_id && pixel_id !== 'all') events = events.filter(function(e) { return e.pixel_id === pixel_id; });
  const today = new Date();
  const todayAll = events.filter(function(e) { return sameDay(parseDate(e.created_at), today); });
  const last30All = events.filter(function(e) {
    const d = parseDate(e.created_at);
    return d && (Date.now() - d.getTime()) <= 30 * 24 * 60 * 60 * 1000;
  });
  const sent30 = last30All.filter(function(e) { return e.status === 'sent'; }).length;
  const chart = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    const dayEvents = events.filter(function(e) { return sameDay(parseDate(e.created_at), d); });
    chart.push({
      day: ('0' + d.getDate()).slice(-2) + '/' + ('0' + (d.getMonth()+1)).slice(-2),
      sent: dayEvents.filter(function(e) { return e.status === 'sent'; }).length,
      errors: dayEvents.filter(function(e) { return e.status === 'error'; }).length
    });
  }
  return {
    eventsToday: todayAll.length,
    valueToday: todayAll.filter(function(e) { return e.status === 'sent'; }).reduce(function(s, e) { return s + (e.value || 0); }, 0),
    errorsToday: todayAll.filter(function(e) { return e.status === 'error'; }).length,
    matchRate: last30All.length > 0 ? ((sent30 / last30All.length) * 100).toFixed(1) : '0.0',
    sent30: sent30,
    last30: last30All.length,
    chart: chart
  };
}

module.exports = { getConfig, saveConfig, getPixels, savePixel, deletePixel, getPixelById, insertEvent, getEvents, getStats };
