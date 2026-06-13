const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('railway.internal')
    ? false
    : { rejectUnauthorized: false }
});

async function init() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS config (
      id INTEGER PRIMARY KEY DEFAULT 1,
      password TEXT,
      auth_token TEXT,
      webhook_token TEXT,
      CHECK (id = 1)
    );

    CREATE TABLE IF NOT EXISTS pixels (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      pixel_id TEXT NOT NULL,
      access_token TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS events (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      pixel_id TEXT,
      pixel_name TEXT,
      name TEXT,
      phone TEXT,
      email TEXT,
      gender TEXT,
      cep TEXT,
      value NUMERIC,
      status TEXT,
      error_msg TEXT,
      source TEXT DEFAULT 'manual'
    );

    CREATE TABLE IF NOT EXISTS ctwa_leads (
      id BIGSERIAL PRIMARY KEY,
      phone VARCHAR(30) NOT NULL,
      ctwa_clid TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(phone)
    );
  `);

  // Migrações seguras em tabelas existentes
  const migrations = [
    "CREATE INDEX IF NOT EXISTS idx_ctwa_phone ON ctwa_leads(phone)",
    "CREATE INDEX IF NOT EXISTS idx_ctwa_clid  ON ctwa_leads(ctwa_clid)",
    // MIGRAÇÃO: adiciona coluna page_id na tabela pixels se não existir
    "ALTER TABLE pixels ADD COLUMN IF NOT EXISTS page_id TEXT DEFAULT ''"
  ];
  for (const sql of migrations) {
    try { await pool.query(sql); } catch(e) {}
  }

  console.log('Banco de dados inicializado');
}

init().catch(function(err) { console.error('Erro ao inicializar banco:', err.message); });

// ─── CONFIG ───────────────────────────────────────────────────────────────────

async function getConfig() {
  const res = await pool.query('SELECT * FROM config WHERE id = 1');
  return res.rows[0] || null;
}

async function saveConfig(cfg) {
  const existing = await getConfig();
  if (!existing) {
    await pool.query(
      'INSERT INTO config (id, password, auth_token, webhook_token) VALUES (1, $1, $2, $3)',
      [cfg.password || '', cfg.auth_token || '', cfg.webhook_token || '']
    );
  } else {
    const fields = [];
    const values = [];
    let i = 1;
    if (cfg.password      !== undefined) { fields.push('password = $'       + i++); values.push(cfg.password); }
    if (cfg.auth_token    !== undefined) { fields.push('auth_token = $'     + i++); values.push(cfg.auth_token); }
    if (cfg.webhook_token !== undefined) { fields.push('webhook_token = $'  + i++); values.push(cfg.webhook_token); }
    if (fields.length > 0) {
      await pool.query('UPDATE config SET ' + fields.join(', ') + ' WHERE id = 1', values);
    }
  }
}

// ─── PIXELS ───────────────────────────────────────────────────────────────────

async function getPixels() {
  const res = await pool.query('SELECT * FROM pixels ORDER BY created_at ASC');
  return res.rows;
}

async function savePixel(data) {
  if (data.id) {
    const exists = await pool.query('SELECT id FROM pixels WHERE id = $1', [data.id]);
    if (exists.rows.length > 0) {
      await pool.query(
        'UPDATE pixels SET name=$1, pixel_id=$2, access_token=$3, page_id=$4 WHERE id=$5',
        [data.name, data.pixel_id, data.access_token, data.page_id || '', data.id]
      );
    } else {
      await pool.query(
        'INSERT INTO pixels (id, name, pixel_id, access_token, page_id) VALUES ($1,$2,$3,$4,$5)',
        [data.id, data.name, data.pixel_id, data.access_token, data.page_id || '']
      );
    }
  } else {
    const newId = Date.now().toString();
    await pool.query(
      'INSERT INTO pixels (id, name, pixel_id, access_token, page_id) VALUES ($1,$2,$3,$4,$5)',
      [newId, data.name, data.pixel_id, data.access_token, data.page_id || '']
    );
  }
  return getPixels();
}

async function deletePixel(id) {
  await pool.query('DELETE FROM pixels WHERE id = $1', [id]);
  return getPixels();
}

async function getPixelById(id) {
  const res = await pool.query('SELECT * FROM pixels WHERE id = $1', [id]);
  return res.rows[0] || null;
}

// ─── EVENTS ───────────────────────────────────────────────────────────────────

async function insertEvent(data) {
  await pool.query(
    'INSERT INTO events (pixel_id, pixel_name, name, phone, email, gender, cep, value, status, error_msg, source) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)',
    [data.pixel_id || '', data.pixel_name || '', data.name || '', data.phone || '', data.email || '', data.gender || '', data.cep || '', parseFloat(data.value) || 0, data.status, data.error_msg || null, data.source || 'manual']
  );
}

async function getEvents(opts) {
  const page   = opts.page || 1;
  const limit  = 50;
  const offset = (page - 1) * limit;

  let where    = 'WHERE 1=1';
  const values = [];
  let i = 1;
  if (opts.status   && opts.status   !== 'all') { where += ' AND status = $'   + i++; values.push(opts.status); }
  if (opts.pixel_id && opts.pixel_id !== 'all') { where += ' AND pixel_id = $' + i++; values.push(opts.pixel_id); }

  const countRes = await pool.query('SELECT COUNT(*) as total FROM events ' + where, values);
  const total    = parseInt(countRes.rows[0].total);

  values.push(limit); values.push(offset);
  const rows = await pool.query(
    'SELECT *, to_char(created_at AT TIME ZONE \'America/Sao_Paulo\', \'DD/MM/YYYY, HH24:MI:SS\') as created_at_br FROM events ' + where + ' ORDER BY id DESC LIMIT $' + i++ + ' OFFSET $' + i++,
    values
  );

  return {
    total: total,
    page:  page,
    rows:  rows.rows.map(function(r) {
      return {
        id:         r.id,
        created_at: r.created_at_br,
        pixel_id:   r.pixel_id,
        pixel_name: r.pixel_name,
        name:       r.name,
        phone:      r.phone,
        email:      r.email,
        gender:     r.gender,
        cep:        r.cep,
        value:      r.value,
        status:     r.status,
        error_msg:  r.error_msg,
        source:     r.source
      };
    })
  };
}

async function getStats(pixel_id) {
  let where    = '';
  const values = [];
  if (pixel_id && pixel_id !== 'all') { where = 'WHERE pixel_id = $1'; values.push(pixel_id); }

  const todayWhere = where
    ? where + ' AND DATE(created_at AT TIME ZONE \'America/Sao_Paulo\') = CURRENT_DATE'
    : 'WHERE DATE(created_at AT TIME ZONE \'America/Sao_Paulo\') = CURRENT_DATE';
  const last30Where = where
    ? where + ' AND created_at >= NOW() - INTERVAL \'30 days\''
    : 'WHERE created_at >= NOW() - INTERVAL \'30 days\'';

  const todayRes  = await pool.query('SELECT COUNT(*) as total, COALESCE(SUM(CASE WHEN status=\'sent\' THEN value ELSE 0 END),0) as value, COUNT(CASE WHEN status=\'error\' THEN 1 END) as errors FROM events ' + todayWhere, values);
  const last30Res = await pool.query('SELECT COUNT(*) as total, COUNT(CASE WHEN status=\'sent\' THEN 1 END) as sent FROM events ' + last30Where, values);

  const today  = todayRes.rows[0];
  const last30 = last30Res.rows[0];
  const matchRate = parseInt(last30.total) > 0
    ? ((parseInt(last30.sent) / parseInt(last30.total)) * 100).toFixed(1)
    : '0.0';

  const chart = [];
  for (let d = 6; d >= 0; d--) {
    const dayWhere = where
      ? where + ' AND DATE(created_at AT TIME ZONE \'America/Sao_Paulo\') = CURRENT_DATE - INTERVAL \'' + d + ' days\''
      : 'WHERE DATE(created_at AT TIME ZONE \'America/Sao_Paulo\') = CURRENT_DATE - INTERVAL \'' + d + ' days\'';
    const dayRes = await pool.query('SELECT COUNT(CASE WHEN status=\'sent\' THEN 1 END) as sent, COUNT(CASE WHEN status=\'error\' THEN 1 END) as errors FROM events ' + dayWhere, values);
    const dt = new Date();
    dt.setDate(dt.getDate() - d);
    chart.push({
      day:    ('0' + dt.getDate()).slice(-2) + '/' + ('0' + (dt.getMonth() + 1)).slice(-2),
      sent:   parseInt(dayRes.rows[0].sent),
      errors: parseInt(dayRes.rows[0].errors)
    });
  }

  return {
    eventsToday: parseInt(today.total),
    valueToday:  parseFloat(today.value),
    errorsToday: parseInt(today.errors),
    matchRate:   matchRate,
    sent30:      parseInt(last30.sent),
    last30:      parseInt(last30.total),
    chart:       chart
  };
}

// ─── CTWA CLID ────────────────────────────────────────────────────────────────

function normalizePhoneForCtwa(phone) {
  let p = String(phone || '').replace(/\D/g, '');
  if (!p.startsWith('55')) p = '55' + p;
  return p;
}

async function saveCtwaClid(phone, ctwa_clid) {
  const p = normalizePhoneForCtwa(phone);
  await pool.query(`
    INSERT INTO ctwa_leads (phone, ctwa_clid, updated_at)
    VALUES ($1, $2, NOW())
    ON CONFLICT (phone)
    DO UPDATE SET ctwa_clid = EXCLUDED.ctwa_clid, updated_at = NOW()
  `, [p, ctwa_clid]);
}

async function getCtwaClid(phone) {
  const p = normalizePhoneForCtwa(phone);

  function variantes(num) {
    const v = new Set();
    v.add(num);
    const local = num.startsWith('55') ? num.slice(2) : num;
    const com55 = num.startsWith('55') ? num : '55' + num;
    v.add(local);
    v.add(com55);
    const clean = num.replace(/^\$/, '');
    v.add(clean);
    v.add('55' + clean.replace(/^55/, ''));
    const localClean = clean.startsWith('55') ? clean.slice(2) : clean;
    if (localClean.length === 11 && localClean[2] === '9') {
      const sem9 = localClean.slice(0, 2) + localClean.slice(3);
      v.add(sem9);
      v.add('55' + sem9);
    } else if (localClean.length === 10) {
      const com9 = localClean.slice(0, 2) + '9' + localClean.slice(2);
      v.add(com9);
      v.add('55' + com9);
    }
    return Array.from(v).filter(Boolean);
  }

  const todos = variantes(p);
  const placeholders = todos.map((_, i) => '$' + (i + 1)).join(', ');
  const res = await pool.query(
    'SELECT ctwa_clid FROM ctwa_leads WHERE phone IN (' + placeholders + ') ORDER BY updated_at DESC LIMIT 1',
    todos
  );
  return res.rows.length > 0 ? res.rows[0].ctwa_clid : null;
}

async function getCtwaLeads(opts) {
  const limit  = opts && opts.limit  ? parseInt(opts.limit)  : 50;
  const offset = opts && opts.offset ? parseInt(opts.offset) : 0;
  const res = await pool.query(
    `SELECT id, phone, ctwa_clid,
            to_char(created_at AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY HH24:MI:SS') as created_at_br,
            to_char(updated_at AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY HH24:MI:SS') as updated_at_br
     FROM ctwa_leads
     ORDER BY updated_at DESC
     LIMIT $1 OFFSET $2`,
    [limit, offset]
  );
  const count = await pool.query('SELECT COUNT(*) FROM ctwa_leads');
  return { total: parseInt(count.rows[0].count), rows: res.rows };
}

module.exports = {
  getConfig, saveConfig,
  getPixels, savePixel, deletePixel, getPixelById,
  insertEvent, getEvents, getStats,
  saveCtwaClid, getCtwaClid, getCtwaLeads
};
