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
  `);
  console.log('Banco de dados inicializado');
}

init().catch(function(err) { console.error('Erro ao inicializar banco:', err.message); });

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
    if (cfg.password !== undefined) { fields.push('password = $' + i++); values.push(cfg.password); }
    if (cfg.auth_token !== undefined) { fields.push('auth_token = $' + i++); values.push(cfg.auth_token); }
    if (cfg.webhook_token !== undefined) { fields.push('webhook_token = $' + i++); values.push(cfg.webhook_token); }
    if (fields.length > 0) {
      await pool.query('UPDATE config SET ' + fields.join(', ') + ' WHERE id = 1', values);
    }
  }
}

async function getPixels() {
  const res = await pool.query('SELECT * FROM pixels ORDER BY created_at ASC');
  return res.rows;
}

async function savePixel(data) {
  if (data.id) {
    const exists = await pool.query('SELECT id FROM pixels WHERE id = $1', [data.id]);
    if (exists.rows.length > 0) {
      await pool.query(
        'UPDATE pixels SET name=$1, pixel_id=$2, access_token=$3 WHERE id=$4',
        [data.name, data.pixel_id, data.access_token, data.id]
      );
    } else {
      await pool.query(
        'INSERT INTO pixels (id, name, pixel_id, access_token) VALUES ($1,$2,$3,$4)',
        [data.id, data.name, data.pixel_id, data.access_token]
      );
    }
  } else {
    const newId = Date.now().toString();
    await pool.query(
      'INSERT INTO pixels (id, name, pixel_id, access_token) VALUES ($1,$2,$3,$4)',
      [newId, data.name, data.pixel_id, data.access_token]
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

async function insertEvent(data) {
  await pool.query(
    'INSERT INTO events (pixel_id, pixel_name, name, phone, email, gender, cep, value, status, error_msg, source) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)',
    [data.pixel_id || '', data.pixel_name || '', data.name || '', data.phone || '', data.email || '', data.gender || '', data.cep || '', parseFloat(data.value) || 0, data.status, data.error_msg || null, data.source || 'manual']
  );
}

async function getEvents(opts) {
  const page = opts.page || 1;
  const status = opts.status;
  const pixel_id = opts.pixel_id;
  const limit = 50;
  const offset = (page - 1) * limit;

  let where = 'WHERE 1=1';
  const values = [];
  let i = 1;
  if (status && status !== 'all') { where += ' AND status = $' + i++; values.push(status); }
  if (pixel_id && pixel_id !== 'all') { where += ' AND pixel_id = $' + i++; values.push(pixel_id); }

  const countRes = await pool.query('SELECT COUNT(*) as total FROM events ' + where, values);
  const total = parseInt(countRes.rows[0].total);

  values.push(limit); values.push(offset);
  const rows = await pool.query(
    'SELECT *, to_char(created_at AT TIME ZONE \'America/Sao_Paulo\', \'DD/MM/YYYY, HH24:MI:SS\') as created_at_br FROM events ' + where + ' ORDER BY id DESC LIMIT $' + i++ + ' OFFSET $' + i++,
    values
  );

  return {
    total: total,
    page: page,
    rows: rows.rows.map(function(r) {
      return {
        id: r.id,
        created_at: r.created_at_br,
        pixel_id: r.pixel_id,
        pixel_name: r.pixel_name,
        name: r.name,
        phone: r.phone,
        email: r.email,
        gender: r.gender,
        cep: r.cep,
        value: r.value,
        status: r.status,
        error_msg: r.error_msg,
        source: r.source
      };
    })
  };
}

async function getStats(pixel_id) {
  let where = '';
  const values = [];
  if (pixel_id && pixel_id !== 'all') { where = 'WHERE pixel_id = $1'; values.push(pixel_id); }

  const todayWhere = where ? where + ' AND DATE(created_at AT TIME ZONE \'America/Sao_Paulo\') = CURRENT_DATE' : 'WHERE DATE(created_at AT TIME ZONE \'America/Sao_Paulo\') = CURRENT_DATE';
  const last30Where = where ? where + ' AND created_at >= NOW() - INTERVAL \'30 days\'' : 'WHERE created_at >= NOW() - INTERVAL \'30 days\'';

  const todayRes = await pool.query('SELECT COUNT(*) as total, COALESCE(SUM(CASE WHEN status=\'sent\' THEN value ELSE 0 END),0) as value, COUNT(CASE WHEN status=\'error\' THEN 1 END) as errors FROM events ' + todayWhere, values);
  const last30Res = await pool.query('SELECT COUNT(*) as total, COUNT(CASE WHEN status=\'sent\' THEN 1 END) as sent FROM events ' + last30Where, values);

  const today = todayRes.rows[0];
  const last30 = last30Res.rows[0];
  const matchRate = parseInt(last30.total) > 0 ? ((parseInt(last30.sent) / parseInt(last30.total)) * 100).toFixed(1) : '0.0';

  const chart = [];
  for (let i = 6; i >= 0; i--) {
    const dayWhere = where ? where + ' AND DATE(created_at AT TIME ZONE \'America/Sao_Paulo\') = CURRENT_DATE - INTERVAL \'' + i + ' days\'' : 'WHERE DATE(created_at AT TIME ZONE \'America/Sao_Paulo\') = CURRENT_DATE - INTERVAL \'' + i + ' days\'';
    const dayRes = await pool.query('SELECT COUNT(CASE WHEN status=\'sent\' THEN 1 END) as sent, COUNT(CASE WHEN status=\'error\' THEN 1 END) as errors FROM events ' + dayWhere, values);
    const d = new Date();
    d.setDate(d.getDate() - i);
    chart.push({
      day: ('0' + d.getDate()).slice(-2) + '/' + ('0' + (d.getMonth() + 1)).slice(-2),
      sent: parseInt(dayRes.rows[0].sent),
      errors: parseInt(dayRes.rows[0].errors)
    });
  }

  return {
    eventsToday: parseInt(today.total),
    valueToday: parseFloat(today.value),
    errorsToday: parseInt(today.errors),
    matchRate: matchRate,
    sent30: parseInt(last30.sent),
    last30: parseInt(last30.total),
    chart: chart
  };
}

module.exports = { getConfig, saveConfig, getPixels, savePixel, deletePixel, getPixelById, insertEvent, getEvents, getStats };
