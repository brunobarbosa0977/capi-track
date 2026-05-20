// ============================================================
// src/kwai.js — Módulo Kwai Tracker para o Infinity Track
// Adiciona ao server.js existente via: require('./kwai')(app, pool)
// ============================================================

const fetch = require('node-fetch');
const { v4: uuidv4 } = require('uuid');

const KWAI_PIXELS = [
  '304883249383318',
  '308803017463304'
];

const WHATSAPP_NUMBER = '5562981585394';
const WHATSAPP_TEXT   = encodeURIComponent('Olá! Gostaria de saber mais sobre o tratamento.');

// ── Disparo server-side para o Kwai ────────────────────────
async function dispararEvento(eventName, clickId, pixelId, extra = {}) {
  const eventId  = uuidv4();
  const userData = {};
  if (clickId)      userData.click_id = clickId;
  if (extra.phone)  userData.ph        = extra.phone.replace(/\D/g, '');

  const payload = {
    pixel_id: pixelId,
    events: [{
      event_name: eventName,
      event_id:   eventId,
      event_time: Math.floor(Date.now() / 1000),
      user_data:  userData,
      custom_data: {
        value:        extra.value    || 297,
        currency:     extra.currency || 'BRL',
        content_name: 'Glivia',
        content_id:   'glivia'
      }
    }]
  };

  try {
    const res = await fetch(
      `https://s21-def.ap4r.com/rest/n/v1/pixel/batch?sdkid=${pixelId}`,
      {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(payload),
        timeout: 6000
      }
    );
    const body = await res.text();
    console.log(`[Kwai ${pixelId}] ${eventName} → ${res.status}: ${body}`);
    return { ok: res.ok, status: res.status, body, event_id: eventId };
  } catch (err) {
    console.error(`[Kwai ${pixelId}] Erro:`, err.message);
    return { ok: false, error: err.message, event_id: eventId };
  }
}

async function dispararTodos(eventName, clickId, extra = {}) {
  return Promise.all(KWAI_PIXELS.map(pid => dispararEvento(eventName, clickId, pid, extra)));
}

// ── Inicialização da tabela ─────────────────────────────────
async function initKwaiDB(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS kwai_leads (
      id           SERIAL PRIMARY KEY,
      phone        VARCHAR(30),
      click_id     VARCHAR(255),
      campaign_id  VARCHAR(100),
      status       VARCHAR(20)  DEFAULT 'lead',
      created_at   TIMESTAMP    DEFAULT NOW(),
      purchased_at TIMESTAMP,
      purchase_value DECIMAL(10,2),
      kwai_results JSONB
    );
    CREATE INDEX IF NOT EXISTS idx_kwai_phone    ON kwai_leads(phone);
    CREATE INDEX IF NOT EXISTS idx_kwai_click_id ON kwai_leads(click_id);
    CREATE INDEX IF NOT EXISTS idx_kwai_status   ON kwai_leads(status);
  `);
  console.log('[Kwai] Tabela kwai_leads pronta');
}

// ── Rotas ───────────────────────────────────────────────────
module.exports = function registerKwaiRoutes(app, pool, authMiddleware) {

  initKwaiDB(pool).catch(console.error);

  // Redirect do botão WhatsApp — captura o lead
  app.get('/kwai/ir', async (req, res) => {
    const { cid, campaign_id } = req.query;

    if (cid) {
      try {
        await pool.query(
          `INSERT INTO kwai_leads (click_id, campaign_id, status) VALUES ($1, $2, 'lead')`,
          [cid, campaign_id || null]
        );
        console.log(`[Kwai Lead] click_id=${cid}`);
      } catch (err) {
        console.error('[Kwai Lead] Erro ao salvar:', err.message);
      }
    }

    res.redirect(302,
      `https://api.whatsapp.com/send/?phone=${WHATSAPP_NUMBER}&text=${WHATSAPP_TEXT}&type=phone_number&app_absent=0`
    );
  });

  // Buscar lead por número
  app.get('/api/kwai/lead/:phone', authMiddleware, async (req, res) => {
    try {
      const phone = req.params.phone.replace(/\D/g, '');
      const result = await pool.query(
        `SELECT * FROM kwai_leads WHERE phone LIKE $1 OR click_id LIKE $1 ORDER BY created_at DESC LIMIT 10`,
        [`%${phone}%`]
      );
      res.json({ leads: result.rows });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Atualizar número do lead (quando o closer identifica quem é)
  app.post('/api/kwai/lead/:id/phone', authMiddleware, async (req, res) => {
    try {
      const { phone } = req.body;
      await pool.query(
        `UPDATE kwai_leads SET phone = $1 WHERE id = $2`,
        [phone.replace(/\D/g, ''), req.params.id]
      );
      res.json({ ok: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Disparar Purchase
  app.post('/api/kwai/purchase', authMiddleware, async (req, res) => {
    const { lead_id, phone, value } = req.body;

    try {
      let lead;

      if (lead_id) {
        const r = await pool.query('SELECT * FROM kwai_leads WHERE id = $1', [lead_id]);
        lead = r.rows[0];
      } else if (phone) {
        const clean = phone.replace(/\D/g, '');
        const r = await pool.query(
          `SELECT * FROM kwai_leads WHERE phone LIKE $1 ORDER BY created_at DESC LIMIT 1`,
          [`%${clean}%`]
        );
        lead = r.rows[0];
      }

      if (!lead) return res.status(404).json({ error: 'Lead não encontrado' });
      if (lead.status === 'purchased') return res.status(400).json({ error: 'Purchase já disparado para este lead' });

      const results = await dispararTodos('PURCHASE', lead.click_id, {
        phone: lead.phone,
        value: parseFloat(value) || 297
      });

      await pool.query(
        `UPDATE kwai_leads SET status = 'purchased', purchased_at = NOW(), purchase_value = $1, kwai_results = $2 WHERE id = $3`,
        [parseFloat(value) || 297, JSON.stringify(results), lead.id]
      );

      res.json({ success: true, lead, results });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Listar leads
  app.get('/api/kwai/leads', authMiddleware, async (req, res) => {
    try {
      const { status, page = 1 } = req.query;
      const limit  = 50;
      const offset = (page - 1) * limit;
      let   query  = 'SELECT * FROM kwai_leads';
      const params = [];
      if (status) { query += ' WHERE status = $1'; params.push(status); }
      query += ` ORDER BY created_at DESC LIMIT ${limit} OFFSET ${offset}`;
      const rows  = await pool.query(query, params);
      const count = await pool.query(
        status ? 'SELECT COUNT(*) FROM kwai_leads WHERE status = $1' : 'SELECT COUNT(*) FROM kwai_leads',
        status ? [status] : []
      );
      res.json({ leads: rows.rows, total: parseInt(count.rows[0].count), page: parseInt(page) });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Stats do Kwai
  app.get('/api/kwai/stats', authMiddleware, async (req, res) => {
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
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  console.log('[Kwai] Rotas registradas: /kwai/ir, /api/kwai/*');
};

