const fetch  = require('node-fetch');
const crypto = require('crypto');

function hash(value) {
  if (!value) return null;
  return crypto.createHash('sha256').update(value.toString().trim().toLowerCase()).digest('hex');
}

function normalizePhone(phone) {
  let p = phone.toString().replace(/\D/g, '');
  if (p.indexOf('55') !== 0) p = '55' + p;
  return p;
}

function normalizeCep(cep) {
  return cep.toString().replace(/\D/g, '').padStart(8, '0');
}

function normalizeGender(gender) {
  if (!gender) return null;
  const g = gender.toString().trim().toLowerCase();
  if (g === 'm' || g === 'masculino' || g === 'male')   return 'm';
  if (g === 'f' || g === 'feminino'  || g === 'female') return 'f';
  return null;
}

/**
 * Dispara evento Purchase via Meta CAPI.
 *
 * @param {object} cfg  - { pixel_id, access_token, page_id }
 * @param {object} data - campos do lead
 * @param {string} [data.ctwa_clid] - token de sessão do WhatsApp Ads (opcional)
 */
async function sendPurchase(cfg, data) {
  const { name, phone, email, value, gender, cep, city, state, ctwa_clid } = data;
  const { pixel_id, access_token, page_id } = cfg;

  // ── user_data ──────────────────────────────────────────────────────────────
  const userData = {};

  if (phone) userData.ph = [hash(normalizePhone(phone))];
  if (email) userData.em = [hash(email)];

  if (name) {
    const parts = name.trim().split(' ');
    userData.fn = [hash(parts[0])];
    if (parts.length > 1) userData.ln = [hash(parts.slice(1).join(' '))];
  }

  if (cep)   userData.zp = [hash(normalizeCep(cep))];
  if (city)  userData.ct = [hash(city.toString().trim().toLowerCase())];
  if (state) userData.st = [hash(state.toString().trim().toLowerCase())];

  if (gender) {
    const g = normalizeGender(gender);
    if (g) userData.ge = [hash(g)];
  }

  userData.country = [hash('br')];

  // ── ctwa_clid — obrigatório para campanhas WhatsApp Ads ───────────────────
  // Enviado em claro (não hashear) conforme especificação da Meta
  // Deve vir acompanhado do page_id da página Facebook vinculada ao WhatsApp
  if (ctwa_clid) {
    userData.ctwa_clid = ctwa_clid;
    if (page_id) userData.page_id = page_id;
  }

  // ── payload ────────────────────────────────────────────────────────────────
  const eventId = crypto.randomBytes(16).toString('hex');

  const payload = {
    data: [{
      event_name:    'Purchase',
      event_time:    Math.floor(Date.now() / 1000),
      event_id:      eventId,
      // CORREÇÃO: action_source deve ser 'business_messaging' para campanhas
      // com destino WhatsApp — não 'website'. Isso permite que o Meta associe
      // corretamente o evento à conversa de WhatsApp e ao ctwa_clid.
      action_source: 'business_messaging',
      user_data:     userData,
      custom_data:   { currency: 'BRL', value: parseFloat(value) }
    }]
  };

  try {
    const url = 'https://graph.facebook.com/v19.0/' + pixel_id + '/events?access_token=' + access_token;
    const response = await fetch(url, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload)
    });

    const result = await response.json();

    if (result.error) {
      return {
        success: false,
        error: result.error.message + ' (code: ' + result.error.code + ', subcode: ' + (result.error.error_subcode || 'n/a') + ')'
      };
    }

    return {
      success:         true,
      events_received: result.events_received,
      fbtrace_id:      result.fbtrace_id,
      ctwa_clid_used:  !!ctwa_clid
    };

  } catch (err) {
    return { success: false, error: err.message };
  }
}

module.exports = { sendPurchase };
