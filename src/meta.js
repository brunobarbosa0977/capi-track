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

  // ctwa_clid — enviado em claro (não hashear) conforme spec do Meta
  if (ctwa_clid) {
    userData.ctwa_clid = ctwa_clid;
  }

  // ── evento ────────────────────────────────────────────────────────────────
  const eventId = crypto.randomBytes(16).toString('hex');

  const event = {
    event_name:        'Purchase',
    event_time:        Math.floor(Date.now() / 1000),
    event_id:          eventId,
    action_source:     'business_messaging',
    messaging_channel: 'whatsapp',
    user_data:         userData,
    custom_data:       { currency: 'BRL', value: parseFloat(value) }
  };

  // page_id no nível do evento — necessário para associação com página
  if (page_id) {
    event.page_id = page_id;
  }

  // messaging_outcome_data — obrigatório para Dataset de Mensagens
  // outcome_type pode ser: 'purchase', 'lead', 'other'
  event.messaging_outcome_data = {
    outcome_type: 'purchase'
  };

  const payload = { data: [event] };

  // Log do payload para debug
  console.log('[Meta CAPI] Payload:', JSON.stringify({
    pixel_id: pixel_id,
    event_name: event.event_name,
    action_source: event.action_source,
    messaging_channel: event.messaging_channel,
    has_ctwa_clid: !!ctwa_clid,
    has_page_id: !!page_id,
    messaging_outcome_data: event.messaging_outcome_data,
    user_data_keys: Object.keys(userData)
  }));

  try {
    const url = 'https://graph.facebook.com/v19.0/' + pixel_id + '/events?access_token=' + access_token;
    const response = await fetch(url, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload)
    });

    const result = await response.json();

    if (result.error) {
      console.log('[Meta CAPI] Erro raw:', JSON.stringify(result.error));
      return {
        success:   false,
        error:     result.error.message + ' (code: ' + result.error.code + ', subcode: ' + (result.error.error_subcode || 'n/a') + ')',
        raw_error: result.error
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
