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
 * cfg.waba_id  → Dataset de Mensagens vinculado ao WABA (action_source: business_messaging)
 * cfg.page_id  → usado como page_id no evento (opcional)
 * sem waba_id  → Pixel de site normal (action_source: website)
 */
async function sendPurchase(cfg, data) {
  const { name, phone, email, value, gender, cep, city, state, ctwa_clid } = data;
  const { pixel_id, access_token, page_id } = cfg;

  // Detecta se é Dataset de Mensagens (tem waba_id ou page_id preenchido)
  // page_id no Infinity Track pode ser usado para guardar o WABA ID
  const isMessagingDataset = !!(ctwa_clid || (page_id && page_id.length > 5));

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

  // ctwa_clid — apenas para Dataset de Mensagens, enviado em claro
  if (ctwa_clid) {
    userData.ctwa_clid = ctwa_clid;
  }

  // ── evento ────────────────────────────────────────────────────────────────
  const eventId = crypto.randomBytes(16).toString('hex');

  const event = {
    event_name:  'Purchase',
    event_time:  Math.floor(Date.now() / 1000),
    event_id:    eventId,
    user_data:   userData,
    custom_data: { currency: 'BRL', value: parseFloat(value) }
  };

  if (isMessagingDataset) {
    // ── Modo Dataset de Mensagens (WABA) ────────────────────────────────────
    event.action_source     = 'business_messaging';
    event.messaging_channel = 'whatsapp';
    event.messaging_outcome_data = { outcome_type: 'purchase' };
    if (page_id) event.page_id = page_id;
  } else {
    // ── Modo Pixel de Site (MASTER COMPRAS, etc) ────────────────────────────
    event.action_source = 'website';
  }

  const payload = { data: [event] };

  // Log para debug
  console.log('[Meta CAPI] pixel=' + pixel_id +
    ' mode=' + (isMessagingDataset ? 'MESSAGING' : 'WEBSITE') +
    ' ctwa=' + (ctwa_clid ? 'SIM' : 'NAO') +
    ' page_id=' + (page_id || 'NAO'));

  try {
    const url = 'https://graph.facebook.com/v22.0/' + pixel_id + '/events?access_token=' + access_token;
    const response = await fetch(url, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload)
    });

    const result = await response.json();

    if (result.error) {
      console.log('[Meta CAPI] Erro: ' + JSON.stringify(result.error));
      return {
        success:   false,
        error:     result.error.message + ' (code: ' + result.error.code + ', subcode: ' + (result.error.error_subcode || 'n/a') + ')',
        raw_error: result.error
      };
    }

    console.log('[Meta CAPI] Sucesso fbtrace=' + result.fbtrace_id);
    return {
      success:         true,
      events_received: result.events_received,
      fbtrace_id:      result.fbtrace_id,
      mode:            isMessagingDataset ? 'messaging' : 'website',
      ctwa_clid_used:  !!ctwa_clid
    };

  } catch (err) {
    return { success: false, error: err.message };
  }
}

module.exports = { sendPurchase };
