const fetch = require('node-fetch');
const crypto = require('crypto');

function hash(value) {
  if (!value) return null;
  return crypto.createHash('sha256').update(value.toString().trim().toLowerCase()).digest('hex');
}

function normalizePhone(phone) {
  let p = phone.toString().replace(/\D/g, '');
  if (!p.startsWith('55')) p = '55' + p;
  return p;
}

async function sendPurchase(cfg, { name, phone, email, value }) {
  const { pixel_id, access_token } = cfg;
  const userData = {};
  if (phone) userData.ph = [hash(normalizePhone(phone))];
  if (email) userData.em = [hash(email)];
  if (name) {
    const parts = name.trim().split(' ');
    userData.fn = [hash(parts[0])];
    if (parts.length > 1) userData.ln = [hash(parts.slice(1).join(' '))];
  }
  userData.country = ['br'];

  const eventId = crypto.randomBytes(16).toString('hex');

  const payload = {
    data: [{
      event_name: 'Purchase',
      event_time: Math.floor(Date.now() / 1000),
      event_id: eventId,
      action_source: 'website',
      user_data: userData,
      custom_data: {
        currency: 'BRL',
        value: parseFloat(value)
      }
    }]
  };

  console.log('=== META CAPI REQUEST ===');
  console.log('Pixel ID:', pixel_id);
  console.log('Payload:', JSON.stringify(payload, null, 2));

  try {
    const url = `https://graph.facebook.com/v19.0/${pixel_id}/events?access_token=${access_token}`;
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    const data = await response.json();
    console.log('=== META CAPI RESPONSE ===');
    console.log(JSON.stringify(data, null, 2));

    if (data.error) return { 
      success: false, 
      error: `${data.error.message} (code: ${data.error.code}, subcode: ${data.error.error_subcode || 'n/a'})`,
      detail: data.error
    };
    return { success: true, events_received: data.events_received, fbtrace_id: data.fbtrace_id };
  } catch (err) {
    console.log('=== META CAPI ERROR ===', err.message);
    return { success: false, error: err.message };
  }
}

module.exports = { sendPurchase };
