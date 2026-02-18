/**
 * =====================================================
 * TALKO CRM - Firebase Functions
 * Telegram Bot + IP Telephony + SendPulse + Forms
 * =====================================================
 */

const functions = require('firebase-functions');
const admin = require('firebase-admin');

admin.initializeApp();
const db = admin.firestore();

// Telegram Bot Config
const TELEGRAM_BOT_TOKEN = '8347933211:AAHzfNNo-v-Z-4rdpf11-KAf4r2dfAIXzSg';
const TELEGRAM_API = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}`;

// =====================================================
// UTILITY: Normalize phone to E.164
// =====================================================
function normalizePhone(phone) {
  if (!phone) return null;
  let clean = String(phone).replace(/[\s\-\(\)\+]/g, '');
  if (clean.startsWith('0') && clean.length === 10) clean = '38' + clean;
  if (clean.startsWith('80') && clean.length === 11) clean = '3' + clean;
  if (!clean.startsWith('+')) clean = '+' + clean;
  return clean;
}

// =====================================================
// UTILITY: Find org by webhook key
// =====================================================
async function findOrgByWebhookKey(webhookKey) {
  if (!webhookKey) return null;
  const orgsSnapshot = await db.collection('organizations').get();
  for (const doc of orgsSnapshot.docs) {
    const data = doc.data();
    if (data.webhook?.key === webhookKey) return { orgId: doc.id, orgData: data };
    if (data.telephony?.webhookKey === webhookKey) return { orgId: doc.id, orgData: data };
    if (data.integrations?.sendpulse?.webhookKey === webhookKey) return { orgId: doc.id, orgData: data };
  }
  return null;
}

// =====================================================
// UTILITY: Find lead by phone/email/chatId
// =====================================================
async function findLead(orgId, { phone, email, telegramChatId, viberChatId, chatId }) {
  const leadsRef = db.collection('organizations').doc(orgId).collection('leads');

  if (phone) {
    const normalized = normalizePhone(phone);
    const snapshot = await leadsRef.get();
    for (const doc of snapshot.docs) {
      const data = doc.data();
      if (data.deleted) continue;
      if (normalizePhone(data.phone) === normalized) return { id: doc.id, ...data };
    }
  }
  if (email) {
    const snapshot = await leadsRef.where('email', '==', email.toLowerCase()).get();
    for (const doc of snapshot.docs) { const d = doc.data(); if (!d.deleted) return { id: doc.id, ...d }; }
  }
  if (telegramChatId) {
    const snapshot = await leadsRef.where('telegramChatId', '==', String(telegramChatId)).get();
    for (const doc of snapshot.docs) { const d = doc.data(); if (!d.deleted) return { id: doc.id, ...d }; }
  }
  if (viberChatId) {
    const snapshot = await leadsRef.where('viberChatId', '==', String(viberChatId)).get();
    for (const doc of snapshot.docs) { const d = doc.data(); if (!d.deleted) return { id: doc.id, ...d }; }
  }
  if (chatId) {
    const snapshot = await leadsRef.where('chatId', '==', String(chatId)).get();
    for (const doc of snapshot.docs) { const d = doc.data(); if (!d.deleted) return { id: doc.id, ...d }; }
  }
  return null;
}

// =====================================================
// UTILITY: Create new lead with auto-assignment
// =====================================================
async function createLead(orgId, orgData, leadData) {
  const leadsRef = db.collection('organizations').doc(orgId).collection('leads');
  const now = new Date().toISOString();
  const id = 'lead_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);

  let assignedTo = null;
  const mode = orgData?.settings?.assignmentMode || 'manual';
  if (mode === 'round-robin') {
    const queue = orgData?.settings?.roundRobinQueue || [];
    const idx = orgData?.settings?.roundRobinIndex || 0;
    if (queue.length > 0) {
      assignedTo = queue[idx % queue.length];
      await db.collection('organizations').doc(orgId).update({ 'settings.roundRobinIndex': (idx + 1) % queue.length });
    }
  }

  const lead = {
    id, status: 'new', funnelId: leadData.funnelId || 'default',
    assignedTo, assignedAt: assignedTo ? now : null,
    phone: leadData.phone || null, tg: leadData.tg || null,
    email: leadData.email || null, viber: leadData.viber || null,
    whatsapp: leadData.whatsapp || null, primaryChannel: leadData.channel || 'phone',
    telegramChatId: leadData.telegramChatId ? String(leadData.telegramChatId) : null,
    viberChatId: leadData.viberChatId ? String(leadData.viberChatId) : null,
    chatId: leadData.chatId ? String(leadData.chatId) : null,
    lastChannel: leadData.channel || null,
    biz: leadData.biz || leadData.name || null, source: leadData.source || 'webhook',
    created: now, createdAt: now, stageDate: now,
    nextDate: now.split('T')[0], nextTime: '10:00', nextAction: 'Подзвонити',
    calls: 0, sms: 0, noshow: 0, repeatCount: 0,
    finDep: null, finTotal: null,
    log: [{ type: 'system', text: `Лід створено автоматично (${leadData.source || 'webhook'})`, date: now }],
    deleted: false
  };
  await leadsRef.doc(id).set(lead);
  return lead;
}

// =====================================================
// UTILITY: Save message to messages subcollection
// =====================================================
async function saveMessage(orgId, messageData) {
  const id = 'msg_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  const message = { id, ...messageData, createdAt: new Date().toISOString() };
  await db.collection('organizations').doc(orgId).collection('messages').doc(id).set(message);
  return message;
}

// =====================================================
// UTILITY: Emit real-time event for CRM popup
// =====================================================
async function emitEvent(orgId, eventData) {
  const eventsRef = db.collection('organizations').doc(orgId).collection('callEvents');
  const id = 'evt_' + Date.now();
  await eventsRef.doc(id).set({ ...eventData, timestamp: new Date() });
  // Auto-cleanup after 60s
  setTimeout(async () => { try { await eventsRef.doc(id).delete(); } catch(e) {} }, 60000);
}

// =====================================================
// CORS helper
// =====================================================
function setCors(res) {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, x-api-key, Authorization');
}


// #####################################################
// ===== 1. TELEGRAM BOT WEBHOOK (existing) ============
// #####################################################

exports.telegramWebhook = functions.https.onRequest(async (req, res) => {
  try {
    if (req.method !== 'POST') {
      return res.status(200).send('Telegram Webhook Active');
    }

    const update = req.body;
    console.log('Telegram update:', JSON.stringify(update));

    if (update.message) {
      const chatId = update.message.chat.id;
      const text = update.message.text || '';
      const firstName = update.message.from.first_name || '';

      if (text.startsWith('/start')) {
        const parts = text.split(' ');
        const crmUserId = parts[1];
        if (crmUserId) {
          await linkTelegramToUser(crmUserId, chatId, firstName);
          await sendTelegramMessage(chatId,
            `✅ Вітаю, ${firstName}!\n\n` +
            `Ваш Telegram успішно підключено до TALKO CRM.\n\n` +
            `Тепер ви будете отримувати сповіщення про:\n` +
            `• 🆕 Нових лідів\n` +
            `• 📞 Завдання на сьогодні\n` +
            `• ⚠️ Прострочені задачі\n\n` +
            `Щоб відписатися — напишіть /stop`
          );
        } else {
          await sendTelegramMessage(chatId,
            `👋 Вітаю!\n\nЩоб підключити сповіщення, перейдіть в налаштування TALKO CRM і натисніть "Підключити Telegram".`
          );
        }
      } else if (text === '/stop') {
        await unlinkTelegram(chatId);
        await sendTelegramMessage(chatId, `🔕 Сповіщення вимкнено.\n\nЩоб підключити знову — перейдіть в налаштування TALKO CRM.`);
      } else if (text === '/status') {
        const status = await getTelegramStatus(chatId);
        await sendTelegramMessage(chatId, status);
      } else {
        await sendTelegramMessage(chatId, `🤖 TALKO CRM Bot\n\nДоступні команди:\n/status — перевірити підключення\n/stop — вимкнути сповіщення`);
      }
    }

    res.status(200).send('OK');
  } catch (error) {
    console.error('Webhook error:', error);
    res.status(200).send('OK');
  }
});


// #####################################################
// ===== 2. IP TELEPHONY WEBHOOK =======================
// #####################################################

exports.telephonyWebhook = functions.https.onRequest(async (req, res) => {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const apiKey = req.headers['x-api-key'] || req.query.key;
    const org = await findOrgByWebhookKey(apiKey);
    if (!org) return res.status(401).json({ error: 'Invalid API key' });

    const { orgId, orgData } = org;
    const provider = orgData.telephony?.provider || 'generic';

    // Normalize payload per provider
    const body = req.body;
    const callData = normalizeCallPayload(provider, body);
    if (!callData.phone) return res.status(400).json({ error: 'No phone number' });

    console.log(`Telephony [${orgId}] [${provider}]: ${callData.type} from ${callData.phone}`);

    // Find or create lead
    let lead = await findLead(orgId, { phone: callData.phone });
    if (!lead) {
      lead = await createLead(orgId, orgData, { phone: normalizePhone(callData.phone), source: 'telephony', channel: 'phone' });
      console.log(`Telephony: Created lead ${lead.id}`);
    }

    // Match manager by extension
    let managerId = null;
    if (callData.extension) {
      const extensions = orgData.telephony?.extensions || {};
      managerId = Object.keys(extensions).find(uid => extensions[uid] === callData.extension) || null;
    }

    // Save call record
    const callId = 'call_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6);
    await db.collection('organizations').doc(orgId).collection('calls').doc(callId).set({
      id: callId, leadId: lead.id, type: callData.type, phone: normalizePhone(callData.phone),
      duration: callData.duration, status: callData.status, recordUrl: callData.recordUrl,
      managerId, managerExtension: callData.extension, provider,
      providerCallId: callData.providerCallId,
      startedAt: callData.startedAt, endedAt: callData.endedAt,
      createdAt: new Date().toISOString()
    });

    // Update lead
    const updates = { calls: (lead.calls || 0) + 1 };
    if (callData.status === 'missed' || callData.status === 'no-answer') {
      updates.nextDate = new Date().toISOString().split('T')[0];
      updates.nextTime = '10:00';
      updates.nextAction = 'Передзвонити (пропущений)';
    }
    updates.log = [...(lead.log || []), {
      type: 'call',
      text: `${callData.type === 'incoming' ? 'Вхідний' : 'Вихідний'} (${callData.status}) ${callData.duration > 0 ? callData.duration + 'с' : ''}`,
      date: new Date().toISOString()
    }];
    await db.collection('organizations').doc(orgId).collection('leads').doc(lead.id).update(updates);

    // Real-time popup for incoming/missed
    if (callData.type === 'incoming') {
      await emitEvent(orgId, {
        type: callData.status === 'missed' ? 'missed_call' : 'incoming_call',
        leadId: lead.id, phone: callData.phone,
        leadName: lead.biz || lead.tg || lead.phone, managerId, callId
      });
    }

    // Notify via Telegram if missed
    if (callData.status === 'missed' && managerId) {
      const chatId = await getUserTelegramChatId(managerId);
      if (chatId) {
        await sendTelegramMessage(chatId,
          `📞 Пропущений дзвінок!\n\n` +
          `${lead.biz || ''} ${callData.phone}\n` +
          `🔗 <a href="https://talko-crm.vercel.app">Відкрити CRM</a>`
        );
      }
    }

    return res.status(200).json({ success: true, leadId: lead.id, callId });
  } catch (err) {
    console.error('Telephony error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

function normalizeCallPayload(provider, body) {
  switch (provider) {
    case 'binotel':
      return {
        type: body.callType === 'incoming' ? 'incoming' : 'outgoing',
        phone: body.externalNumber || body.src || body.dst,
        extension: body.internalNumber || body.extension,
        duration: parseInt(body.billsec || body.duration || 0),
        status: body.disposition === 'ANSWERED' ? 'answered' : body.disposition === 'BUSY' ? 'busy' : body.disposition === 'NO ANSWER' ? 'no-answer' : 'missed',
        recordUrl: body.pbxRecordLink || body.recordLink || null,
        providerCallId: body.generalCallID || body.callID || null,
        startedAt: body.startTime || new Date().toISOString(),
        endedAt: body.stopTime || null
      };
    case 'ringostat':
      return {
        type: body.direction === 'incoming' ? 'incoming' : 'outgoing',
        phone: body.caller_number || body.called_number,
        extension: body.employee_ext,
        duration: parseInt(body.duration || 0),
        status: body.status === 'answered' ? 'answered' : 'missed',
        recordUrl: body.record_url || null,
        providerCallId: body.call_id || null,
        startedAt: body.started_at || new Date().toISOString(),
        endedAt: body.ended_at || null
      };
    case 'stream':
      return {
        type: body.direction || 'incoming', phone: body.phone || body.caller,
        extension: body.ext || body.operator_ext, duration: parseInt(body.talk_time || body.duration || 0),
        status: body.answered ? 'answered' : 'missed', recordUrl: body.record || null,
        providerCallId: body.id || null, startedAt: body.start || new Date().toISOString(), endedAt: body.end || null
      };
    case 'phonet':
      return {
        type: body.type === 1 ? 'incoming' : 'outgoing', phone: body.otherLeg?.number || body.phone,
        extension: body.leg?.ext, duration: parseInt(body.duration || 0),
        status: body.status === 'answered' ? 'answered' : 'missed', recordUrl: body.recordUrl || null,
        providerCallId: body.uuid || null, startedAt: body.startedAt || new Date().toISOString(), endedAt: body.endedAt || null
      };
    default:
      return {
        type: body.type || body.direction || body.callType || 'incoming',
        phone: body.phone || body.number || body.caller || body.externalNumber,
        extension: body.extension || body.ext || body.internalNumber,
        duration: parseInt(body.duration || body.billsec || 0),
        status: body.status || body.disposition || 'unknown',
        recordUrl: body.recordUrl || body.record || null,
        providerCallId: body.callId || body.id || null,
        startedAt: body.startedAt || body.startTime || new Date().toISOString(),
        endedAt: body.endedAt || body.stopTime || null
      };
  }
}


// #####################################################
// ===== 3. SENDPULSE WEBHOOK ==========================
// #####################################################
// Receives messages from Telegram, Viber, WhatsApp,
// Instagram, Facebook via SendPulse chatbot webhook

exports.sendpulseWebhook = functions.https.onRequest(async (req, res) => {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const apiKey = req.headers['x-api-key'] || req.query.key;
    const org = await findOrgByWebhookKey(apiKey);
    if (!org) return res.status(401).json({ error: 'Invalid API key' });

    const { orgId, orgData } = org;
    let body = req.body;

    // SendPulse sends array - unwrap first element
    if (Array.isArray(body)) body = body[0] || {};

    // Log full body for debugging
    console.log('SendPulse raw body:', JSON.stringify(body).slice(0, 500));

    // SendPulse structure: { info: { message: { text, chat_id } }, service: "telegram", title: "incoming_message" }
    const info = body.info || {};
    const msg = info.message || body.message || {};

    // Determine channel
    const serviceRaw = body.service || body.channel || body.service_type || '';
    const serviceMap = { 1: 'telegram', 2: 'facebook', 3: 'viber', 4: 'whatsapp', 5: 'instagram' };
    let normalizedChannel = 'unknown';
    if (typeof serviceRaw === 'number') {
      normalizedChannel = serviceMap[serviceRaw] || 'unknown';
    } else {
      const ch = String(serviceRaw).toLowerCase();
      if (ch.includes('telegram')) normalizedChannel = 'telegram';
      else if (ch.includes('viber')) normalizedChannel = 'viber';
      else if (ch.includes('whatsapp')) normalizedChannel = 'whatsapp';
      else if (ch.includes('instagram')) normalizedChannel = 'instagram';
      else if (ch.includes('facebook') || ch.includes('fb')) normalizedChannel = 'facebook';
    }
    // Fallback: detect from bot info
    if (normalizedChannel === 'unknown') {
      if (body.bot_id || body.bot?.type === 'tg') normalizedChannel = 'telegram';
      else if (body.bot?.type === 'vb') normalizedChannel = 'viber';
      else if (body.bot?.type === 'wa') normalizedChannel = 'whatsapp';
      else if (body.bot?.type === 'ig') normalizedChannel = 'instagram';
      else if (body.bot?.type === 'fb') normalizedChannel = 'facebook';
    }

    // Contact info - from nested info.message or top-level
    const contact = body.contact || body.subscriber || info.contact || {};
    const chatId = String(msg.chat_id || contact.id || body.chat_id || body.contact_id || body.subscriber_id || '');
    const firstName = contact.first_name || contact.name || body.first_name || '';
    const lastName = contact.last_name || body.last_name || '';
    const name = [firstName, lastName].filter(Boolean).join(' ') || null;
    const phone = contact.phone || body.phone || null;
    const username = contact.username || body.username || null;
    const text = msg.text || body.text || (typeof body.message === 'string' ? body.message : null) || null;

    // Event type
    const eventType = body.title || body.event || body.type || 'incoming_message';
    if (!['incoming_message', 'new_message', 'message', 'text', 'subscribe'].includes(eventType) && !text) {
      return res.status(200).json({ success: true, skipped: true });
    }

    console.log(`SendPulse [${orgId}] [${normalizedChannel}]: ${name || chatId} — ${(text || '').slice(0, 50)}`);

    // Find or create lead
    const searchParams = { chatId };
    if (normalizedChannel === 'telegram') searchParams.telegramChatId = chatId;
    else if (normalizedChannel === 'viber') searchParams.viberChatId = chatId;
    if (contact.phone || phone) searchParams.phone = contact.phone || phone;
    if (contact.email) searchParams.email = contact.email;

    let lead = await findLead(orgId, searchParams);
    let isNewLead = false;

    if (!lead) {
      lead = await createLead(orgId, orgData, {
        biz: name, tg: normalizedChannel === 'telegram' ? (username || name) : null,
        phone: phone || null, email: contact.email || null,
        telegramChatId: normalizedChannel === 'telegram' ? chatId : null,
        viberChatId: normalizedChannel === 'viber' ? chatId : null,
        chatId: chatId,
        source: `sendpulse_${normalizedChannel}`, channel: normalizedChannel
      });
      isNewLead = true;
      console.log(`SendPulse: Created lead ${lead.id}`);
    } else {
      // Update chatId if missing
      const field = normalizedChannel === 'telegram' ? 'telegramChatId' : normalizedChannel === 'viber' ? 'viberChatId' : null;
      if (field && !lead[field]) {
        await db.collection('organizations').doc(orgId).collection('leads').doc(lead.id).update({ [field]: chatId });
      }
    }

    // Save message
    const message = await saveMessage(orgId, {
      leadId: lead.id, channel: normalizedChannel, direction: 'in', type: 'text',
      text, senderName: name, senderAvatar: contact.photo || null,
      chatId, externalMsgId: msg.message_id || msg.id || body.message_id || null,
      managerId: lead.assignedTo || null, read: false, source: 'sendpulse'
    });

    // Update lead log
    await db.collection('organizations').doc(orgId).collection('leads').doc(lead.id).update({
      log: admin.firestore.FieldValue.arrayUnion({
        type: 'message_in',
        text: `[${normalizedChannel}] ${name || 'Клієнт'}: ${(text || '').slice(0, 100)}`,
        date: new Date().toISOString()
      }),
      lastMessageAt: new Date().toISOString(),
      lastChannel: normalizedChannel
    });

    // Real-time event
    await emitEvent(orgId, {
      type: 'new_message', channel: normalizedChannel,
      leadId: lead.id, leadName: lead.biz || name || lead.tg || lead.phone,
      text: (text || '').slice(0, 100), senderName: name,
      managerId: lead.assignedTo, isNewLead
    });

    // Telegram notification to manager
    if (lead.assignedTo) {
      const managerChatId = await getUserTelegramChatId(lead.assignedTo);
      if (managerChatId) {
        await sendTelegramMessage(managerChatId,
          `💬 Нове повідомлення [${normalizedChannel}]\n\n` +
          `${name || 'Клієнт'}: ${(text || '').slice(0, 200)}\n\n` +
          `🔗 <a href="https://talko-crm.vercel.app">Відкрити CRM</a>`
        );
      }
    }

    return res.status(200).json({ success: true, leadId: lead.id, messageId: message.id, isNewLead });
  } catch (err) {
    console.error('SendPulse error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


// #####################################################
// ===== 4. SENDPULSE SEND (from CRM to customer) =====
// #####################################################

exports.sendpulseSend = functions.https.onRequest(async (req, res) => {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { orgId, leadId, channel, text, managerId } = req.body;
    if (!orgId || !leadId || !text) return res.status(400).json({ error: 'Missing orgId, leadId, or text' });

    // Get SendPulse config
    const orgDoc = await db.collection('organizations').doc(orgId).get();
    if (!orgDoc.exists) return res.status(404).json({ error: 'Org not found' });
    const orgData = orgDoc.data();
    const spConfig = orgData.integrations?.sendpulse;
    if (!spConfig?.apiId || !spConfig?.apiSecret) return res.status(400).json({ error: 'SendPulse not configured' });

    // Get lead
    const leadDoc = await db.collection('organizations').doc(orgId).collection('leads').doc(leadId).get();
    if (!leadDoc.exists) return res.status(404).json({ error: 'Lead not found' });
    const lead = leadDoc.data();

    // Determine channel & chatId
    const sendChannel = channel || lead.lastChannel || 'telegram';
    const chatId = sendChannel === 'telegram' ? (lead.telegramChatId || lead.chatId)
                 : sendChannel === 'viber' ? lead.viberChatId
                 : lead.telegramChatId || lead.viberChatId;
    if (!chatId) return res.status(400).json({ error: `No ${sendChannel} chatId` });

    // Get SendPulse token
    const tokenResp = await fetch('https://api.sendpulse.com/oauth/access_token', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ grant_type: 'client_credentials', client_id: spConfig.apiId, client_secret: spConfig.apiSecret })
    });
    const tokenData = await tokenResp.json();
    if (!tokenData.access_token) return res.status(500).json({ error: 'SendPulse auth failed' });

    // Send via SendPulse
    const endpoints = {
      telegram: 'https://api.sendpulse.com/telegram/contacts/send',
      viber: 'https://api.sendpulse.com/viber/contacts/send',
      whatsapp: 'https://api.sendpulse.com/whatsapp/contacts/send',
      facebook: 'https://api.sendpulse.com/facebook/contacts/send',
      instagram: 'https://api.sendpulse.com/instagram/contacts/send'
    };
    const sendResp = await fetch(endpoints[sendChannel] || endpoints.telegram, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${tokenData.access_token}` },
      body: JSON.stringify({ contact_id: chatId, messages: [{ type: 'text', text: { body: text } }] })
    });
    const sendResult = await sendResp.json();

    // Save outgoing message
    const message = await saveMessage(orgId, {
      leadId, channel: sendChannel, direction: 'out', type: 'text',
      text, managerId: managerId || null, chatId, read: true, source: 'crm'
    });

    // Update lead log
    await db.collection('organizations').doc(orgId).collection('leads').doc(leadId).update({
      log: admin.firestore.FieldValue.arrayUnion({
        type: 'message_out', text: `[${sendChannel}] Менеджер: ${text.slice(0, 100)}`, date: new Date().toISOString()
      }),
      lastMessageAt: new Date().toISOString()
    });

    return res.status(200).json({ success: true, messageId: message.id, sendPulseResult: sendResult });
  } catch (err) {
    console.error('SendPulse send error:', err);
    return res.status(500).json({ error: err.message });
  }
});


// #####################################################
// ===== 5. FORMS WEBHOOK ==============================
// #####################################################
// Accept leads from external forms & landing pages

exports.formsWebhook = functions.https.onRequest(async (req, res) => {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const apiKey = req.headers['x-api-key'] || req.query.key;
    const org = await findOrgByWebhookKey(apiKey);
    if (!org) return res.status(401).json({ error: 'Invalid API key' });

    const { orgId, orgData } = org;
    const body = req.body;

    // Map fields (support various naming)
    const phone = body.phone || body.tel || body.telephone || body.mobile || null;
    const email = body.email || body.mail || null;
    const name = body.name || body.fullName || body.full_name
              || [body.firstName || body.first_name, body.lastName || body.last_name].filter(Boolean).join(' ') || null;
    const message = body.message || body.comment || body.text || body.notes || null;
    const source = body.source || body.utm_source || body.form_name || 'form';

    if (!phone && !email) return res.status(400).json({ error: 'Phone or email required' });

    console.log(`Form [${orgId}]: ${name || 'Anon'} | ${phone || email} | ${source}`);

    // Check duplicate
    let lead = await findLead(orgId, { phone, email });
    let isNewLead = false;

    if (lead) {
      // Update existing — add log
      const logEntry = {
        type: 'form',
        text: `Повторна заявка (${source})${message ? ': ' + message.slice(0, 200) : ''}`,
        date: new Date().toISOString()
      };
      const updates = { log: [...(lead.log || []), logEntry] };

      // Reactivate if failed/frozen
      if (lead.status === 'failed' || lead.status === 'frozen') {
        updates.status = 'new';
        updates.nextDate = new Date().toISOString().split('T')[0];
        updates.nextTime = '10:00';
        updates.nextAction = 'Подзвонити (повторна заявка)';
        updates.stageDate = new Date().toISOString();
      }
      await db.collection('organizations').doc(orgId).collection('leads').doc(lead.id).update(updates);
    } else {
      lead = await createLead(orgId, orgData, {
        phone: phone ? normalizePhone(phone) : null,
        email: email ? email.toLowerCase() : null,
        biz: name, source, channel: phone ? 'phone' : 'email'
      });
      isNewLead = true;
      if (message || name) {
        const notes = [name ? `Ім'я: ${name}` : null, message ? `Коментар: ${message}` : null].filter(Boolean).join('\n');
        await db.collection('organizations').doc(orgId).collection('leads').doc(lead.id).update({ notes });
      }
    }

    // Real-time event
    await emitEvent(orgId, {
      type: 'new_form_lead', leadId: lead.id,
      leadName: name || phone || email, source, isNewLead,
      managerId: lead.assignedTo
    });

    // Telegram notification
    if (lead.assignedTo) {
      const chatId = await getUserTelegramChatId(lead.assignedTo);
      if (chatId) {
        await sendTelegramMessage(chatId,
          `📋 Нова заявка з форми!\n\n` +
          `${name || ''} ${phone || email || ''}\n` +
          `${message ? '💬 ' + message.slice(0, 200) : ''}\n\n` +
          `🔗 <a href="https://talko-crm.vercel.app">Відкрити CRM</a>`
        );
      }
    }

    return res.status(200).json({
      success: true, leadId: lead.id, isNewLead,
      message: orgData.formSuccessMessage || 'Дякуємо! Ми зв\'яжемось з вами найближчим часом.'
    });
  } catch (err) {
    console.error('Form error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


// #####################################################
// ===== TELEGRAM HELPERS (existing) ===================
// #####################################################

async function linkTelegramToUser(crmUserId, chatId, firstName) {
  try {
    const orgsSnapshot = await db.collection('organizations').get();
    for (const orgDoc of orgsSnapshot.docs) {
      const membersSnapshot = await orgDoc.ref.collection('members').where('userId', '==', crmUserId).get();
      if (!membersSnapshot.empty) {
        await membersSnapshot.docs[0].ref.update({
          telegramChatId: chatId.toString(), telegramFirstName: firstName,
          telegramLinkedAt: admin.firestore.FieldValue.serverTimestamp(), telegramNotifications: true
        });
        console.log(`Linked Telegram ${chatId} to user ${crmUserId} in org ${orgDoc.id}`);
        return true;
      }
    }
    await db.collection('users').doc(crmUserId).set({
      telegramChatId: chatId.toString(), telegramFirstName: firstName,
      telegramLinkedAt: admin.firestore.FieldValue.serverTimestamp(), telegramNotifications: true
    }, { merge: true });
    return true;
  } catch (error) { console.error('Link error:', error); return false; }
}

async function unlinkTelegram(chatId) {
  try {
    const chatIdStr = chatId.toString();
    const orgsSnapshot = await db.collection('organizations').get();
    for (const orgDoc of orgsSnapshot.docs) {
      const snap = await orgDoc.ref.collection('members').where('telegramChatId', '==', chatIdStr).get();
      for (const doc of snap.docs) await doc.ref.update({ telegramNotifications: false });
    }
    const usersSnap = await db.collection('users').where('telegramChatId', '==', chatIdStr).get();
    for (const doc of usersSnap.docs) await doc.ref.update({ telegramNotifications: false });
    return true;
  } catch (error) { console.error('Unlink error:', error); return false; }
}

async function getTelegramStatus(chatId) {
  try {
    const chatIdStr = chatId.toString();
    const snap = await db.collection('users').where('telegramChatId', '==', chatIdStr).get();
    if (!snap.empty) {
      const data = snap.docs[0].data();
      const status = data.telegramNotifications ? '✅ Активні' : '🔕 Вимкнені';
      return `📊 Статус:\nСповіщення: ${status}\nПідключено: ${data.telegramLinkedAt?.toDate()?.toLocaleDateString('uk-UA') || '—'}`;
    }
    return `❌ Telegram не підключено. Перейдіть в налаштування CRM.`;
  } catch (error) { return '⚠️ Помилка перевірки'; }
}

async function sendTelegramMessage(chatId, text, options = {}) {
  try {
    const response = await fetch(`${TELEGRAM_API}/sendMessage`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML', ...options })
    });
    const result = await response.json();
    if (!result.ok) console.error('Telegram send error:', result);
    return result.ok;
  } catch (error) { console.error('Send error:', error); return false; }
}

async function getUserTelegramChatId(userId) {
  try {
    const doc = await db.collection('users').doc(userId).get();
    if (doc.exists) {
      const data = doc.data();
      if (data.telegramNotifications && data.telegramChatId) return data.telegramChatId;
    }
    return null;
  } catch (error) { return null; }
}

function formatLeadNotification(lead) {
  const phone = lead.phone || '—';
  const tg = lead.tg || '—';
  let msg = `🆕 <b>Новий лід!</b>\n\n📋 Джерело: ${lead.sourceName || lead.source || '—'}\n`;
  if (phone !== '—') msg += `📞 ${phone}\n`;
  if (tg !== '—') msg += `📱 ${tg}\n`;
  if (lead.problem) msg += `\n💬 ${lead.problem}\n`;
  if (lead.notes) msg += `📝 ${lead.notes}\n`;
  msg += `\n🔗 <a href="https://talko-crm.vercel.app">Відкрити CRM</a>`;
  return msg;
}


// #####################################################
// ===== NOTIFICATION FUNCTIONS (existing) =============
// #####################################################

exports.sendLeadNotification = functions.https.onCall(async (data, context) => {
  try {
    const { orgId, lead, recipientUserIds } = data;
    if (!orgId || !lead) throw new functions.https.HttpsError('invalid-argument', 'Missing orgId or lead');

    let recipients = [];
    if (recipientUserIds?.length > 0) {
      for (const userId of recipientUserIds) {
        const chatId = await getUserTelegramChatId(userId);
        if (chatId) recipients.push(chatId);
      }
    } else {
      const snap = await db.collection('organizations').doc(orgId).collection('members')
        .where('telegramNotifications', '==', true).get();
      for (const doc of snap.docs) { const d = doc.data(); if (d.telegramChatId) recipients.push(d.telegramChatId); }
    }
    if (recipients.length === 0) return { success: true, sent: 0 };

    const message = formatLeadNotification(lead);
    let sent = 0;
    for (const chatId of recipients) { if (await sendTelegramMessage(chatId, message)) sent++; }
    return { success: true, sent, total: recipients.length };
  } catch (error) {
    console.error('Notification error:', error);
    throw new functions.https.HttpsError('internal', error.message);
  }
});

exports.sendLeadNotificationHttp = functions.https.onRequest(async (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') { res.set('Access-Control-Allow-Methods', 'POST'); res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization'); return res.status(204).send(''); }

  try {
    const { orgId, lead, secret } = req.body;
    if (secret !== 'talko-crm-2024') return res.status(401).json({ error: 'Unauthorized' });
    if (!orgId || !lead) return res.status(400).json({ error: 'Missing orgId or lead' });

    const membersSnap = await db.collection('organizations').doc(orgId).collection('members')
      .where('telegramNotifications', '==', true).get();

    let recipients = [];
    for (const doc of membersSnap.docs) {
      const d = doc.data();
      if (d.telegramChatId && (d.role === 'owner' || lead.assignedTo === d.userId)) recipients.push(d.telegramChatId);
    }

    const usersSnap = await db.collection('users').where('telegramNotifications', '==', true).get();
    for (const doc of usersSnap.docs) {
      const d = doc.data();
      if (d.telegramChatId && !recipients.includes(d.telegramChatId) && lead.assignedTo === doc.id) recipients.push(d.telegramChatId);
    }

    if (recipients.length === 0) return res.json({ success: true, sent: 0 });

    const message = formatLeadNotification(lead);
    let sent = 0;
    for (const chatId of recipients) { if (await sendTelegramMessage(chatId, message)) sent++; }
    res.json({ success: true, sent, total: recipients.length });
  } catch (error) { res.status(500).json({ error: error.message }); }
});


// #####################################################
// ===== FIRESTORE TRIGGER (existing) ==================
// #####################################################

exports.onNewLead = functions.firestore
  .document('organizations/{orgId}/leads/{leadId}')
  .onCreate(async (snap, context) => {
    try {
      const lead = snap.data();
      const orgId = context.params.orgId;
      if (!lead.syncedAt) return;

      const membersSnap = await db.collection('organizations').doc(orgId).collection('members')
        .where('telegramNotifications', '==', true).get();
      const message = formatLeadNotification(lead);

      for (const doc of membersSnap.docs) {
        const member = doc.data();
        if (member.telegramChatId && (member.role === 'owner' || lead.assignedTo === member.userId)) {
          await sendTelegramMessage(member.telegramChatId, message);
        }
      }
    } catch (error) { console.error('onNewLead error:', error); }
  });
