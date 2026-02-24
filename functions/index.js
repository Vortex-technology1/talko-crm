/**
 * =====================================================
 * TALKO CRM - Firebase Functions
 * Telegram Bot + IP Telephony + SendPulse + Forms
 * + DIRECT Messenger Integrations (TG, Viber, WA, IG, FB)
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
async function findLead(orgId, { phone, email, telegramChatId, viberChatId, whatsappChatId, instagramChatId, facebookChatId, chatId }) {
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
  if (whatsappChatId) {
    const snapshot = await leadsRef.where('whatsappChatId', '==', String(whatsappChatId)).get();
    for (const doc of snapshot.docs) { const d = doc.data(); if (!d.deleted) return { id: doc.id, ...d }; }
  }
  if (instagramChatId) {
    const snapshot = await leadsRef.where('instagramChatId', '==', String(instagramChatId)).get();
    for (const doc of snapshot.docs) { const d = doc.data(); if (!d.deleted) return { id: doc.id, ...d }; }
  }
  if (facebookChatId) {
    const snapshot = await leadsRef.where('facebookChatId', '==', String(facebookChatId)).get();
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
    whatsappChatId: leadData.whatsappChatId ? String(leadData.whatsappChatId) : null,
    instagramChatId: leadData.instagramChatId ? String(leadData.instagramChatId) : null,
    facebookChatId: leadData.facebookChatId ? String(leadData.facebookChatId) : null,
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

    // SendPulse structure: { info: { message: { text, chat_id } }, service: "telegram", title: "incoming_message", bot: { id, external_id, name }, contact: { id, name } }
    const info = body.info || {};
    const msg = info.message || body.message || {};
    const botInfo = body.bot || {};
    const spBotId = botInfo.id || null; // SendPulse internal bot ID
    const spContactInfo = body.contact || {};
    const spContactId = spContactInfo.id || null; // SendPulse contact ID (if available)

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
        chatId: chatId, sendpulseBotId: spBotId, sendpulseContactId: spContactId,
        source: `sendpulse_${normalizedChannel}`, channel: normalizedChannel
      });
      isNewLead = true;
      console.log(`SendPulse: Created lead ${lead.id}`);
    } else {
      // Update chatId and SendPulse IDs if missing
      const updates = {};
      const field = normalizedChannel === 'telegram' ? 'telegramChatId' : normalizedChannel === 'viber' ? 'viberChatId' : null;
      if (field && !lead[field]) updates[field] = chatId;
      if (spBotId && !lead.sendpulseBotId) updates.sendpulseBotId = spBotId;
      if (spContactId && !lead.sendpulseContactId) updates.sendpulseContactId = spContactId;
      if (!lead.chatId) updates.chatId = chatId;
      if (!lead.lastChannel) updates.lastChannel = normalizedChannel;
      if (Object.keys(updates).length > 0) {
        await db.collection('organizations').doc(orgId).collection('leads').doc(lead.id).update(updates);
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
                 : lead.telegramChatId || lead.viberChatId || lead.chatId;
    if (!chatId) return res.status(400).json({ error: `No ${sendChannel} chatId` });

    // Get SendPulse token
    const tokenResp = await fetch('https://api.sendpulse.com/oauth/access_token', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ grant_type: 'client_credentials', client_id: spConfig.apiId, client_secret: spConfig.apiSecret })
    });
    const tokenData = await tokenResp.json();
    if (!tokenData.access_token) return res.status(500).json({ error: 'SendPulse auth failed' });

    const authHeaders = { 'Content-Type': 'application/json', 'Authorization': `Bearer ${tokenData.access_token}` };

    // Get all bots to find the right one
    const botsResp = await fetch(`https://api.sendpulse.com/${sendChannel}/bots`, { headers: authHeaders });
    const botsData = await botsResp.json();
    console.log('SendPulse bots:', JSON.stringify(botsData).slice(0, 500));

    // Find the bot that this lead came from (stored on lead) or use first bot
    const leadBotId = lead.sendpulseBotId || null;
    let botId = null;
    if (Array.isArray(botsData)) {
      botId = leadBotId ? botsData.find(b => b.id === leadBotId)?.id : botsData[0]?.id;
      if (!botId && botsData.length > 0) botId = botsData[0].id;
    }

    // Lookup SendPulse contact_id by chat_id using getContact API
    let spContactId = lead.sendpulseContactId || null;
    if (!spContactId && botId) {
      // Try getting contact by chat_id
      try {
        const url = `https://api.sendpulse.com/${sendChannel}/contacts/get?id=${chatId}&bot_id=${botId}`;
        const resp = await fetch(url, { headers: authHeaders });
        const data = await resp.json();
        console.log('SendPulse contact by chatId:', JSON.stringify(data).slice(0, 300));
        if (data?.data?.id) spContactId = data.data.id;
      } catch (e) { console.log('Contact lookup failed:', e.message); }
    }

    // If still no contact_id, try searching through bot contacts
    if (!spContactId && botId) {
      try {
        const url = `https://api.sendpulse.com/${sendChannel}/contacts?bot_id=${botId}&tag=chat_id&value=${chatId}`;
        const resp = await fetch(url, { headers: authHeaders });
        const data = await resp.json();
        console.log('SendPulse contact search:', JSON.stringify(data).slice(0, 300));
        if (data?.data?.[0]?.id) spContactId = data.data[0].id;
      } catch (e) { console.log('Contact search failed:', e.message); }
    }

    // Try multiple send approaches
    let sendResult = null;
    let sendSuccess = false;

    // Approach 1: Send via SendPulse contact_id
    if (spContactId) {
      const resp1 = await fetch(`https://api.sendpulse.com/${sendChannel}/contacts/send`, {
        method: 'POST', headers: authHeaders,
        body: JSON.stringify({ contact_id: spContactId, messages: [{ type: 'text', text: { body: text } }] })
      });
      sendResult = await resp1.json();
      console.log('SendPulse send (spContactId):', JSON.stringify(sendResult).slice(0, 300));
      if (resp1.ok && !sendResult.error && !sendResult.message) sendSuccess = true;
    }

    // Approach 2: Send via chat_id as contact_id
    if (!sendSuccess) {
      const resp2 = await fetch(`https://api.sendpulse.com/${sendChannel}/contacts/send`, {
        method: 'POST', headers: authHeaders,
        body: JSON.stringify({ contact_id: chatId, messages: [{ type: 'text', text: { body: text } }] })
      });
      sendResult = await resp2.json();
      console.log('SendPulse send (chatId):', JSON.stringify(sendResult).slice(0, 300));
      if (resp2.ok && !sendResult.error && !sendResult.message) sendSuccess = true;
    }

    // Approach 3: Send via SendPulse sendText API (alternative format)
    if (!sendSuccess && botId) {
      try {
        const resp3 = await fetch(`https://api.sendpulse.com/${sendChannel}/contacts/sendText`, {
          method: 'POST', headers: authHeaders,
          body: JSON.stringify({ bot_id: botId, chat_id: chatId, message: { type: 'text', text: { body: text } } })
        });
        sendResult = await resp3.json();
        console.log('SendPulse sendText:', JSON.stringify(sendResult).slice(0, 300));
        if (resp3.ok && !sendResult.error && !sendResult.message) sendSuccess = true;
      } catch (e) { console.log('sendText failed:', e.message); }
    }

    // Approach 4: Direct Telegram Bot API fallback
    if (!sendSuccess && sendChannel === 'telegram') {
      console.log('All SendPulse methods failed, trying direct Telegram API...');
      const orgTgToken = orgData?.settings?.telegramBot?.token || orgData?.integrations?.telegram?.botToken || orgData?.integrations?.sendpulse?.telegramBotToken || spConfig?.telegramBotToken;
      if (orgTgToken) {
        const tgResp = await fetch(`https://api.telegram.org/bot${orgTgToken}/sendMessage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId, text })
        });
        sendResult = await tgResp.json();
        console.log('Telegram direct send:', JSON.stringify(sendResult).slice(0, 300));
        sendSuccess = tgResp.ok;
      }
    }

    console.log(`SendPulse send result: success=${sendSuccess}, channel=${sendChannel}, chatId=${chatId}`);

    // Save SendPulse contact_id on lead for future sends
    if (spContactId && !lead.sendpulseContactId) {
      await db.collection('organizations').doc(orgId).collection('leads').doc(leadId).update({ sendpulseContactId: spContactId });
    }

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
// ===== 6. DIRECT TELEGRAM BOT WEBHOOK ================
// #####################################################
// Receives messages from client's own Telegram bot
// (NOT the CRM notification bot, but the business bot)
// URL: /directTelegramWebhook?orgId=xxx

exports.directTelegramWebhook = functions.https.onRequest(async (req, res) => {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method === 'GET') return res.status(200).json({ status: 'ok', service: 'TALKO CRM Direct Telegram' });
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const orgId = req.query.orgId;
  if (!orgId) return res.status(400).json({ error: 'Missing orgId' });

  try {
    const update = req.body;
    const message = update.message || update.edited_message;

    if (!message) {
      // Handle callback_query (button clicks)
      if (update.callback_query) {
        const cb = update.callback_query;
        const from = cb.from || {};
        const cbChatId = String(cb.message?.chat?.id || from.id || '');
        if (!cbChatId) return res.status(200).json({ ok: true });

        const senderName = [from.first_name, from.last_name].filter(Boolean).join(' ') || from.username || 'Unknown';
        const orgDoc = await db.collection('organizations').doc(orgId).get();
        const orgData = orgDoc.exists ? orgDoc.data() : {};

        let lead = await findLead(orgId, { telegramChatId: cbChatId });
        if (!lead) {
          const autoCreate = orgData.integrations?.direct?.global?.autoCreateLead !== false;
          if (autoCreate) {
            lead = await createLead(orgId, orgData, {
              biz: senderName, tg: from.username ? '@' + from.username : null,
              telegramChatId: cbChatId, source: 'telegram_direct', channel: 'telegram'
            });
          }
        }
        if (lead) {
          await saveMessage(orgId, {
            leadId: lead.id, channel: 'telegram', direction: 'in', type: 'text',
            text: `🔘 Натиснув кнопку: ${cb.data || ''}`, senderName,
            chatId: cbChatId, read: false, source: 'telegram_direct'
          });
        }
        return res.status(200).json({ ok: true });
      }
      return res.status(200).json({ ok: true });
    }

    const chat = message.chat;
    const from = message.from || {};

    // Skip groups/channels
    if (!chat || chat.type !== 'private') return res.status(200).json({ ok: true, skipped: 'not_private' });

    const chatId = String(chat.id);
    const senderName = [from.first_name, from.last_name].filter(Boolean).join(' ') || from.username || 'Unknown';
    const username = from.username || '';
    const phone = message.contact?.phone_number || '';

    // Extract text and attachments
    let text = message.text || message.caption || '';
    const attachments = [];

    if (message.photo) {
      const photo = message.photo[message.photo.length - 1];
      attachments.push({ type: 'image', fileId: photo.file_id, width: photo.width, height: photo.height });
      if (!text) text = '🖼 Фото';
    }
    if (message.document) {
      attachments.push({ type: 'file', fileId: message.document.file_id, name: message.document.file_name, mimeType: message.document.mime_type });
      if (!text) text = `📎 ${message.document.file_name || 'Файл'}`;
    }
    if (message.voice) {
      attachments.push({ type: 'voice', fileId: message.voice.file_id, duration: message.voice.duration });
      if (!text) text = '🎤 Голосове повідомлення';
    }
    if (message.video) {
      attachments.push({ type: 'video', fileId: message.video.file_id, duration: message.video.duration });
      if (!text) text = '🎬 Відео';
    }
    if (message.sticker) { if (!text) text = `${message.sticker.emoji || '🏷️'} Стікер`; }
    if (message.location) { if (!text) text = `📍 Локація: ${message.location.latitude}, ${message.location.longitude}`; }
    if (message.contact) { if (!text) text = `👤 Контакт: ${message.contact.first_name || ''} ${message.contact.phone_number || ''}`; }

    if (text === '/start') text = '▶️ Розпочав чат з ботом';

    // Get org data
    const orgDoc = await db.collection('organizations').doc(orgId).get();
    const orgData = orgDoc.exists ? orgDoc.data() : {};

    // Find or create lead
    let lead = await findLead(orgId, { telegramChatId: chatId, phone: phone || undefined });

    // Also try by @username
    if (!lead && username) {
      const tgHandle = '@' + username.replace('@', '');
      const leadsRef = db.collection('organizations').doc(orgId).collection('leads');
      const snap = await leadsRef.where('tg', '==', tgHandle).limit(1).get();
      if (!snap.empty) {
        const doc = snap.docs[0];
        const d = doc.data();
        if (!d.deleted) {
          lead = { id: doc.id, ...d };
          // Update telegramChatId for future
          await leadsRef.doc(doc.id).update({ telegramChatId: chatId });
        }
      }
    }

    let isNewLead = false;
    if (!lead) {
      const autoCreate = orgData.integrations?.direct?.global?.autoCreateLead !== false;
      if (!autoCreate) return res.status(200).json({ ok: true, action: 'skipped_no_autocreate' });

      lead = await createLead(orgId, orgData, {
        biz: senderName, tg: username ? '@' + username : null,
        phone: phone ? normalizePhone(phone) : null,
        telegramChatId: chatId, source: 'telegram_direct', channel: 'telegram'
      });
      isNewLead = true;
    }

    // Save message
    const savedMsg = await saveMessage(orgId, {
      leadId: lead.id, channel: 'telegram', direction: 'in', type: 'text',
      text, senderName, chatId, attachments,
      externalMsgId: String(message.message_id),
      managerId: lead.assignedTo || null, read: false, source: 'telegram_direct'
    });

    // Update lead
    await db.collection('organizations').doc(orgId).collection('leads').doc(lead.id).update({
      lastChannel: 'telegram', telegramChatId: chatId,
      lastMessageAt: new Date().toISOString(),
      log: admin.firestore.FieldValue.arrayUnion({
        type: 'message_in',
        text: `[telegram] ${senderName}: ${(text || '').slice(0, 100)}`,
        date: new Date().toISOString()
      })
    });

    // Real-time event
    await emitEvent(orgId, {
      type: 'new_message', channel: 'telegram',
      leadId: lead.id, leadName: lead.biz || senderName || lead.tg || lead.phone,
      text: (text || '').slice(0, 100), senderName,
      managerId: lead.assignedTo, isNewLead
    });

    // Notify manager via CRM Telegram bot
    if (lead.assignedTo) {
      const managerChatId = await getUserTelegramChatId(lead.assignedTo);
      if (managerChatId) {
        await sendTelegramMessage(managerChatId,
          `💬 Нове повідомлення [telegram]\n\n` +
          `${senderName}: ${(text || '').slice(0, 200)}\n\n` +
          `🔗 <a href="https://talko-crm.vercel.app">Відкрити CRM</a>`
        );
      }
    }

    // Auto-responder for new leads
    if (isNewLead && orgData.integrations?.direct?.global?.aiAutoResponder) {
      const botToken = orgData.integrations?.direct?.telegram?.botToken;
      if (botToken) {
        const bizName = orgData.aiConfig?.business?.name || '';
        const greeting = bizName
          ? `Доброго дня! Дякую за звернення до ${bizName}. Менеджер зв'яжеться з вами найближчим часом!`
          : "Доброго дня! Ваше повідомлення отримано, менеджер зв'яжеться з вами найближчим часом!";

        await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId, text: greeting })
        });

        // Save auto-response
        await saveMessage(orgId, {
          leadId: lead.id, channel: 'telegram', direction: 'out', type: 'text',
          text: greeting, senderName: 'AI Авто-відповідач', chatId,
          managerId: null, read: true, source: 'auto_responder'
        });
      }
    }

    console.log(`DirectTG [${orgId}] lead:${lead.id} ${isNewLead ? '(NEW)' : ''} text:"${(text||'').slice(0, 50)}"`);
    return res.status(200).json({ ok: true, leadId: lead.id, messageId: savedMsg.id, isNewLead });

  } catch (err) {
    console.error('[DirectTG] Error:', err);
    return res.status(200).json({ ok: true, error: err.message }); // Always 200 for Telegram
  }
});


// #####################################################
// ===== 7. DIRECT VIBER BOT WEBHOOK ==================
// #####################################################
// URL: /directViberWebhook?orgId=xxx

exports.directViberWebhook = functions.https.onRequest(async (req, res) => {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method === 'GET') return res.status(200).json({ status: 'ok', service: 'TALKO CRM Direct Viber' });
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const orgId = req.query.orgId;
  if (!orgId) return res.status(400).json({ error: 'Missing orgId' });

  try {
    const event = req.body;
    const eventType = event.event;

    // Viber webhook verification
    if (eventType === 'webhook') {
      console.log(`[DirectViber] Webhook set for org ${orgId}`);
      return res.status(200).json({ status: 0, status_message: 'ok' });
    }

    // Message event
    if (eventType === 'message') {
      const sender = event.sender || {};
      const viberChatId = sender.id;
      if (!viberChatId) return res.status(200).json({ status: 0, status_message: 'ok' });

      const senderName = sender.name || 'Viber User';
      const message = event.message || {};
      if (!message.type) return res.status(200).json({ status: 0, status_message: 'ok' });

      // Extract text
      let text = '';
      const attachments = [];
      switch (message.type) {
        case 'text': text = message.text || ''; break;
        case 'picture':
          text = message.text || '🖼 Фото';
          if (message.media) attachments.push({ type: 'image', url: message.media });
          break;
        case 'video': text = '🎬 Відео'; if (message.media) attachments.push({ type: 'video', url: message.media }); break;
        case 'file':
          text = `📎 ${message.file_name || 'Файл'}`;
          if (message.media) attachments.push({ type: 'file', url: message.media, name: message.file_name });
          break;
        case 'contact':
          const c = message.contact || {};
          text = `👤 Контакт: ${c.name || ''} ${c.phone_number || ''}`;
          break;
        case 'location':
          const loc = message.location || {};
          text = `📍 Локація: ${loc.lat}, ${loc.lon}`;
          break;
        case 'sticker': text = '🏷️ Стікер'; break;
        default: text = message.text || `[${message.type || 'unknown'}]`;
      }

      const orgDoc = await db.collection('organizations').doc(orgId).get();
      const orgData = orgDoc.exists ? orgDoc.data() : {};

      let lead = await findLead(orgId, { viberChatId });
      let isNewLead = false;

      if (!lead) {
        const autoCreate = orgData.integrations?.direct?.global?.autoCreateLead !== false;
        if (!autoCreate) return res.status(200).json({ status: 0, status_message: 'ok' });

        lead = await createLead(orgId, orgData, {
          biz: senderName, viberChatId,
          source: 'viber_direct', channel: 'viber'
        });
        isNewLead = true;
      }

      await saveMessage(orgId, {
        leadId: lead.id, channel: 'viber', direction: 'in', type: 'text',
        text, senderName, chatId: viberChatId, attachments,
        managerId: lead.assignedTo || null, read: false, source: 'viber_direct'
      });

      await db.collection('organizations').doc(orgId).collection('leads').doc(lead.id).update({
        lastChannel: 'viber', viberChatId,
        lastMessageAt: new Date().toISOString(),
        log: admin.firestore.FieldValue.arrayUnion({
          type: 'message_in', text: `[viber] ${senderName}: ${(text || '').slice(0, 100)}`, date: new Date().toISOString()
        })
      });

      await emitEvent(orgId, {
        type: 'new_message', channel: 'viber',
        leadId: lead.id, leadName: lead.biz || senderName,
        text: (text || '').slice(0, 100), senderName,
        managerId: lead.assignedTo, isNewLead
      });

      if (lead.assignedTo) {
        const managerChatId = await getUserTelegramChatId(lead.assignedTo);
        if (managerChatId) {
          await sendTelegramMessage(managerChatId,
            `💬 Нове повідомлення [viber]\n\n${senderName}: ${(text || '').slice(0, 200)}\n\n🔗 <a href="https://talko-crm.vercel.app">Відкрити CRM</a>`
          );
        }
      }

      console.log(`DirectViber [${orgId}] lead:${lead.id} ${isNewLead ? '(NEW)' : ''} text:"${(text||'').slice(0,50)}"`);
    }

    // Conversation started
    if (eventType === 'conversation_started') {
      const user = event.user || {};
      if (user.id) {
        const orgDoc = await db.collection('organizations').doc(orgId).get();
        const orgData = orgDoc.exists ? orgDoc.data() : {};
        const autoCreate = orgData.integrations?.direct?.global?.autoCreateLead !== false;
        if (autoCreate) {
          let lead = await findLead(orgId, { viberChatId: user.id });
          if (!lead) {
            lead = await createLead(orgId, orgData, {
              biz: user.name || 'Viber User', viberChatId: user.id,
              source: 'viber_direct', channel: 'viber'
            });
          }
          await saveMessage(orgId, {
            leadId: lead.id, channel: 'viber', direction: 'in', type: 'system',
            text: '▶️ Розпочав діалог з ботом', senderName: user.name || 'Viber User',
            chatId: user.id, read: false, source: 'viber_direct'
          });
        }
      }
    }

    // Delivery/seen status
    if (eventType === 'delivered' || eventType === 'seen') {
      // Non-critical, skip for now
    }

    return res.status(200).json({ status: 0, status_message: 'ok' });
  } catch (err) {
    console.error('[DirectViber] Error:', err);
    return res.status(200).json({ status: 0, status_message: 'ok' });
  }
});


// #####################################################
// ===== 8. DIRECT META WEBHOOK ========================
// #####################################################
// Handles: WhatsApp Business, Instagram Direct, Facebook Messenger
// URL: /directMetaWebhook?orgId=xxx

exports.directMetaWebhook = functions.https.onRequest(async (req, res) => {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  const orgId = req.query.orgId;

  // GET — Meta webhook verification
  if (req.method === 'GET') {
    const mode = req.query['hub.mode'];
    const token = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];

    if (mode === 'subscribe' && orgId) {
      try {
        const orgDoc = await db.collection('organizations').doc(orgId).get();
        const orgData = orgDoc.exists ? orgDoc.data() : {};
        const verifyToken = orgData.integrations?.direct?.whatsapp?.verifyToken
          || orgData.integrations?.direct?.instagram?.verifyToken
          || orgData.webhook?.key;

        if (token === verifyToken) {
          console.log(`[DirectMeta] Webhook verified for org ${orgId}`);
          return res.status(200).send(challenge);
        }
      } catch (e) { console.error('[DirectMeta] Verify error:', e); }
    }
    return res.status(403).json({ error: 'Verification failed' });
  }

  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
  if (!orgId) return res.status(400).json({ error: 'Missing orgId' });

  try {
    const body = req.body;
    const entries = body.entry || [];

    const orgDoc = await db.collection('organizations').doc(orgId).get();
    const orgData = orgDoc.exists ? orgDoc.data() : {};
    const igPageId = orgData.integrations?.direct?.instagram?.pageId;
    const fbPageId = orgData.integrations?.direct?.facebook?.pageId;

    for (const entry of entries) {
      const changes = entry.changes || [];
      const messagingEvents = entry.messaging || [];

      // WhatsApp Business — uses changes[].value
      for (const change of changes) {
        if (change.field === 'messages') {
          const value = change.value || {};
          const messages = value.messages || [];
          const contacts = value.contacts || [];
          const statuses = value.statuses || [];

          // Process incoming messages
          for (const msg of messages) {
            const waFrom = msg.from;
            if (!waFrom) continue;

            const contactInfo = contacts.find(c => c.wa_id === waFrom) || {};
            const senderName = contactInfo.profile?.name || waFrom;

            let text = '';
            const attachments = [];
            switch (msg.type) {
              case 'text': text = msg.text?.body || ''; break;
              case 'image': text = msg.image?.caption || '🖼 Фото'; attachments.push({ type: 'image', mediaId: msg.image?.id }); break;
              case 'video': text = msg.video?.caption || '🎬 Відео'; attachments.push({ type: 'video', mediaId: msg.video?.id }); break;
              case 'audio': text = '🎤 Аудіо'; attachments.push({ type: 'audio', mediaId: msg.audio?.id }); break;
              case 'document': text = `📎 ${msg.document?.filename || 'Документ'}`; attachments.push({ type: 'file', mediaId: msg.document?.id, name: msg.document?.filename }); break;
              case 'location': text = `📍 ${msg.location?.name || 'Локація'}: ${msg.location?.latitude}, ${msg.location?.longitude}`; break;
              case 'contacts': { const c = msg.contacts?.[0] || {}; text = `👤 ${c.name?.formatted_name || ''} ${c.phones?.[0]?.phone || ''}`; break; }
              case 'sticker': text = '🏷️ Стікер'; break;
              case 'reaction': text = `${msg.reaction?.emoji || '👍'} Реакція`; break;
              case 'button': text = `🔘 ${msg.button?.text || 'Кнопка'}`; break;
              case 'interactive': { const r = msg.interactive?.button_reply || msg.interactive?.list_reply || {}; text = `🔘 ${r.title || r.id || 'Вибір'}`; break; }
              default: text = `[${msg.type}]`;
            }

            let lead = await findLead(orgId, { whatsappChatId: waFrom, phone: '+' + waFrom });
            let isNewLead = false;
            if (!lead) {
              const autoCreate = orgData.integrations?.direct?.global?.autoCreateLead !== false;
              if (!autoCreate) continue;
              lead = await createLead(orgId, orgData, {
                biz: senderName, phone: normalizePhone('+' + waFrom),
                whatsappChatId: waFrom, source: 'whatsapp_direct', channel: 'whatsapp'
              });
              isNewLead = true;
            }

            await saveMessage(orgId, {
              leadId: lead.id, channel: 'whatsapp', direction: 'in', type: 'text',
              text, senderName, chatId: waFrom, attachments,
              externalMsgId: msg.id, managerId: lead.assignedTo || null, read: false, source: 'whatsapp_direct'
            });

            await db.collection('organizations').doc(orgId).collection('leads').doc(lead.id).update({
              lastChannel: 'whatsapp', whatsappChatId: waFrom,
              lastMessageAt: new Date().toISOString(),
              log: admin.firestore.FieldValue.arrayUnion({
                type: 'message_in', text: `[whatsapp] ${senderName}: ${(text || '').slice(0, 100)}`, date: new Date().toISOString()
              })
            });

            await emitEvent(orgId, {
              type: 'new_message', channel: 'whatsapp',
              leadId: lead.id, leadName: lead.biz || senderName,
              text: (text || '').slice(0, 100), senderName,
              managerId: lead.assignedTo, isNewLead
            });

            if (lead.assignedTo) {
              const managerChatId = await getUserTelegramChatId(lead.assignedTo);
              if (managerChatId) {
                await sendTelegramMessage(managerChatId,
                  `💬 Нове повідомлення [whatsapp]\n\n${senderName}: ${(text || '').slice(0, 200)}\n\n🔗 <a href="https://talko-crm.vercel.app">Відкрити CRM</a>`
                );
              }
            }

            console.log(`DirectWA [${orgId}] lead:${lead.id} ${isNewLead ? '(NEW)' : ''} from:${waFrom}`);
          }

          // Status updates (delivered/read) — non-critical
          // for (const st of statuses) { ... }
        }
      }

      // Instagram Direct + Facebook Messenger — uses entry.messaging
      for (const msgEvent of messagingEvents) {
        const senderId = msgEvent.sender?.id;
        const recipientId = msgEvent.recipient?.id;
        if (!senderId) continue;

        // Skip echo
        if (msgEvent.message?.is_echo) continue;
        if (senderId === recipientId) continue;
        if (igPageId && senderId === igPageId) continue;
        if (fbPageId && senderId === fbPageId) continue;

        // Determine channel
        let channel = 'facebook';
        if (igPageId && (recipientId === igPageId || entry.id === igPageId)) channel = 'instagram';
        else if (fbPageId && (recipientId === fbPageId || entry.id === fbPageId)) channel = 'facebook';

        const chatIdField = channel + 'ChatId';
        const evMessage = msgEvent.message;
        const evPostback = msgEvent.postback;

        let text = '';
        const attachments = [];

        if (evMessage) {
          text = evMessage.text || '';
          if (evMessage.attachments) {
            for (const att of evMessage.attachments) {
              if (att.type === 'image') { attachments.push({ type: 'image', url: att.payload?.url }); if (!text) text = '🖼 Фото'; }
              else if (att.type === 'video') { attachments.push({ type: 'video', url: att.payload?.url }); if (!text) text = '🎬 Відео'; }
              else if (att.type === 'audio') { attachments.push({ type: 'audio', url: att.payload?.url }); if (!text) text = '🎤 Аудіо'; }
              else if (att.type === 'file') { attachments.push({ type: 'file', url: att.payload?.url }); if (!text) text = '📎 Файл'; }
            }
          }
          if (evMessage.story_mention && !text) text = '📖 Згадав(ла) вас в Stories';
          if (evMessage.quick_reply && !text) text = `🔘 ${evMessage.quick_reply.payload || 'Вибір'}`;
        }
        if (evPostback) {
          text = `🔘 ${evPostback.title || evPostback.payload || 'Кнопка'}`;
        }

        if (!text && !attachments.length) continue;

        // Get sender name from Meta API
        let senderName = 'Клієнт';
        try {
          const token = channel === 'instagram'
            ? orgData.integrations?.direct?.instagram?.accessToken
            : orgData.integrations?.direct?.facebook?.accessToken;
          if (token) {
            const resp = await fetch(`https://graph.facebook.com/v18.0/${senderId}?fields=name,first_name&access_token=${token}`);
            const data = await resp.json();
            if (data.name) senderName = data.name;
            else if (data.first_name) senderName = data.first_name;
          }
        } catch (e) { /* non-critical */ }

        const searchParams = { [chatIdField]: senderId };
        let lead = await findLead(orgId, searchParams);
        let isNewLead = false;

        if (!lead) {
          const autoCreate = orgData.integrations?.direct?.global?.autoCreateLead !== false;
          if (!autoCreate) continue;
          lead = await createLead(orgId, orgData, {
            biz: senderName, [chatIdField]: senderId,
            source: `${channel}_direct`, channel
          });
          isNewLead = true;
        }

        await saveMessage(orgId, {
          leadId: lead.id, channel, direction: 'in', type: 'text',
          text, senderName, chatId: senderId, attachments,
          externalMsgId: evMessage?.mid, managerId: lead.assignedTo || null,
          read: false, source: `${channel}_direct`
        });

        await db.collection('organizations').doc(orgId).collection('leads').doc(lead.id).update({
          lastChannel: channel, [chatIdField]: senderId,
          lastMessageAt: new Date().toISOString(),
          log: admin.firestore.FieldValue.arrayUnion({
            type: 'message_in', text: `[${channel}] ${senderName}: ${(text || '').slice(0, 100)}`, date: new Date().toISOString()
          })
        });

        await emitEvent(orgId, {
          type: 'new_message', channel,
          leadId: lead.id, leadName: lead.biz || senderName,
          text: (text || '').slice(0, 100), senderName,
          managerId: lead.assignedTo, isNewLead
        });

        if (lead.assignedTo) {
          const managerChatId = await getUserTelegramChatId(lead.assignedTo);
          if (managerChatId) {
            await sendTelegramMessage(managerChatId,
              `💬 Нове повідомлення [${channel}]\n\n${senderName}: ${(text || '').slice(0, 200)}\n\n🔗 <a href="https://talko-crm.vercel.app">Відкрити CRM</a>`
            );
          }
        }

        console.log(`Direct${channel.charAt(0).toUpperCase() + channel.slice(1)} [${orgId}] lead:${lead.id} ${isNewLead ? '(NEW)' : ''}`);
      }
    }

    return res.status(200).json({ status: 'ok' });
  } catch (err) {
    console.error('[DirectMeta] Error:', err);
    return res.status(200).json({ status: 'ok' });
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
