/**
 * =====================================================
 * TALKO CRM - Firebase Functions
 * Telegram Bot Webhook + Notifications
 * =====================================================
 */

const functions = require('firebase-functions');
const admin = require('firebase-admin');

admin.initializeApp();
const db = admin.firestore();

// Telegram Bot Config
const TELEGRAM_BOT_TOKEN = '8347933211:AAHzfNNo-v-Z-4rdpf11-KAf4r2dfAIXzSg';
const TELEGRAM_API = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}`;

// ===== TELEGRAM BOT WEBHOOK =====
// Handles /start command with user linking

exports.telegramWebhook = functions.https.onRequest(async (req, res) => {
  try {
    if (req.method !== 'POST') {
      return res.status(200).send('Telegram Webhook Active');
    }

    const update = req.body;
    console.log('Telegram update:', JSON.stringify(update));

    // Handle message
    if (update.message) {
      const chatId = update.message.chat.id;
      const text = update.message.text || '';
      const firstName = update.message.from.first_name || '';

      // /start command with CRM user ID
      if (text.startsWith('/start')) {
        const parts = text.split(' ');
        const crmUserId = parts[1]; // /start USER_ID

        if (crmUserId) {
          // Link Telegram to CRM user
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
            `👋 Вітаю!\n\n` +
            `Щоб підключити сповіщення, перейдіть в налаштування TALKO CRM ` +
            `і натисніть кнопку "Підключити Telegram".\n\n` +
            `Це створить унікальне посилання для вашого акаунту.`
          );
        }
      }

      // /stop command - unsubscribe
      else if (text === '/stop') {
        await unlinkTelegram(chatId);
        await sendTelegramMessage(chatId,
          `🔕 Сповіщення вимкнено.\n\n` +
          `Щоб підключити знову — перейдіть в налаштування TALKO CRM.`
        );
      }

      // /status command
      else if (text === '/status') {
        const status = await getTelegramStatus(chatId);
        await sendTelegramMessage(chatId, status);
      }

      // Unknown command
      else {
        await sendTelegramMessage(chatId,
          `🤖 TALKO CRM Bot\n\n` +
          `Доступні команди:\n` +
          `/status — перевірити підключення\n` +
          `/stop — вимкнути сповіщення`
        );
      }
    }

    res.status(200).send('OK');
  } catch (error) {
    console.error('Webhook error:', error);
    res.status(200).send('OK'); // Always return 200 to Telegram
  }
});

// ===== HELPER FUNCTIONS =====

async function linkTelegramToUser(crmUserId, chatId, firstName) {
  try {
    // Find user across all organizations
    const orgsSnapshot = await db.collection('organizations').get();
    
    for (const orgDoc of orgsSnapshot.docs) {
      const membersSnapshot = await orgDoc.ref.collection('members')
        .where('userId', '==', crmUserId)
        .get();
      
      if (!membersSnapshot.empty) {
        // Update member with Telegram chat ID
        const memberDoc = membersSnapshot.docs[0];
        await memberDoc.ref.update({
          telegramChatId: chatId.toString(),
          telegramFirstName: firstName,
          telegramLinkedAt: admin.firestore.FieldValue.serverTimestamp(),
          telegramNotifications: true
        });
        
        console.log(`Linked Telegram ${chatId} to user ${crmUserId} in org ${orgDoc.id}`);
        return true;
      }
    }
    
    // Also check/create in global users collection
    await db.collection('users').doc(crmUserId).set({
      telegramChatId: chatId.toString(),
      telegramFirstName: firstName,
      telegramLinkedAt: admin.firestore.FieldValue.serverTimestamp(),
      telegramNotifications: true
    }, { merge: true });
    
    console.log(`Linked Telegram ${chatId} to user ${crmUserId} (global)`);
    return true;
  } catch (error) {
    console.error('Link error:', error);
    return false;
  }
}

async function unlinkTelegram(chatId) {
  try {
    const chatIdStr = chatId.toString();
    
    // Find and update in organizations
    const orgsSnapshot = await db.collection('organizations').get();
    
    for (const orgDoc of orgsSnapshot.docs) {
      const membersSnapshot = await orgDoc.ref.collection('members')
        .where('telegramChatId', '==', chatIdStr)
        .get();
      
      for (const memberDoc of membersSnapshot.docs) {
        await memberDoc.ref.update({
          telegramNotifications: false
        });
      }
    }
    
    // Also update global users
    const usersSnapshot = await db.collection('users')
      .where('telegramChatId', '==', chatIdStr)
      .get();
    
    for (const userDoc of usersSnapshot.docs) {
      await userDoc.ref.update({
        telegramNotifications: false
      });
    }
    
    return true;
  } catch (error) {
    console.error('Unlink error:', error);
    return false;
  }
}

async function getTelegramStatus(chatId) {
  try {
    const chatIdStr = chatId.toString();
    
    // Check in users collection
    const usersSnapshot = await db.collection('users')
      .where('telegramChatId', '==', chatIdStr)
      .get();
    
    if (!usersSnapshot.empty) {
      const userData = usersSnapshot.docs[0].data();
      const status = userData.telegramNotifications ? '✅ Активні' : '🔕 Вимкнені';
      return `📊 Статус підключення:\n\n` +
        `Сповіщення: ${status}\n` +
        `Підключено: ${userData.telegramLinkedAt?.toDate()?.toLocaleDateString('uk-UA') || '—'}`;
    }
    
    return `❌ Telegram не підключено до TALKO CRM.\n\n` +
      `Перейдіть в налаштування CRM і натисніть "Підключити Telegram".`;
  } catch (error) {
    console.error('Status error:', error);
    return '⚠️ Помилка перевірки статусу';
  }
}

async function sendTelegramMessage(chatId, text, options = {}) {
  try {
    const response = await fetch(`${TELEGRAM_API}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text: text,
        parse_mode: 'HTML',
        ...options
      })
    });
    
    const result = await response.json();
    if (!result.ok) {
      console.error('Telegram send error:', result);
    }
    return result.ok;
  } catch (error) {
    console.error('Send message error:', error);
    return false;
  }
}

// ===== NOTIFICATION FUNCTION =====
// Call this from Google Apps Script or trigger on new lead

exports.sendLeadNotification = functions.https.onCall(async (data, context) => {
  try {
    const { orgId, lead, recipientUserIds } = data;
    
    if (!orgId || !lead) {
      throw new functions.https.HttpsError('invalid-argument', 'Missing orgId or lead');
    }
    
    // Get recipients
    let recipients = [];
    
    if (recipientUserIds && recipientUserIds.length > 0) {
      // Specific recipients
      for (const userId of recipientUserIds) {
        const chatId = await getUserTelegramChatId(userId);
        if (chatId) recipients.push(chatId);
      }
    } else {
      // All org members with notifications enabled
      const membersSnapshot = await db.collection('organizations')
        .doc(orgId)
        .collection('members')
        .where('telegramNotifications', '==', true)
        .get();
      
      for (const doc of membersSnapshot.docs) {
        const data = doc.data();
        if (data.telegramChatId) {
          recipients.push(data.telegramChatId);
        }
      }
    }
    
    if (recipients.length === 0) {
      return { success: true, sent: 0, message: 'No recipients' };
    }
    
    // Format message
    const message = formatLeadNotification(lead);
    
    // Send to all recipients
    let sent = 0;
    for (const chatId of recipients) {
      const success = await sendTelegramMessage(chatId, message);
      if (success) sent++;
    }
    
    return { success: true, sent, total: recipients.length };
  } catch (error) {
    console.error('Notification error:', error);
    throw new functions.https.HttpsError('internal', error.message);
  }
});

// HTTP version for Google Apps Script
exports.sendLeadNotificationHttp = functions.https.onRequest(async (req, res) => {
  // CORS
  res.set('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') {
    res.set('Access-Control-Allow-Methods', 'POST');
    res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    return res.status(204).send('');
  }
  
  try {
    const { orgId, lead, secret } = req.body;
    
    // Simple secret validation (replace with better auth in production)
    if (secret !== 'talko-crm-2024') {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    
    if (!orgId || !lead) {
      return res.status(400).json({ error: 'Missing orgId or lead' });
    }
    
    // Get all org members with notifications enabled
    const membersSnapshot = await db.collection('organizations')
      .doc(orgId)
      .collection('members')
      .where('telegramNotifications', '==', true)
      .get();
    
    let recipients = [];
    for (const doc of membersSnapshot.docs) {
      const data = doc.data();
      if (data.telegramChatId) {
        // Check if lead is assigned to this user or user is owner
        if (data.role === 'owner' || lead.assignedTo === data.userId) {
          recipients.push(data.telegramChatId);
        }
      }
    }
    
    // Also check global users collection
    const usersSnapshot = await db.collection('users')
      .where('telegramNotifications', '==', true)
      .get();
    
    for (const doc of usersSnapshot.docs) {
      const data = doc.data();
      if (data.telegramChatId && !recipients.includes(data.telegramChatId)) {
        if (lead.assignedTo === doc.id) {
          recipients.push(data.telegramChatId);
        }
      }
    }
    
    if (recipients.length === 0) {
      return res.json({ success: true, sent: 0, message: 'No recipients with notifications enabled' });
    }
    
    // Format and send
    const message = formatLeadNotification(lead);
    let sent = 0;
    
    for (const chatId of recipients) {
      const success = await sendTelegramMessage(chatId, message);
      if (success) sent++;
    }
    
    res.json({ success: true, sent, total: recipients.length });
  } catch (error) {
    console.error('HTTP notification error:', error);
    res.status(500).json({ error: error.message });
  }
});

async function getUserTelegramChatId(userId) {
  try {
    // Check global users
    const userDoc = await db.collection('users').doc(userId).get();
    if (userDoc.exists) {
      const data = userDoc.data();
      if (data.telegramNotifications && data.telegramChatId) {
        return data.telegramChatId;
      }
    }
    return null;
  } catch (error) {
    console.error('Get chat ID error:', error);
    return null;
  }
}

function formatLeadNotification(lead) {
  const sourceName = lead.sourceName || lead.source || '—';
  const phone = lead.phone || '—';
  const tg = lead.tg || '—';
  const problem = lead.problem || '';
  const notes = lead.notes || '';
  
  let message = `🆕 <b>Новий лід!</b>\n\n`;
  message += `📋 Джерело: ${sourceName}\n`;
  
  if (phone !== '—') message += `📞 ${phone}\n`;
  if (tg !== '—') message += `📱 ${tg}\n`;
  if (problem) message += `\n💬 ${problem}\n`;
  if (notes) message += `📝 ${notes}\n`;
  
  message += `\n🔗 <a href="https://talko-crm.vercel.app">Відкрити CRM</a>`;
  
  return message;
}

// ===== FIRESTORE TRIGGER =====
// Auto-send notification when new lead created

exports.onNewLead = functions.firestore
  .document('organizations/{orgId}/leads/{leadId}')
  .onCreate(async (snap, context) => {
    try {
      const lead = snap.data();
      const orgId = context.params.orgId;
      
      // Skip if not a new lead from sync (has syncedAt)
      if (!lead.syncedAt) return;
      
      // Get org members with notifications
      const membersSnapshot = await db.collection('organizations')
        .doc(orgId)
        .collection('members')
        .where('telegramNotifications', '==', true)
        .get();
      
      const message = formatLeadNotification(lead);
      
      for (const doc of membersSnapshot.docs) {
        const member = doc.data();
        if (member.telegramChatId) {
          // Owner gets all, others only assigned
          if (member.role === 'owner' || lead.assignedTo === member.userId) {
            await sendTelegramMessage(member.telegramChatId, message);
          }
        }
      }
      
      console.log(`Sent notifications for new lead ${context.params.leadId}`);
    } catch (error) {
      console.error('onNewLead error:', error);
    }
  });
