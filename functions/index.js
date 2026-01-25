/**
 * TALKO CRM - Cloud Functions
 * Telegram сповіщення, нагадування, автоматизація
 */

const functions = require('firebase-functions');
const admin = require('firebase-admin');
const fetch = require('node-fetch');

admin.initializeApp();
const db = admin.firestore();

const BOT_TOKEN = '8347933211:AAHzfNNo-v-Z-4rdpf11-KAf4r2dfAIXzSg';
const REGION = 'europe-west1';
const TIMEZONE = 'Europe/Kiev';

async function sendTelegram(chatId, text) {
  if (!BOT_TOKEN) return false;
  try {
    const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text: text,
        parse_mode: 'HTML'
      })
    });
    return response.ok;
  } catch (error) {
    console.error('Telegram send error:', error);
    return false;
  }
}

// Telegram Webhook - обробка команд бота
exports.telegramWebhook = functions.region(REGION).https.onRequest(async (req, res) => {
  try {
    const { message } = req.body;
    if (!message) {
      return res.status(200).send('OK');
    }

    const chatId = message.chat.id;
    const text = message.text || '';
    const userId = message.from.id;

    console.log(`Telegram from ${userId}: ${text}`);

    // Команда /start
    if (text === '/start') {
      await sendTelegram(chatId, 
        '👋 Вітаю! Я бот TALKO CRM.\n\n' +
        '🔗 Щоб підключити сповіщення, введіть код з CRM системи.\n\n' +
        'Команди:\n' +
        '/status - перевірити підключення\n' +
        '/disconnect - відключити сповіщення'
      );
      return res.status(200).send('OK');
    }

    // Команда /status
    if (text === '/start' || text === '/status') {
      const usersSnapshot = await db.collectionGroup('users')
        .where('telegramChatId', '==', chatId.toString())
        .get();
      
      if (usersSnapshot.empty) {
        await sendTelegram(chatId, '❌ Ваш Telegram не підключено до жодної організації.');
      } else {
        let statusText = '✅ Ваш Telegram підключено:\n\n';
        usersSnapshot.forEach(doc => {
          const data = doc.data();
          statusText += `• ${data.name || 'Користувач'}\n`;
        });
        await sendTelegram(chatId, statusText);
      }
      return res.status(200).send('OK');
    }

    // Команда /disconnect
    if (text === '/disconnect') {
      const usersSnapshot = await db.collectionGroup('users')
        .where('telegramChatId', '==', chatId.toString())
        .get();
      
      const batch = db.batch();
      usersSnapshot.forEach(doc => {
        batch.update(doc.ref, { 
          telegramChatId: admin.firestore.FieldValue.delete(),
          telegramConnected: false 
        });
      });
      await batch.commit();
      
      await sendTelegram(chatId, '✅ Сповіщення відключено.');
      return res.status(200).send('OK');
    }

    // Перевірка коду підключення (формат: TALKO_XXXXXX)
    if (text.startsWith('TALKO_')) {
      const code = text.trim();
      const codesSnapshot = await db.collectionGroup('telegramCodes')
        .where('code', '==', code)
        .where('used', '==', false)
        .get();

      if (codesSnapshot.empty) {
        await sendTelegram(chatId, '❌ Код не знайдено або вже використано.');
        return res.status(200).send('OK');
      }

      const codeDoc = codesSnapshot.docs[0];
      const codeData = codeDoc.data();

      // Оновлюємо користувача
      await db.doc(codeData.userPath).update({
        telegramChatId: chatId.toString(),
        telegramConnected: true,
        telegramConnectedAt: admin.firestore.FieldValue.serverTimestamp()
      });

      // Позначаємо код як використаний
      await codeDoc.ref.update({ used: true });

      await sendTelegram(chatId, 
        '✅ Telegram успішно підключено!\n\n' +
        'Тепер ви будете отримувати сповіщення про:\n' +
        '• Нові ліди\n' +
        '• Зміни статусів\n' +
        '• Нагадування про задачі'
      );
      return res.status(200).send('OK');
    }

    // Невідома команда
    await sendTelegram(chatId, 
      '🤔 Не розумію команду.\n\n' +
      'Доступні команди:\n' +
      '/start - почати\n' +
      '/status - статус підключення\n' +
      '/disconnect - відключити'
    );

    res.status(200).send('OK');
  } catch (error) {
    console.error('Webhook error:', error);
    res.status(200).send('OK');
  }
});

// Тригер на створення нового ліда
exports.onLeadCreate = functions.region(REGION)
  .firestore.document('organizations/{orgId}/leads/{leadId}')
  .onCreate(async (snap, context) => {
    const lead = snap.data();
    const { orgId } = context.params;

    // Отримуємо користувачів з увімкненими сповіщеннями
    const usersSnapshot = await db.collection(`organizations/${orgId}/users`)
      .where('telegramConnected', '==', true)
      .where('notifications.newLeads', '==', true)
      .get();

    const message = 
      `🆕 <b>Новий лід!</b>\n\n` +
      `👤 ${lead.name || 'Без імені'}\n` +
      `📞 ${lead.phone || 'Немає телефону'}\n` +
      `📧 ${lead.email || ''}\n` +
      `💬 ${lead.source || 'Невідоме джерело'}`;

    const promises = [];
    usersSnapshot.forEach(doc => {
      const user = doc.data();
      if (user.telegramChatId) {
        promises.push(sendTelegram(user.telegramChatId, message));
      }
    });

    await Promise.all(promises);
  });

// Тригер на оновлення ліда (зміна статусу)
exports.onLeadUpdate = functions.region(REGION)
  .firestore.document('organizations/{orgId}/leads/{leadId}')
  .onUpdate(async (change, context) => {
    const before = change.before.data();
    const after = change.after.data();
    const { orgId } = context.params;

    // Перевіряємо чи змінився статус
    if (before.status === after.status) return;

    const usersSnapshot = await db.collection(`organizations/${orgId}/users`)
      .where('telegramConnected', '==', true)
      .where('notifications.statusChanges', '==', true)
      .get();

    const message = 
      `🔄 <b>Зміна статусу</b>\n\n` +
      `👤 ${after.name || 'Без імені'}\n` +
      `📊 ${before.status} → <b>${after.status}</b>`;

    const promises = [];
    usersSnapshot.forEach(doc => {
      const user = doc.data();
      if (user.telegramChatId) {
        promises.push(sendTelegram(user.telegramChatId, message));
      }
    });

    await Promise.all(promises);
  });

// Щоденний звіт (запускається о 9:00)
exports.dailyReport = functions.region(REGION)
  .pubsub.schedule('0 9 * * *')
  .timeZone(TIMEZONE)
  .onRun(async (context) => {
    const orgsSnapshot = await db.collection('organizations').get();

    for (const orgDoc of orgsSnapshot.docs) {
      const orgId = orgDoc.id;
      
      // Рахуємо статистику за вчора
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      yesterday.setHours(0, 0, 0, 0);
      
      const today = new Date();
      today.setHours(0, 0, 0, 0);

      const leadsSnapshot = await db.collection(`organizations/${orgId}/leads`)
        .where('createdAt', '>=', yesterday)
        .where('createdAt', '<', today)
        .get();

      const usersSnapshot = await db.collection(`organizations/${orgId}/users`)
        .where('telegramConnected', '==', true)
        .where('notifications.dailyReport', '==', true)
        .get();

      if (usersSnapshot.empty) continue;

      const message = 
        `📊 <b>Звіт за вчора</b>\n\n` +
        `🆕 Нових лідів: ${leadsSnapshot.size}\n` +
        `\nГарного дня! 🌟`;

      const promises = [];
      usersSnapshot.forEach(doc => {
        const user = doc.data();
        if (user.telegramChatId) {
          promises.push(sendTelegram(user.telegramChatId, message));
        }
      });

      await Promise.all(promises);
    }
  });

// Webhook для зовнішніх інтеграцій (форми, лендінги)
exports.leadWebhook = functions.region(REGION).https.onRequest(async (req, res) => {
  // CORS
  res.set('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') {
    res.set('Access-Control-Allow-Methods', 'POST');
    res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    return res.status(204).send('');
  }

  if (req.method !== 'POST') {
    return res.status(405).send('Method not allowed');
  }

  try {
    const { orgId, apiKey, lead } = req.body;

    // Перевірка API ключа
    const orgDoc = await db.doc(`organizations/${orgId}`).get();
    if (!orgDoc.exists || orgDoc.data().apiKey !== apiKey) {
      return res.status(401).send('Unauthorized');
    }

    // Створюємо ліда
    const leadRef = await db.collection(`organizations/${orgId}/leads`).add({
      ...lead,
      source: lead.source || 'API',
      status: 'new',
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    });

    res.status(200).json({ success: true, leadId: leadRef.id });
  } catch (error) {
    console.error('Lead webhook error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Тестова функція для перевірки сповіщень
exports.testNotification = functions.region(REGION).https.onRequest(async (req, res) => {
  const { chatId, message } = req.query;
  
  if (!chatId) {
    return res.status(400).send('Missing chatId parameter');
  }

  const text = message || '🔔 Тестове сповіщення від TALKO CRM!';
  const result = await sendTelegram(chatId, text);
  
  res.status(200).json({ success: result });
});