/**
 * TALKO CRM - Cloud Functions
 * Telegram сповіщення, нагадування, автоматизація
 * 
 * Функції:
 * 1. telegramWebhook - підключення Telegram акаунтів
 * 2. onLeadCreate - сповіщення про нових лідів
 * 3. onLeadUpdate - сповіщення при зміні статусу/призначенні
 * 4. checkTaskReminders - нагадування про задачі (кожні 5 хв)
 * 5. dailyReport - ранковий звіт (9:00)
 * 6. leadWebhook - прийом лідів з зовнішніх джерел (сайт, форми)
 * 7. testNotification - тестування сповіщень
 */

const functions = require('firebase-functions');
const admin = require('firebase-admin');
const fetch = require('node-fetch');

admin.initializeApp();
const db = admin.firestore();

// ============================================================
// КОНФІГУРАЦІЯ
// ============================================================

// Telegram Bot Token (встановити через: firebase functions:config:set telegram.token="YOUR_TOKEN")
const BOT_TOKEN = functions.config().telegram?.token || process.env.TELEGRAM_BOT_TOKEN || '';

// Регіон для функцій
const REGION = 'europe-west1';

// Часова зона
const TIMEZONE = 'Europe/Kiev';

// ============================================================
// ДОПОМІЖНІ ФУНКЦІЇ
// ============================================================

/**
 * Надсилає повідомлення в Telegram
 */
async function sendTelegram(chatId, text, options = {}) {
    if (!BOT_TOKEN) {
        console.error('Telegram BOT_TOKEN not configured!');
        return false;
    }
    
    try {
        const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                chat_id: chatId,
                text: text,
                parse_mode: 'HTML',
                disable_web_page_preview: true,
                ...options
            })
        });
        
        const result = await response.json();
        if (!result.ok) {
            console.error('Telegram API error:', result);
            return false;
        }
        return true;
    } catch (error) {
        console.error('Telegram send error:', error);
        return false;
    }
}

/**
 * Отримує всіх користувачів організації з Telegram
 */
async function getOrgUsersWithTelegram(orgId, roles = null) {
    let query = db.collection('organizations').doc(orgId).collection('team');
    
    if (roles && roles.length > 0) {
        query = query.where('role', 'in', roles);
    }
    
    const snapshot = await query.get();
    return snapshot.docs
        .map(doc => ({ id: doc.id, ...doc.data() }))
        .filter(user => user.telegramChatId);
}

/**
 * Отримує налаштування сповіщень користувача
 */
function getUserNotificationSettings(user) {
    return {
        newLeads: user.notifications?.newLeads !== false,
        statusChanges: user.notifications?.statusChanges !== false,
        assignments: user.notifications?.assignments !== false,
        reminders: user.notifications?.reminders !== false,
        dailyReport: user.notifications?.dailyReport !== false,
        quietHoursStart: user.notifications?.quietHoursStart || null,
        quietHoursEnd: user.notifications?.quietHoursEnd || null
    };
}

/**
 * Перевіряє чи зараз тиха година
 */
function isQuietHours(settings) {
    if (!settings.quietHoursStart || !settings.quietHoursEnd) return false;
    
    const now = new Date();
    const hours = now.getHours();
    const start = parseInt(settings.quietHoursStart);
    const end = parseInt(settings.quietHoursEnd);
    
    if (start < end) {
        return hours >= start && hours < end;
    } else {
        // Перехід через північ (напр. 22:00 - 08:00)
        return hours >= start || hours < end;
    }
}

/**
 * Форматує дату
 */
function formatDate(date) {
    if (!date) return '-';
    const d = date.toDate ? date.toDate() : new Date(date);
    return d.toLocaleDateString('uk-UA', { day: '2-digit', month: '2-digit' });
}

/**
 * Форматує час
 */
function formatTime(date) {
    if (!date) return '-';
    const d = date.toDate ? date.toDate() : new Date(date);
    return d.toLocaleTimeString('uk-UA', { hour: '2-digit', minute: '2-digit' });
}

/**
 * Отримує назву статусу
 */
function getStatusName(status, orgStatuses = {}) {
    const defaultStatuses = {
        'new': 'Новий',
        'contacted': 'Контакт',
        'scheduled': 'Призначена',
        'completed': 'Проведена',
        'report_sent': 'Звіт відправлено',
        'deposit': 'Завдаток',
        'paid': 'Оплачено',
        'failed': 'Відмова',
        'frozen': 'Заморожений',
        'repeat': 'Повторна'
    };
    
    return orgStatuses[status]?.name || defaultStatuses[status] || status;
}

/**
 * Генерує випадковий код підключення
 */
function generateConnectionCode() {
    return 'TALKO_' + Math.random().toString(36).substring(2, 8).toUpperCase();
}

// ============================================================
// 1. TELEGRAM WEBHOOK - Підключення користувачів
// ============================================================

exports.telegramWebhook = functions.region(REGION).https.onRequest(async (req, res) => {
    // Telegram надсилає POST з даними
    if (req.method !== 'POST') {
        return res.status(405).send('Method not allowed');
    }
    
    const { message } = req.body;
    
    if (!message || !message.text) {
        return res.sendStatus(200);
    }
    
    const chatId = message.chat.id;
    const text = message.text.trim();
    const username = message.from?.username || '';
    const firstName = message.from?.first_name || '';
    
    console.log(`Telegram message from ${chatId}: ${text}`);
    
    // Команда /start
    if (text === '/start') {
        await sendTelegram(chatId,
            `👋 <b>Вітаю в TALKO CRM Bot!</b>\n\n` +
            `Щоб підключити сповіщення:\n` +
            `1. Відкрийте CRM → Налаштування → Telegram\n` +
            `2. Натисніть "Отримати код"\n` +
            `3. Надішліть код сюди\n\n` +
            `Після підключення ви отримуватимете:\n` +
            `• 🔔 Сповіщення про нових лідів\n` +
            `• ⏰ Нагадування про задачі\n` +
            `• 📊 Ранкові звіти`
        );
        return res.sendStatus(200);
    }
    
    // Команда /status
    if (text === '/status') {
        // Шукаємо користувача по chatId
        const orgsSnapshot = await db.collection('organizations').get();
        let found = false;
        
        for (const orgDoc of orgsSnapshot.docs) {
            const teamSnapshot = await db.collection('organizations').doc(orgDoc.id)
                .collection('team')
                .where('telegramChatId', '==', String(chatId))
                .get();
            
            if (!teamSnapshot.empty) {
                const user = teamSnapshot.docs[0].data();
                await sendTelegram(chatId,
                    `✅ <b>Telegram підключено</b>\n\n` +
                    `📧 ${user.email || '-'}\n` +
                    `🏢 Організація: ${orgDoc.data().name || orgDoc.id}\n\n` +
                    `Для відключення напишіть /disconnect`
                );
                found = true;
                break;
            }
        }
        
        if (!found) {
            await sendTelegram(chatId,
                `❌ Telegram не підключено.\n\n` +
                `Надішліть код з CRM для підключення.`
            );
        }
        return res.sendStatus(200);
    }
    
    // Команда /disconnect
    if (text === '/disconnect') {
        const orgsSnapshot = await db.collection('organizations').get();
        let disconnected = false;
        
        for (const orgDoc of orgsSnapshot.docs) {
            const teamSnapshot = await db.collection('organizations').doc(orgDoc.id)
                .collection('team')
                .where('telegramChatId', '==', String(chatId))
                .get();
            
            if (!teamSnapshot.empty) {
                await teamSnapshot.docs[0].ref.update({
                    telegramChatId: admin.firestore.FieldValue.delete(),
                    telegramUsername: admin.firestore.FieldValue.delete(),
                    telegramConnectedAt: admin.firestore.FieldValue.delete()
                });
                disconnected = true;
                break;
            }
        }
        
        if (disconnected) {
            await sendTelegram(chatId, `✅ Telegram відключено від CRM.\n\nНадішліть новий код для підключення.`);
        } else {
            await sendTelegram(chatId, `❌ Telegram не було підключено.`);
        }
        return res.sendStatus(200);
    }
    
    // Команда /help
    if (text === '/help') {
        await sendTelegram(chatId,
            `📖 <b>Команди бота:</b>\n\n` +
            `/start - Почати роботу\n` +
            `/status - Статус підключення\n` +
            `/disconnect - Відключити сповіщення\n` +
            `/help - Ця довідка\n\n` +
            `🔗 Для підключення надішліть код з CRM`
        );
        return res.sendStatus(200);
    }
    
    // Перевіряємо чи це код підключення (формат: TALKO_XXXXXX)
    if (text.startsWith('TALKO_') && text.length >= 10) {
        const code = text.toUpperCase();
        
        // Шукаємо pending connection з цим кодом
        const orgsSnapshot = await db.collection('organizations').get();
        let connected = false;
        
        for (const orgDoc of orgsSnapshot.docs) {
            const teamSnapshot = await db.collection('organizations').doc(orgDoc.id)
                .collection('team')
                .where('telegramPendingCode', '==', code)
                .get();
            
            if (!teamSnapshot.empty) {
                const userDoc = teamSnapshot.docs[0];
                const codeData = userDoc.data();
                
                // Перевіряємо чи код не протермінований (30 хвилин)
                const codeTime = codeData.telegramCodeCreated?.toDate?.() || new Date(0);
                const now = new Date();
                if ((now - codeTime) > 30 * 60 * 1000) {
                    await sendTelegram(chatId, `⏰ Код протермінований.\n\nОтримайте новий код в CRM.`);
                    return res.sendStatus(200);
                }
                
                // Підключаємо
                await userDoc.ref.update({
                    telegramChatId: String(chatId),
                    telegramUsername: username,
                    telegramConnectedAt: admin.firestore.FieldValue.serverTimestamp(),
                    telegramPendingCode: admin.firestore.FieldValue.delete(),
                    telegramCodeCreated: admin.firestore.FieldValue.delete()
                });
                
                await sendTelegram(chatId,
                    `✅ <b>Telegram успішно підключено!</b>\n\n` +
                    `📧 ${codeData.email}\n` +
                    `🏢 ${orgDoc.data().name || 'Організація'}\n\n` +
                    `Тепер ви отримуватимете:\n` +
                    `• 🔔 Сповіщення про нових лідів\n` +
                    `• ⏰ Нагадування про задачі\n` +
                    `• 📊 Ранкові звіти\n\n` +
                    `Налаштувати сповіщення можна в CRM.`
                );
                
                connected = true;
                break;
            }
        }
        
        if (!connected) {
            await sendTelegram(chatId, `❌ Невірний або протермінований код.\n\nОтримайте новий код в CRM → Налаштування → Telegram.`);
        }
        return res.sendStatus(200);
    }
    
    // Невідома команда
    await sendTelegram(chatId,
        `🤔 Невідома команда.\n\n` +
        `Надішліть код з CRM для підключення або /help для довідки.`
    );
    
    return res.sendStatus(200);
});

// ============================================================
// 2. СПОВІЩЕННЯ ПРО НОВИХ ЛІДІВ
// ============================================================

exports.onLeadCreate = functions.region(REGION).firestore
    .document('organizations/{orgId}/leads/{leadId}')
    .onCreate(async (snap, context) => {
        const { orgId, leadId } = context.params;
        const lead = snap.data();
        
        console.log(`New lead created: ${leadId} in org ${orgId}`);
        
        // Отримуємо налаштування організації
        const orgDoc = await db.collection('organizations').doc(orgId).get();
        const org = orgDoc.data() || {};
        const statuses = org.statuses || {};
        
        // Формуємо повідомлення
        let message = `🆕 <b>Новий лід!</b>\n\n`;
        
        if (lead.tg) message += `👤 ${lead.tg}\n`;
        if (lead.name) message += `📝 ${lead.name}\n`;
        if (lead.phone) message += `📞 ${lead.phone}\n`;
        if (lead.email) message += `📧 ${lead.email}\n`;
        if (lead.biz) message += `🏢 ${lead.biz}\n`;
        if (lead.source) message += `📍 Джерело: ${lead.source}\n`;
        if (lead.problem) message += `\n💬 ${lead.problem.substring(0, 200)}${lead.problem.length > 200 ? '...' : ''}\n`;
        
        // Надсилаємо власникам та менеджерам
        const users = await getOrgUsersWithTelegram(orgId, ['owner', 'manager']);
        
        for (const user of users) {
            const settings = getUserNotificationSettings(user);
            
            if (!settings.newLeads) continue;
            if (isQuietHours(settings)) continue;
            
            await sendTelegram(user.telegramChatId, message);
        }
        
        return null;
    });

// ============================================================
// 3. СПОВІЩЕННЯ ПРО ЗМІНИ
// ============================================================

exports.onLeadUpdate = functions.region(REGION).firestore
    .document('organizations/{orgId}/leads/{leadId}')
    .onUpdate(async (change, context) => {
        const { orgId, leadId } = context.params;
        const before = change.before.data();
        const after = change.after.data();
        
        // Отримуємо налаштування організації
        const orgDoc = await db.collection('organizations').doc(orgId).get();
        const org = orgDoc.data() || {};
        const statuses = org.statuses || {};
        
        // Зміна статусу
        if (before.status !== after.status) {
            const statusName = getStatusName(after.status, statuses);
            const message = 
                `📊 <b>Зміна статусу</b>\n\n` +
                `👤 ${after.tg || after.name || after.phone}\n` +
                `${getStatusName(before.status, statuses)} → <b>${statusName}</b>`;
            
            // Надсилаємо assigned user або всім менеджерам
            let targetUsers = [];
            
            if (after.assignedTo) {
                const assigneeDoc = await db.collection('organizations').doc(orgId)
                    .collection('team').doc(after.assignedTo).get();
                if (assigneeDoc.exists && assigneeDoc.data().telegramChatId) {
                    targetUsers.push(assigneeDoc.data());
                }
            } else {
                targetUsers = await getOrgUsersWithTelegram(orgId, ['owner', 'manager']);
            }
            
            for (const user of targetUsers) {
                const settings = getUserNotificationSettings(user);
                if (!settings.statusChanges) continue;
                if (isQuietHours(settings)) continue;
                
                await sendTelegram(user.telegramChatId, message);
            }
        }
        
        // Призначення відповідального
        if (before.assignedTo !== after.assignedTo && after.assignedTo) {
            const assigneeDoc = await db.collection('organizations').doc(orgId)
                .collection('team').doc(after.assignedTo).get();
            
            if (assigneeDoc.exists) {
                const assignee = assigneeDoc.data();
                
                if (assignee.telegramChatId) {
                    const settings = getUserNotificationSettings(assignee);
                    
                    if (settings.assignments && !isQuietHours(settings)) {
                        const message = 
                            `👤 <b>Вам призначено ліда</b>\n\n` +
                            `${after.tg || after.name || 'Без імені'}\n` +
                            (after.phone ? `📞 ${after.phone}\n` : '') +
                            (after.biz ? `🏢 ${after.biz}\n` : '') +
                            `\n📅 Наступна дія: ${after.nextAction || 'Подзвонити'}\n` +
                            `🕐 ${after.nextDate || 'Сьогодні'} ${after.nextTime || ''}`;
                        
                        await sendTelegram(assignee.telegramChatId, message);
                    }
                }
            }
        }
        
        return null;
    });

// ============================================================
// 4. НАГАДУВАННЯ ПРО ЗАДАЧІ (кожні 5 хвилин)
// ============================================================

exports.checkTaskReminders = functions.region(REGION).pubsub
    .schedule('every 5 minutes')
    .timeZone(TIMEZONE)
    .onRun(async (context) => {
        const now = new Date();
        const today = now.toISOString().split('T')[0];
        const currentHour = now.getHours();
        const currentMinute = now.getMinutes();
        const currentTime = `${String(currentHour).padStart(2, '0')}:${String(Math.floor(currentMinute / 5) * 5).padStart(2, '0')}`;
        
        console.log(`Checking reminders for ${today} ${currentTime}`);
        
        const orgsSnapshot = await db.collection('organizations').get();
        
        for (const orgDoc of orgsSnapshot.docs) {
            const orgId = orgDoc.id;
            const org = orgDoc.data();
            
            // Отримуємо ліди з задачами на сьогодні
            const leadsSnapshot = await db.collection('organizations').doc(orgId)
                .collection('leads')
                .where('nextDate', '==', today)
                .get();
            
            for (const leadDoc of leadsSnapshot.docs) {
                const lead = leadDoc.data();
                
                // Перевіряємо час нагадування (за 15 хвилин до)
                if (lead.nextTime) {
                    const [h, m] = lead.nextTime.split(':').map(Number);
                    const taskMinutes = h * 60 + m;
                    const nowMinutes = currentHour * 60 + currentMinute;
                    const diff = taskMinutes - nowMinutes;
                    
                    // Нагадування за 15 хвилин
                    if (diff >= 10 && diff <= 15) {
                        const reminderKey = `reminder_15_${today}`;
                        if (lead[reminderKey]) continue; // Вже надіслано
                        
                        // Визначаємо кому надсилати
                        let targetUsers = [];
                        
                        if (lead.assignedTo) {
                            const assigneeDoc = await db.collection('organizations').doc(orgId)
                                .collection('team').doc(lead.assignedTo).get();
                            if (assigneeDoc.exists && assigneeDoc.data().telegramChatId) {
                                targetUsers.push(assigneeDoc.data());
                            }
                        } else {
                            targetUsers = await getOrgUsersWithTelegram(orgId, ['owner', 'manager']);
                        }
                        
                        for (const user of targetUsers) {
                            const settings = getUserNotificationSettings(user);
                            if (!settings.reminders) continue;
                            
                            await sendTelegram(user.telegramChatId,
                                `⏰ <b>Нагадування через 15 хв!</b>\n\n` +
                                `👤 ${lead.tg || lead.phone}\n` +
                                (lead.biz ? `🏢 ${lead.biz}\n` : '') +
                                `📋 ${lead.nextAction || 'Задача'}\n` +
                                `🕐 ${lead.nextTime}`
                            );
                        }
                        
                        // Позначаємо як надіслано
                        await leadDoc.ref.update({ [reminderKey]: true });
                    }
                }
                
                // Нагадування про консультацію за 1 годину
                if (lead.consult && lead.status === 'scheduled') {
                    const consultDate = lead.consult.split('T')[0];
                    const consultTime = lead.consult.split('T')[1]?.substring(0, 5);
                    
                    if (consultDate === today && consultTime) {
                        const [ch, cm] = consultTime.split(':').map(Number);
                        const consultMinutes = ch * 60 + cm;
                        const nowMinutes = currentHour * 60 + currentMinute;
                        const diff = consultMinutes - nowMinutes;
                        
                        // За 1 годину
                        if (diff >= 55 && diff <= 65) {
                            const reminderKey = `reminder_60_${today}`;
                            if (lead[reminderKey]) continue;
                            
                            let targetUsers = [];
                            
                            if (lead.assignedTo) {
                                const assigneeDoc = await db.collection('organizations').doc(orgId)
                                    .collection('team').doc(lead.assignedTo).get();
                                if (assigneeDoc.exists && assigneeDoc.data().telegramChatId) {
                                    targetUsers.push(assigneeDoc.data());
                                }
                            } else {
                                targetUsers = await getOrgUsersWithTelegram(orgId, ['owner', 'manager']);
                            }
                            
                            for (const user of targetUsers) {
                                const settings = getUserNotificationSettings(user);
                                if (!settings.reminders) continue;
                                
                                await sendTelegram(user.telegramChatId,
                                    `📅 <b>Консультація через 1 годину!</b>\n\n` +
                                    `👤 ${lead.tg || lead.phone}\n` +
                                    (lead.biz ? `🏢 ${lead.biz}\n` : '') +
                                    (lead.phone ? `📞 ${lead.phone}\n` : '') +
                                    `🕐 ${consultTime}`
                                );
                            }
                            
                            await leadDoc.ref.update({ [reminderKey]: true });
                        }
                    }
                }
            }
        }
        
        return null;
    });

// ============================================================
// 5. РАНКОВИЙ ЗВІТ (9:00)
// ============================================================

exports.dailyReport = functions.region(REGION).pubsub
    .schedule('0 9 * * *')
    .timeZone(TIMEZONE)
    .onRun(async (context) => {
        const today = new Date().toISOString().split('T')[0];
        const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
        
        console.log(`Generating daily report for ${today}`);
        
        const orgsSnapshot = await db.collection('organizations').get();
        
        for (const orgDoc of orgsSnapshot.docs) {
            const orgId = orgDoc.id;
            const orgName = orgDoc.data().name || 'Компанія';
            
            // Статистика
            const leadsSnapshot = await db.collection('organizations').doc(orgId)
                .collection('leads').get();
            
            const leads = leadsSnapshot.docs.map(d => d.data());
            
            // Підрахунки
            const stats = {
                total: leads.length,
                new: leads.filter(l => l.status === 'new').length,
                todayTasks: leads.filter(l => l.nextDate === today && !['paid', 'failed', 'frozen'].includes(l.status)).length,
                overdue: leads.filter(l => l.nextDate && l.nextDate < today && !['paid', 'failed', 'frozen'].includes(l.status)).length,
                scheduled: leads.filter(l => (l.status === 'scheduled' || l.status === 'repeat') && l.consult?.startsWith(today)).length,
                deposit: leads.filter(l => l.status === 'deposit').length,
                paidThisMonth: leads.filter(l => l.status === 'paid' && l.finPaidDate?.startsWith(today.slice(0, 7))).length
            };
            
            // Сума завдатків та оплат
            const depositSum = leads
                .filter(l => l.status === 'deposit' && l.finDep)
                .reduce((sum, l) => sum + (Number(l.finDep) || 0), 0);
            
            const paidSum = leads
                .filter(l => l.status === 'paid' && l.finPaidDate?.startsWith(today.slice(0, 7)) && l.finTotal)
                .reduce((sum, l) => sum + (Number(l.finTotal) || 0), 0);
            
            // Формуємо звіт
            const report = 
                `📊 <b>Ранковий звіт — ${orgName}</b>\n` +
                `${today}\n\n` +
                `📋 <b>На сьогодні:</b>\n` +
                `• Задач: ${stats.todayTasks}\n` +
                `• Консультацій: ${stats.scheduled}\n` +
                (stats.overdue > 0 ? `• ⚠️ Прострочено: ${stats.overdue}\n` : '') +
                `\n📈 <b>В роботі:</b>\n` +
                `• Нових лідів: ${stats.new}\n` +
                `• Завдатків: ${stats.deposit}` + (depositSum > 0 ? ` (${depositSum.toLocaleString()} грн)` : '') + `\n` +
                `\n💰 <b>Цього місяця:</b>\n` +
                `• Оплат: ${stats.paidThisMonth}` + (paidSum > 0 ? ` (${paidSum.toLocaleString()} грн)` : '') + `\n` +
                `\n💪 Гарного продуктивного дня!`;
            
            // Надсилаємо звіт
            const users = await getOrgUsersWithTelegram(orgId);
            
            for (const user of users) {
                const settings = getUserNotificationSettings(user);
                if (!settings.dailyReport) continue;
                
                await sendTelegram(user.telegramChatId, report);
            }
        }
        
        return null;
    });

// ============================================================
// 6. WEBHOOK ДЛЯ ЗОВНІШНІХ ЛІДІВ (сайт, лендінги)
// ============================================================

exports.leadWebhook = functions.region(REGION).https.onRequest(async (req, res) => {
    // CORS
    res.set('Access-Control-Allow-Origin', '*');
    res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    
    if (req.method === 'OPTIONS') {
        return res.sendStatus(204);
    }
    
    if (req.method !== 'POST') {
        return res.status(405).json({ error: 'Method not allowed' });
    }
    
    const { 
        orgId,          // ID організації (обов'язково)
        apiKey,         // API ключ організації
        phone,          // Телефон (обов'язково)
        tg,             // Telegram
        email,          // Email
        name,           // Ім'я
        biz,            // Бізнес
        source,         // Джерело
        problem,        // Проблема/запит
        goal,           // Ціль
        funnelId,       // ID воронки
        utm_source,     // UTM мітки
        utm_medium,
        utm_campaign
    } = req.body;
    
    // Валідація
    if (!orgId) {
        return res.status(400).json({ error: 'orgId is required' });
    }
    
    if (!phone && !tg && !email) {
        return res.status(400).json({ error: 'At least one contact (phone, tg, or email) is required' });
    }
    
    // Перевіряємо API ключ (якщо налаштовано)
    const orgDoc = await db.collection('organizations').doc(orgId).get();
    if (!orgDoc.exists) {
        return res.status(404).json({ error: 'Organization not found' });
    }
    
    const org = orgDoc.data();
    if (org.apiKey && org.apiKey !== apiKey) {
        return res.status(401).json({ error: 'Invalid API key' });
    }
    
    // Генеруємо ID
    const leadId = 'lead_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
    
    // Створюємо ліда
    const leadData = {
        id: leadId,
        phone: phone || '',
        tg: tg || '',
        email: email || '',
        name: name || '',
        biz: biz || '',
        source: source || 'Сайт',
        problem: problem || '',
        goal: goal || '',
        funnelId: funnelId || 'default',
        status: 'new',
        calls: 0,
        sms: 0,
        noshow: 0,
        nextDate: new Date().toISOString().split('T')[0],
        nextTime: '10:00',
        nextAction: 'Подзвонити',
        created: admin.firestore.FieldValue.serverTimestamp(),
        createdAt: new Date().toISOString(),
        stageDate: new Date().toISOString(),
        utm: {
            source: utm_source || null,
            medium: utm_medium || null,
            campaign: utm_campaign || null
        },
        log: [{
            type: 'system',
            text: `Лід створено через API (${source || 'Сайт'})`,
            date: new Date().toISOString()
        }]
    };
    
    await db.collection('organizations').doc(orgId)
        .collection('leads').doc(leadId).set(leadData);
    
    console.log(`Lead created via webhook: ${leadId}`);
    
    // Сповіщення надішлеться автоматично через onLeadCreate trigger
    
    return res.status(201).json({ 
        success: true, 
        leadId: leadId,
        message: 'Lead created successfully'
    });
});

// ============================================================
// 7. ГЕНЕРАЦІЯ КОДУ ПІДКЛЮЧЕННЯ (HTTP endpoint для CRM)
// ============================================================

exports.generateTelegramCode = functions.region(REGION).https.onRequest(async (req, res) => {
    // CORS
    res.set('Access-Control-Allow-Origin', '*');
    res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    
    if (req.method === 'OPTIONS') {
        return res.sendStatus(204);
    }
    
    if (req.method !== 'POST') {
        return res.status(405).json({ error: 'Method not allowed' });
    }
    
    const { orgId, memberId, email } = req.body;
    
    if (!orgId || !memberId) {
        return res.status(400).json({ error: 'orgId and memberId are required' });
    }
    
    // Генеруємо код
    const code = generateConnectionCode();
    
    // Зберігаємо в профіль користувача
    await db.collection('organizations').doc(orgId)
        .collection('team').doc(memberId).update({
            telegramPendingCode: code,
            telegramCodeCreated: admin.firestore.FieldValue.serverTimestamp()
        });
    
    return res.json({ 
        success: true, 
        code: code,
        botUsername: 'talko_crm_bot', // Змініть на свій username бота
        expiresIn: '30 minutes'
    });
});

// ============================================================
// ТЕСТОВА ФУНКЦІЯ
// ============================================================

exports.testNotification = functions.region(REGION).https.onRequest(async (req, res) => {
    const { chatId, message } = req.query;
    
    if (!chatId) {
        return res.status(400).json({ error: 'chatId required' });
    }
    
    const result = await sendTelegram(
        chatId, 
        message || '✅ Тестове повідомлення з TALKO CRM!'
    );
    
    return res.json({ success: result });
});
