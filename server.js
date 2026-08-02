/* ══════════════════════════════════════════════════════════
   بروكر هلا شات الخاص
   نفس بروتوكول البروكر العام (MQTT عبر WebSocket) اللي يتكلم معه
   تطبيق "هلا شات" أصلاً — فقط نستضيفه إحنا بدل ما نعتمد على سيرفر
   عام مشترك مع آلاف المشاريع الثانية. صفر تغيير على كود التطبيق
   غير عنوان الاتصال.
   ══════════════════════════════════════════════════════════ */

const http = require('http');
const { WebSocketServer, createWebSocketStream } = require('ws');
const { Aedes } = require('aedes');
const aedesPersistenceRedis = require('aedes-persistence-redis');
const Redis = require('ioredis');
const webpush = require('web-push');

// شبكة أمان: أي خطأ برمجي غير متوقع بأي مكان بالسيرفر يُسجَّل بس،
// وما يوقف البرنامج كامل. بدون هذا، أول خطأ غريب (رسالة فاسدة،
// انقطاع غير متوقع) يطفّي السيرفر على كل المتصلين مرة وحدة —
// وهذا أخطر شي وقت عرض حي قدام ناس.
process.on('uncaughtException', (err) => {
  console.error('[خطأ غير متوقع — تم تجاهله عشان السيرفر يكمل]', err && err.message);
});
process.on('unhandledRejection', (err) => {
  console.error('[وعد مرفوض غير متوقع — تم تجاهله]', err && err.message);
});

const MAX_PAYLOAD = 128 * 1024; // ١٢٨ كيلوبايت — أكثر من كافي لأي رسالة نصية، يمنع إساءة استخدام الذاكرة

async function main() {
  // لو فيه رابط Redis (REDIS_URL) بمتغيرات البيئة، نستخدمه كذاكرة دائمة —
  // الحسابات وسجل الرسائل يبقون حتى لو السيرفر أعاد التشغيل أو نام وصحى.
  // بدونه، يشتغل بالذاكرة المؤقتة العادية (يُمسح كل إعادة تشغيل) —
  // مفيد للتجربة المحلية، بس مو للاستخدام الحقيقي.
  var persistence;
  var redisClient = null;
  if (process.env.REDIS_URL) {
    redisClient = new Redis(process.env.REDIS_URL, { maxRetriesPerRequest: null, tls: process.env.REDIS_URL.startsWith('rediss://') ? {} : undefined });
    redisClient.on('error', (e) => console.error('[خطأ اتصال Redis]', e && e.message));
    persistence = aedesPersistenceRedis({ conn: redisClient });
    console.log('💾 ذاكرة دائمة مفعّلة عبر Redis — الحسابات ما تنمسح عند إعادة التشغيل');
  } else {
    console.log('⚠️  ما فيه REDIS_URL — شغّال بذاكرة مؤقتة، كل شي ينمسح عند إعادة التشغيل');
  }

  // تخزين اشتراكات التنبيهات (push): username -> subscription. لو عندنا
  // Redis نحفظها فيه (تبقى بعد إعادة التشغيل)، وإلا بالذاكرة المؤقتة —
  // بس تنمسح عند إعادة التشغيل، نفس أي شي ثاني بدون Redis.
  var MEM_PUSH = {};
  function savePushSub(username, sub) {
    if (redisClient) return redisClient.set('push:' + username, JSON.stringify(sub));
    MEM_PUSH[username] = sub; return Promise.resolve();
  }
  function loadPushSub(username) {
    if (redisClient) return redisClient.get('push:' + username).then(function (v) { return v ? JSON.parse(v) : null; });
    return Promise.resolve(MEM_PUSH[username] || null);
  }

  // إشعارات الدفع (Push) — توصل حتى لو التطبيق مقفول تماماً، مو بس
  // بالخلفية. تحتاج مفتاحين (VAPID) يميّزون سيرفرنا؛ لو ما وصّلناهم،
  // الميزة تسكت بهدوء بدل ما تكسر شي.
  var PUSH_READY = !!(process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY);
  if (PUSH_READY) {
    webpush.setVapidDetails('mailto:admin@example.com', process.env.VAPID_PUBLIC_KEY, process.env.VAPID_PRIVATE_KEY);
  }

  const aedes = await Aedes.createBroker(persistence ? { persistence: persistence } : {});
  const PORT = process.env.PORT || 4001;

  // نرفض أي رسالة أكبر من الحد المسموح قبل ما توصل لباقي المتصلين
  aedes.authorizePublish = (client, packet, callback) => {
    if (packet.payload && packet.payload.length > MAX_PAYLOAD) {
      return callback(new Error('الرسالة أكبر من الحد المسموح'));
    }
    callback(null);
  };

  // فحص صحة بسيط — Render (وأي مراقب خارجي) يستخدمه يتأكد إن السيرفر حي
  // ونقطة نهاية "أكوا" — وسيط بيننا وبين خدمة الذكاء الاصطناعي (Gemini من
  // جوجل، باقتها المجانية). المفتاح السري يبقى هنا بالسيرفر بس، أبداً ما
  // يوصل لمتصفح المستخدم — لو حطيناه بكود التطبيق مباشرة، أي حد يقدر
  // يفتحه من "عرض المصدر" ويسرقه ويستهلك حصتنا المجانية كاملة.
  var AQUA_LIMIT = {};   // clientId -> [timestamps] آخر دقيقة — يمنع إساءة استخدام تفرّغ الحصة اليومية
  var AQUA_SYSTEM = 'اسمك أكوا، مساعد ذكي داخل تطبيق "هلا شات". ' +
    'رد بالعربي دايماً (لهجة خليجية بسيطة ومفهومة)، بإيجاز واضح مناسب لمحادثة، ' +
    'بدون مقدمات طويلة. كن ودود ومباشر ومفيد. مهم: خلّ ردك دايماً جملة أو جملتين ' +
    'قصار ومكتملة — لا تبدأ فكرة وما تكملها، ولا تطوّل بدون داعي.';

  // بوتات المجموعات — شخصية كل نوع محددة مسبقاً من طرفنا (مو نص حر
  // يكتبه أي أدمن)، عشان تبقى آمنة ومضبوطة على موضوعها دايماً مهما
  // كانت المحادثة حواليها. كل بوت يشارك بالمجموعة كعضو له شخصية، بس
  // يركّز على تخصصه ويرجّع أي سؤال بره مجاله بأدب.
  var BOT_BASE_SYSTEM = 'أنت عضو داخل مجموعة دردشة بتطبيق "هلا شات"، ' +
    'وأعضاء المجموعة غالباً طلاب صغار. رد بالعربي (لهجة خليجية بسيطة)، بإيجاز شديد ' +
    '(سطر أو سطرين بس)، وابق دايماً بحدود تخصصك المذكور تحت — لو أحد سألك شي بره ' +
    'تخصصك، اعتذر بلطف وقول له تخصصك وش هو، بدون ما تحاول تجاوب على كل شي.';
  var BOT_SPECIALTIES = {
    poetry: { name: 'بوت الشعر والأدب', prompt: 'تخصصك: الشعر والأدب العربي. تساعد تشرح أبيات، تقترح قوافي، تحلل معنى قصيدة، أو تناقش أسلوب كاتب.' },
    math: { name: 'بوت الرياضيات', prompt: 'تخصصك: الرياضيات. تساعد تحل مسائل، تشرح خطوات الحل بوضوح، وتراجع إجابات الطلاب.' },
    science: { name: 'بوت العلوم', prompt: 'تخصصك: العلوم العامة (فيزياء، كيمياء، أحياء). تشرح مفاهيم علمية بطريقة مبسطة تناسب طالب مدرسة.' },
    english: { name: 'بوت اللغة الإنجليزية', prompt: 'تخصصك: اللغة الإنجليزية. تساعد بالترجمة، تصحيح جمل، شرح قواعد، وتوسيع المفردات.' },
    history: { name: 'بوت التاريخ والجغرافيا', prompt: 'تخصصك: التاريخ والجغرافيا. تجاوب على أسئلة عن أحداث تاريخية، حضارات، دول، وخرائط.' },
    study: { name: 'بوت مساعد المذاكرة العام', prompt: 'تخصصك: مساعدة عامة بالمذاكرة والتنظيم — جدولة وقت المذاكرة، تلخيص دروس، ونصائح تركيز.' },
    football: { name: 'بوت كرة القدم', prompt: 'تخصصك: كرة القدم — بطولات، فرق، لاعبين، وقوانين اللعبة.' },
    anime: { name: 'بوت الأنمي', prompt: 'تخصصك: الأنمي والمانجا — قصص، شخصيات، اقتراحات مشاهدة.' }
  };

  const httpServer = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    // نقطة نهاية سيرفر الترحيل (TURN) — يساعد المكالمات تتصل بسرعة حتى
    // على شبكات صعبة (بيانات جوال، شبكات مقيّدة). المفتاح يبقى هنا بس،
    // ما يوصل للمتصفح أبداً — العميل يطلب منا الإعدادات الجاهزة فقط.
    if (req.method === 'GET' && req.url === '/ice') {
      if (!process.env.METERED_API_KEY || !process.env.METERED_APP_NAME) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ iceServers: [] }));   // بدون إعداد، العميل يرجع للـ STUN الافتراضي بنفسه
        return;
      }
      var turnUrl = 'https://' + process.env.METERED_APP_NAME + '.metered.live/api/v1/turn/credentials?apiKey=' + encodeURIComponent(process.env.METERED_API_KEY);
      fetch(turnUrl)
        .then(function (r) { return r.json(); })
        .then(function (data) {
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ iceServers: Array.isArray(data) ? data : [] }));
        }).catch(function (e) {
          console.error('[TURN] خطأ جلب الإعدادات:', e && e.message);
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ iceServers: [] }));
        });
      return;
    }

  // دالة مشتركة تنادي Gemini بأي نص نظام وأي سؤال — يستخدمها أكوا
  // وبوتات المجموعات الجديدة، بدل ما نكرر نفس منطق المحاولة والموديل
  // الاحتياطي في كل نقطة نهاية لحالها.
  var MODELS = ['gemini-3.6-flash', 'gemini-3.5-flash-lite'];
  function callGemini(systemPrompt, userText, res, logTag) {
    var payload = JSON.stringify({
      contents: [{ parts: [{ text: userText }] }],
      systemInstruction: { parts: [{ text: systemPrompt }] },
      generationConfig: { maxOutputTokens: 800 }
    });
    function tryModel(idx) {
      if (idx >= MODELS.length) {
        res.writeHead(502, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: logTag + ' ما قدر يرد الحين، جرّب بعدين' })); return;
      }
      var ctrl = new AbortController();
      var killer = setTimeout(function () { ctrl.abort(); }, 12000);
      fetch('https://generativelanguage.googleapis.com/v1beta/models/' + MODELS[idx] + ':generateContent', {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-goog-api-key': process.env.GEMINI_API_KEY },
        body: payload,
        signal: ctrl.signal
      }).then(function (r) { clearTimeout(killer); return r.json().then(function (data) { return { status: r.status, data: data }; }); })
        .then(function (res2) {
          if (res2.status === 404) { console.error('[' + logTag + '] موديل', MODELS[idx], 'مو متوفر، نجرّب البديل'); tryModel(idx + 1); return; }
          var reply = res2.data && res2.data.candidates && res2.data.candidates[0] && res2.data.candidates[0].content &&
            res2.data.candidates[0].content.parts && res2.data.candidates[0].content.parts[0] && res2.data.candidates[0].content.parts[0].text;
          if (!reply) {
            console.error('[' + logTag + '] رد غير متوقع من Gemini:', JSON.stringify(res2.data).slice(0, 300));
            res.writeHead(502, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: logTag + ' ما قدر يرد الحين، جرّب بعدين' })); return;
          }
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ reply: reply }));
        }).catch(function (e) {
          clearTimeout(killer);
          console.error('[' + logTag + '] خطأ اتصال:', e && e.message);
          res.writeHead(502, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: logTag + ' ما قدر يرد الحين، جرّب بعدين' }));
        });
    }
    tryModel(0);
  }

  if (req.method === 'POST' && req.url === '/aqua') {
      var chunks = [];
      req.on('data', function (c) { chunks.push(c); if (Buffer.concat(chunks).length > 8000) req.destroy(); });
      req.on('end', function () {
        var body;
        try { body = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'); } catch (e) {
          res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'صيغة غير صحيحة' })); return;
        }
        var who = String(body.who || 'مجهول').slice(0, 40);
        var text = String(body.text || '').trim().slice(0, 1200);
        if (!text) { res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'ما فيه سؤال' })); return; }
        if (!process.env.GEMINI_API_KEY) {
          res.writeHead(503, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'أكوا مو مفعّل بعد على السيرفر' })); return;
        }
        // حد أقصى ٦ رسائل بالدقيقة لكل شخص — يحمي حصتنا اليومية المجانية
        // من الانتهاء بسبب شخص وحد يسبّم أو يجرّب بسرعة
        var now = Date.now();
        var list = (AQUA_LIMIT[who] = (AQUA_LIMIT[who] || []).filter(function (t) { return now - t < 60000; }));
        if (list.length >= 6) {
          res.writeHead(429, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'رسايل كثيرة بوقت قصير، انتظر شوي' })); return;
        }
        list.push(now);
        callGemini(AQUA_SYSTEM, text, res, 'أكوا');
      });
      return;
  }

  if (req.method === 'POST' && req.url === '/bot') {
      var chunks9 = [];
      req.on('data', function (c) { chunks9.push(c); if (Buffer.concat(chunks9).length > 8000) req.destroy(); });
      req.on('end', function () {
        var body;
        try { body = JSON.parse(Buffer.concat(chunks9).toString('utf8') || '{}'); } catch (e) {
          res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'صيغة غير صحيحة' })); return;
        }
        var bot = BOT_SPECIALTIES[body.botId];
        if (!bot) { res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'نوع بوت غير معروف' })); return; }
        var question = String(body.question || '').trim().slice(0, 1200);
        if (!question) { res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'ما فيه سؤال' })); return; }
        if (!process.env.GEMINI_API_KEY) {
          res.writeHead(503, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'البوتات مو مفعّلة بعد على السيرفر' })); return;
        }
        var history = Array.isArray(body.history) ? body.history.slice(-10) : [];
        var who = 'bot:' + (body.botId || '؟');
        var now = Date.now();
        var list = (AQUA_LIMIT[who] = (AQUA_LIMIT[who] || []).filter(function (t) { return now - t < 60000; }));
        if (list.length >= 6) {
          res.writeHead(429, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'رسايل كثيرة بوقت قصير، انتظر شوي' })); return;
        }
        list.push(now);

        var convo = history.map(function (h) { return String(h.name || '؟').slice(0, 24) + ': ' + String(h.text || '').slice(0, 200); }).join('\n');
        var fullText = (convo ? ('سياق آخر رسائل بالمجموعة:\n' + convo + '\n\n') : '') + 'آخر رسالة توجّهت لك: ' + question;
        callGemini(BOT_BASE_SYSTEM + ' ' + bot.prompt, fullText, res, bot.name);
      });
      return;
  }

    if (req.method === 'POST' && req.url === '/push/subscribe') {
      var chunks5 = [];
      req.on('data', function (c) { chunks5.push(c); if (Buffer.concat(chunks5).length > 6000) req.destroy(); });
      req.on('end', function () {
        var body;
        try { body = JSON.parse(Buffer.concat(chunks5).toString('utf8') || '{}'); } catch (e) {
          res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'صيغة غير صحيحة' })); return;
        }
        var username = String(body.username || '').trim().toLowerCase();
        if (!/^[a-z0-9_]{3,20}$/.test(username) || !body.subscription || !body.subscription.endpoint) {
          res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'بيانات ناقصة' })); return;
        }
        savePushSub(username, body.subscription).then(function () {
          res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: true }));
        }).catch(function () {
          res.writeHead(500, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'ما قدرنا نحفظ الاشتراك' }));
        });
      });
      return;
    }

    if (req.method === 'POST' && req.url === '/push/notify') {
      if (!PUSH_READY) { res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: false, reason: 'push غير مفعّل' })); return; }
      var chunks6 = [];
      req.on('data', function (c) { chunks6.push(c); if (Buffer.concat(chunks6).length > 4000) req.destroy(); });
      req.on('end', function () {
        var body;
        try { body = JSON.parse(Buffer.concat(chunks6).toString('utf8') || '{}'); } catch (e) {
          res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'صيغة غير صحيحة' })); return;
        }
        var toUsername = String(body.toUsername || '').trim().toLowerCase();
        var title = String(body.title || 'هلا شات').slice(0, 60);
        var text = String(body.body || '').slice(0, 140);
        var tag = String(body.tag || 'hala').slice(0, 30);
        if (!toUsername) { res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'بيانات ناقصة' })); return; }
        loadPushSub(toUsername).then(function (sub) {
          if (!sub) { res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: false, reason: 'مو مشترك' })); return; }
          webpush.sendNotification(sub, JSON.stringify({ title: title, body: text, tag: tag }))
            .then(function () { res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: true })); })
            .catch(function (e) {
              // الاشتراك ممكن يصير قديم/منتهي (المستخدم مسح بيانات المتصفح
              // مثلاً) — نتجاهل الخطأ بهدوء، ما يستاهل نوقف السيرفر عشانه
              console.error('[push] فشل الإرسال:', e && e.message);
              res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: false, reason: 'فشل الإرسال' }));
            });
        });
      });
      return;
    }

    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, service: 'hala-broker', clients: aedes.connectedClients, persistent: !!persistence, aqua: !!process.env.GEMINI_API_KEY, push: PUSH_READY }));
  });

  const wss = new WebSocketServer({ server: httpServer });
  wss.on('connection', (websocket, req) => {
    const stream = createWebSocketStream(websocket);
    // اتصال واحد يفشل ما لازم يأثر على البرنامج كامل ولا على باقي المتصلين
    websocket.on('error', () => {});
    stream.on('error', () => {});
    aedes.handle(stream, req);
  });

  httpServer.listen(PORT, () => {
    console.log('🫖 بروكر هلا شات شغّال على المنفذ', PORT);
  });

  // سجل مختصر بالتيرمنال — يفيدك تتأكد إن الرسائل توصل فعلاً وأنت تراقب
  aedes.on('client', (c) => console.log('دخل:', c.id));
  aedes.on('clientDisconnect', (c) => console.log('طلع:', c.id));
  aedes.on('publish', (packet, c) => {
    if (c && !packet.topic.startsWith('$SYS')) console.log('نشر:', c.id, '←', packet.topic);
  });
}

main();
