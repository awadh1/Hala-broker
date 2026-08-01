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
  if (process.env.REDIS_URL) {
    var redisClient = new Redis(process.env.REDIS_URL, { maxRetriesPerRequest: null, tls: process.env.REDIS_URL.startsWith('rediss://') ? {} : undefined });
    redisClient.on('error', (e) => console.error('[خطأ اتصال Redis]', e && e.message));
    persistence = aedesPersistenceRedis({ conn: redisClient });
    console.log('💾 ذاكرة دائمة مفعّلة عبر Redis — الحسابات ما تنمسح عند إعادة التشغيل');
  } else {
    console.log('⚠️  ما فيه REDIS_URL — شغّال بذاكرة مؤقتة، كل شي ينمسح عند إعادة التشغيل');
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
    'بدون مقدمات طويلة. كن ودود ومباشر ومفيد.';

  const httpServer = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

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

        var payload = JSON.stringify({
          contents: [{ parts: [{ text: text }] }],
          systemInstruction: { parts: [{ text: AQUA_SYSTEM }] },
          generationConfig: { maxOutputTokens: 400 }
        });
        // جوجل توقف موديلات وتطلق ثانية بشكل متكرر — لو الأساسي رجع "غير
        // متوفر" (404)، نجرّب موديل احتياطي تلقائياً بدل ما نرجع نعلّق
        // على تحديث كود يدوي في كل مرة.
        var MODELS = ['gemini-3.6-flash', 'gemini-3.5-flash-lite'];
        function tryModel(idx) {
          if (idx >= MODELS.length) {
            res.writeHead(502, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'أكوا ما قدر يرد الحين، جرّب بعدين' })); return;
          }
          var ctrl = new AbortController();
          var killer = setTimeout(function () { ctrl.abort(); }, 12000);   // ما ننتظر جيميناي أكثر من ١٢ ثانية
          fetch('https://generativelanguage.googleapis.com/v1beta/models/' + MODELS[idx] + ':generateContent', {
            method: 'POST',
            headers: { 'content-type': 'application/json', 'x-goog-api-key': process.env.GEMINI_API_KEY },
            body: payload,
            signal: ctrl.signal
          }).then(function (r) { clearTimeout(killer); return r.json().then(function (data) { return { status: r.status, data: data }; }); })
            .then(function (res2) {
              if (res2.status === 404) {
                console.error('[أكوا] موديل', MODELS[idx], 'مو متوفر، نجرّب البديل');
                tryModel(idx + 1); return;
              }
              var reply = res2.data && res2.data.candidates && res2.data.candidates[0] && res2.data.candidates[0].content &&
                res2.data.candidates[0].content.parts && res2.data.candidates[0].content.parts[0] && res2.data.candidates[0].content.parts[0].text;
              if (!reply) {
                console.error('[أكوا] رد غير متوقع من Gemini:', JSON.stringify(res2.data).slice(0, 300));
                res.writeHead(502, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'أكوا ما قدر يرد الحين، جرّب بعدين' })); return;
              }
              res.writeHead(200, { 'content-type': 'application/json' });
              res.end(JSON.stringify({ reply: reply }));
            }).catch(function (e) {
              clearTimeout(killer);
              console.error('[أكوا] خطأ اتصال:', e && e.message);
              res.writeHead(502, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'أكوا ما قدر يرد الحين، جرّب بعدين' }));
            });
        }
        tryModel(0);
      });
      return;
    }

    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, service: 'hala-broker', clients: aedes.connectedClients, persistent: !!persistence, aqua: !!process.env.GEMINI_API_KEY }));
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
