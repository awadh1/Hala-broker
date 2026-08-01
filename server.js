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
  const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, service: 'hala-broker', clients: aedes.connectedClients, persistent: !!persistence }));
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
