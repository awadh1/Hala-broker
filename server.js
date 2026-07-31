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

async function main() {
  const aedes = await Aedes.createBroker();
  const PORT = process.env.PORT || 4001;

  // فحص صحة بسيط — Render (وأي مراقب خارجي) يستخدمه يتأكد إن السيرفر حي
  const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, service: 'hala-broker', clients: aedes.connectedClients }));
  });

  const wss = new WebSocketServer({ server: httpServer });
  wss.on('connection', (websocket, req) => {
    const stream = createWebSocketStream(websocket);
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
