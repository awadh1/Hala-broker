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

process.on('uncaughtException', (err) => {
  console.error('[خطأ غير متوقع — تم تجاهله عشان السيرفر يكمل]', err && err.message);
});
process.on('unhandledRejection', (err) => {
  console.error('[وعد مرفوض غير متوقع — تم تجاهله]', err && err.message);
});

const MAX_PAYLOAD = 128 * 1024;

async function main() {
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

  var MEM_PUSH = {};
  function savePushSub(username, sub) {
    if (redisClient) return redisClient.set('push:' + username, JSON.stringify(sub));
    MEM_PUSH[username] = sub; return Promise.resolve();
  }
  function loadPushSub(username) {
    if (redisClient) return redisClient.get('push:' + username).then(function (v) { return v ? JSON.parse(v) : null; });
    return Promise.resolve(MEM_PUSH[username] || null);
  }

  var MEM_REPORTS = [];
  var MEM_BANNED = [];
  function addReport(r) {
    if (redisClient) return redisClient.lpush('reports', JSON.stringify(r)).then(function () { return redisClient.ltrim('reports', 0, 499); });
    MEM_REPORTS.unshift(r); if (MEM_REPORTS.length > 500) MEM_REPORTS.pop();
    return Promise.resolve();
  }
  function loadReports() {
    if (redisClient) return redisClient.lrange('reports', 0, 199).then(function (list) { return list.map(function (s) { try { return JSON.parse(s); } catch (e) { return null; } }).filter(Boolean); });
    return Promise.resolve(MEM_REPORTS.slice(0, 200));
  }
  function loadBanned() {
    if (redisClient) return redisClient.smembers('banned');
    return Promise.resolve(MEM_BANNED.slice());
  }
  function banUser(username) {
    if (redisClient) return redisClient.sadd('banned', username);
    if (MEM_BANNED.indexOf(username) < 0) MEM_BANNED.push(username);
    return Promise.resolve();
  }
  function unbanUser(username) {
    if (redisClient) return redisClient.srem('banned', username);
    var i = MEM_BANNED.indexOf(username); if (i >= 0) MEM_BANNED.splice(i, 1);
    return Promise.resolve();
  }
  function broadcastBanned() {
    loadBanned().then(function (list) {
      aedes.publish({ cmd: 'publish', topic: 'halachat9x/banned', payload: Buffer.from(JSON.stringify(list)), qos: 1, retain: true, dup: false }, function (err) {
        if (err) console.error('[حظر] فشل بث القائمة:', err.message);
      });
    });
  }

  var PUSH_READY = !!(process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY);
  if (PUSH_READY) {
    webpush.setVapidDetails('mailto:admin@example.com', process.env.VAPID_PUBLIC_KEY, process.env.VAPID_PRIVATE_KEY);
  }

  const aedes = await Aedes.createBroker(persistence ? { persistence: persistence } : {});
  const PORT = process.env.PORT || 4001;

  aedes.authorizePublish = (client, packet, callback) => {
    if (packet.payload && packet.payload.length > MAX_PAYLOAD) {
      return callback(new Error('الرسالة أكبر من الحد المسموح'));
    }
    callback(null);
  };

  var AQUA_LIMIT = {};
  var AQUA_SYSTEM = 'اسمك أكوا، مساعد ذكي داخل تطبيق "هلا شات". ' +
    'رد بالعربي دايماً (لهجة خليجية بسيطة ومفهومة)، بإيجاز واضح مناسب لمحادثة، ' +
    'بدون مقدمات طويلة. كن ودود ومباشر ومفيد. مهم: خلّ ردك دايماً جملة أو جملتين ' +
    'قصار ومكتملة — لا تبدأ فكرة وما تكملها، ولا تطوّل بدون داعي.';

  var BOT_BASE_SYSTEM = 'أنت عضو داخل مجموعة دردشة بتطبيق "هلا شات"، ' +
    'وأعضاء المجموعة غالباً طلاب صغار. رد بالعربي (لهجة خليجية بسيطة)، بإيجاز شديد ' +
    '(سطر أو سطرين بس)، وابق دايماً بحدود تخصصك المذكور تحت — لو أحد سألك شي بره ' +
    'تخصصك، اعتذر بلطف وقول له تخصصك وش هو، بدون ما تحاول تجاوب على كل شي.';
  var STUDY_SUBJECTS = { science: 'العلوم', social: 'الاجتماعيات', islamic: 'التربية الإسلامية' };
  var STUDY_BASE_SYSTEM = 'أنت «المساعد الدراسي»، معلّم كويتي لطلاب الصف الثامن. مصدرك الوحيد هو المقاطع المرفقة من الكتاب (الرمز [ص ..] يدل على رقم الصفحة، اذكره بين قوسين مثل (ص 64) لما تحدد مكان المعلومة). ' +
    'قواعد صارمة: ' +
    '1) لا تضف أي معلومة غير موجودة في المقاطع، ولا تستخدم معرفتك العامة. لو ما لقيت الجواب فيها قل: «ما لقيت هالمعلومة في المقاطع اللي عندي من الكتاب» واقترح الدرس القريب. ' +
    '2) سؤال تعريف أو معلومة محددة: انقل نص الكتاب حرفياً بدون تغيير وبدون مقدمة. المقاطع مقروءة بالـOCR، فلو كلمة فيها خطأ قراءة واضح صححها إملائياً فقط. ' +
    '3) اشرح/وضح/لخص/فهمني: اشرح الدرس خطوة خطوة بلغة سهلة لطالب الصف الثامن، في نقاط قصيرة مرقمة، مع الحفاظ على كل معلومة ورقم ومصطلح من الكتاب وإبراز المصطلحات بـ **، وبعده سطر «الخلاصة». ' +
    '4) أسئلة التمارين (أكمل، علل، قارن، صح أو خطأ، اختر، فسّر، ما سبب...): حلّ السؤال اعتماداً على نص الكتاب فقط. اكتب الجواب النهائي أولاً ثم سطراً «من الكتاب:» بنص قصير يدعمه. لو السؤال حسابي طبّق قانون الكتاب وبيّن الخطوات. ' +
    '5) طلب أسئلة/اختبرني/أسئلة متوقعة: اكتب 5 أسئلة متنوعة من نفس الدرس (تعريف، علل، صح أو خطأ، أكمل) مرقّمة بدون إجاباتها، واطلب من الطالب يكتب إجابته لتصحّحها. ' +
    '6) لو الطالب أرسل إجابة لسؤال سابق: صحّحها من الكتاب، وقل صح أو خطأ ثم الجواب الصحيح. ' +
    '7) الأسلوب: عربية مبسطة وودودة، ولا تتجاوز 600 كلمة. ' +
    '8) في آخر كل رد اكتب سطراً منفصلاً فيه [[أسئلة]] ثم ثلاثة أسئلة قصيرة (كل سؤال في سطر) يمكن الإجابة عنها من الكتاب وقريبة من الموضوع. لا تكتب شيئاً بعدها.';
  function studyIntent(q) {
    var has = function (arr) { for (var i = 0; i < arr.length; i++) { if (q.indexOf(arr[i]) >= 0) return true; } return false; };
    if (has(['اختبرني', 'أسئلة', 'اسئلة', 'متوقعة', 'متوقعه', 'اختبار'])) return { k: 'quiz', n: 5, label: 'طلب أسئلة تدريبية' };
    if (has(['اشرح', 'شرح', 'وضح', 'فهمني', 'لخص', 'تلخيص', 'ابسط'])) return { k: 'explain', n: 5, label: 'طلب شرح' };
    if (has(['أكمل', 'اكمل', 'علل', 'علّل', 'قارن', 'املأ', 'صح أو', 'صح او', 'اختر', 'فسر', 'فسّر', 'ما سبب', 'لماذا', 'ليش', 'احسب', 'حل '])) return { k: 'solve', n: 4, label: 'حل سؤال تمرين' };
    return { k: 'fact', n: 3, label: 'سؤال معلومة أو تعريف' };
  }
  function studyClean(q) {
    var w = ['أسئلة', 'اسئلة', 'اختبرني', 'متوقعة', 'متوقعه', 'اشرح', 'شرح', 'وضح', 'فهمني', 'لخص', 'درس', 'الدرس', 'أكمل', 'اكمل', 'علل', 'قارن'];
    var parts = q.split(' ').filter(function (x) { return w.indexOf(x) < 0; });
    return parts.join(' ');
  }
  function shapeReply(t) {
    var m = t.match(/\[\[\s*أسئلة\s*\]\]/);
    if (!m) return { reply: t.trim() };
    var rest = t.slice(m.index + m[0].length).split('\n').map(function (x) { return x.replace(/^[\s\-*•\d.)]+/, '').replace(/[*]+/g, '').trim(); }).filter(function (x) { return x.length > 3 && x.length < 140; }).slice(0, 3);
    return { reply: t.slice(0, m.index).trim(), suggestions: rest };
  }
  var MEM_STUDY = {};
  function clearStudy(subject) {
    if (redisClient) return redisClient.del('study:' + subject);
    MEM_STUDY[subject] = []; return Promise.resolve();
  }
  function addStudyChunk(subject, text) {
    if (redisClient) return redisClient.rpush('study:' + subject, text);
    MEM_STUDY[subject] = MEM_STUDY[subject] || []; MEM_STUDY[subject].push(text); return Promise.resolve();
  }
  function loadStudyChunks(subject) {
    if (redisClient) return redisClient.lrange('study:' + subject, 0, -1);
    return Promise.resolve((MEM_STUDY[subject] || []).slice());
  }
  function pickRelevantChunks(chunks, question, topN) {
    function nrm(s) {
      var o = '';
      for (var i = 0; i < s.length; i++) {
        var ch = s.charCodeAt(i);
        if (ch >= 1611 && ch <= 1618) continue;
        if (ch === 1571 || ch === 1573 || ch === 1570) ch = 1575;
        else if (ch === 1609) ch = 1610;
        else if (ch === 1577) ch = 1607;
        o += String.fromCharCode(ch);
      }
      return o;
    }
    function toks(s) {
      var out = [], cur = '';
      s = nrm(s);
      for (var i = 0; i < s.length; i++) {
        var ch = s.charCodeAt(i);
        var isL = (ch >= 1569 && ch <= 1610) || (ch >= 48 && ch <= 57) || (ch >= 97 && ch <= 122) || (ch >= 65 && ch <= 90);
        if (isL) cur += s.charAt(i);
        else { if (cur) out.push(cur); cur = ''; }
      }
      if (cur) out.push(cur);
      return out;
    }
    function stem(w) { return (w.length > 4 && w.indexOf('ال') === 0) ? w.slice(2) : w; }
    var STOP = nrm(' ما هو هي ماذا من في على الى عن هل كيف لماذا ليش اشرح وضح فهمني اذكر عدد هذا هذه التي الذي وما ماهو ماهي ان كل تعريف عرف مفهوم معنى المقصود المراد اعط اعطني قل لي ');
    var qs = [];
    toks(question).forEach(function (w) { var st = stem(w); if (st.length >= 2 && STOP.indexOf(' ' + w + ' ') < 0) qs.push(st); });
    var docs = chunks.map(function (c) { return ' ' + toks(c).map(stem).join(' ') + ' '; });
    var N = chunks.length;
    function pat(q) { return q.length >= 5 ? ' ' + q : ' ' + q + ' '; }
    function cnt(dd, q) {
      var p = pat(q), n = 0, i = dd.indexOf(p);
      while (i >= 0) { n++; i = dd.indexOf(p, i + p.length - 1); }
      return n;
    }
    var weights = qs.map(function (q) {
      var df = 0;
      docs.forEach(function (dd) { if (cnt(dd, q) > 0) df++; });
      return df > 0 ? Math.log(1 + N / df) : 0;
    });
    var scored = chunks.map(function (c, idx) {
      var dd = docs[idx], score = 0;
      qs.forEach(function (q, i) { var t = cnt(dd, q); if (weights[i] > 0 && t > 0) score += weights[i] * (1 + 0.25 * Math.log(t)); });
      for (var i = 0; i + 1 < qs.length; i++) { if (dd.indexOf(' ' + qs[i] + ' ' + qs[i + 1]) >= 0) score += 3; }
      if (c.indexOf('\u0627\u0644\u0633\u0624\u0627\u0644') >= 0 || c.indexOf('....') >= 0) score *= 0.6;
      return { c: c, score: score };
    });
    scored.sort(function (a, b) { return b.score - a.score; });
    return scored.filter(function (s) { return s.score > 0; }).slice(0, topN).map(function (s) { return s.c; });
  }

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

    if (req.method === 'GET' && req.url === '/ice') {
      if (!process.env.METERED_API_KEY || !process.env.METERED_APP_NAME) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ iceServers: [] }));
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

  var GEMINI_MODELS = ['gemini-3.6-flash', 'gemini-3.5-flash-lite'];
  var GROQ_MODELS = ['openai/gpt-oss-120b', 'openai/gpt-oss-20b'];

  function tryGemini(systemPrompt, userText, done) {
    if (!process.env.GEMINI_API_KEY) { done(false); return; }
    var payload = JSON.stringify({
      contents: [{ parts: [{ text: userText }] }],
      systemInstruction: { parts: [{ text: systemPrompt }] },
      generationConfig: { maxOutputTokens: 2000 }
    });
    function tryModel(idx) {
      if (idx >= GEMINI_MODELS.length) { done(false); return; }
      var ctrl = new AbortController();
      var killer = setTimeout(function () { ctrl.abort(); }, 28000);
      fetch('https://generativelanguage.googleapis.com/v1beta/models/' + GEMINI_MODELS[idx] + ':generateContent', {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-goog-api-key': process.env.GEMINI_API_KEY },
        body: payload,
        signal: ctrl.signal
      }).then(function (r) { clearTimeout(killer); return r.json().then(function (data) { return { status: r.status, data: data }; }); })
        .then(function (res2) {
          if (res2.status === 404) { tryModel(idx + 1); return; }
          if (res2.status === 429) { console.error('[Gemini] تجاوزت الحصة، ننتقل لـ Groq لو متوفر'); done(false, null, 'quota'); return; }
          var reply = res2.data && res2.data.candidates && res2.data.candidates[0] && res2.data.candidates[0].content &&
            res2.data.candidates[0].content.parts && res2.data.candidates[0].content.parts[0] && res2.data.candidates[0].content.parts[0].text;
          if (!reply) { console.error('[Gemini] رد غير متوقع:', JSON.stringify(res2.data).slice(0, 300)); done(false, null, 'error'); return; }
          done(true, reply);
        }).catch(function (e) { clearTimeout(killer); console.error('[Gemini] خطأ اتصال:', e && e.message); done(false, null, 'error'); });
    }
    tryModel(0);
  }

  function tryGroq(systemPrompt, userText, done) {
    if (!process.env.GROQ_API_KEY) { done(false); return; }
    function tryModel(idx) {
      if (idx >= GROQ_MODELS.length) { done(false); return; }
      var ctrl = new AbortController();
      var killer = setTimeout(function () { ctrl.abort(); }, 28000);
      fetch('https://api.groq.com/openai/v1/chat/completions', {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'authorization': 'Bearer ' + process.env.GROQ_API_KEY },
        body: JSON.stringify({ model: GROQ_MODELS[idx], messages: [{ role: 'system', content: systemPrompt }, { role: 'user', content: userText }], max_tokens: 2000 }),
        signal: ctrl.signal
      }).then(function (r) { clearTimeout(killer); return r.json().then(function (data) { return { status: r.status, data: data }; }); })
        .then(function (res2) {
          if (res2.status === 404) { tryModel(idx + 1); return; }
          if (res2.status === 429) { console.error('[Groq] تجاوزت الحصة'); done(false, null, 'quota'); return; }
          var reply = res2.data && res2.data.choices && res2.data.choices[0] && res2.data.choices[0].message && res2.data.choices[0].message.content;
          if (!reply) { console.error('[Groq] رد غير متوقع:', JSON.stringify(res2.data).slice(0, 300)); done(false, null, 'error'); return; }
          done(true, reply);
        }).catch(function (e) { clearTimeout(killer); console.error('[Groq] خطأ اتصال:', e && e.message); done(false, null, 'error'); });
    }
    tryModel(0);
  }

  function callGemini(systemPrompt, userText, res, logTag) {
    if (!process.env.GEMINI_API_KEY && !process.env.GROQ_API_KEY) {
      res.writeHead(503, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: logTag + ' مو مفعّل بعد على السيرفر' })); return;
    }
    tryGemini(systemPrompt, userText, function (ok1, reply1, reason1) {
      if (ok1) { res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify(shapeReply(reply1))); return; }
      tryGroq(systemPrompt, userText, function (ok2, reply2, reason2) {
        if (ok2) { res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify(shapeReply(reply2))); return; }
        var bothQuota = reason1 === 'quota' && (reason2 === 'quota' || !process.env.GROQ_API_KEY);
        var msg = bothQuota
          ? logTag + ' وصل الحد اليومي المجاني اليوم — جرّب بكرة، أو استخدم المحادثة العادية بالتطبيق بدالها'
          : logTag + ' ما قدر يرد الحين، جرّب بعدين';
        res.writeHead(502, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: msg }));
      });
    });
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
        if (!process.env.GEMINI_API_KEY && !process.env.GROQ_API_KEY) {
          res.writeHead(503, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'أكوا مو مفعّل بعد على السيرفر' })); return;
        }
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
        if (!process.env.GEMINI_API_KEY && !process.env.GROQ_API_KEY) {
          res.writeHead(503, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'البوتات مو مفعّلة بعد على السيرفر' })); return;
        }
        var history = Array.isArray(body.history) ? body.history.slice(-10) : [];
        var who = 'bot:' + (body.botId || '؟');
        var now = Date.now();
        var list = (AQUA_LIMIT[who] = (AQUA_LIMIT[who] || []).filter(function (t) { return now - t < 60000; }));
        if (list.length >= 12) {
          res.writeHead(429, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'المجموعة سألت البوت كثير بوقت قصير، انتظروا شوي' })); return;
        }
        list.push(now);

        var convo = history.map(function (h) { return String(h.name || '؟').slice(0, 24) + ': ' + String(h.text || '').slice(0, 200); }).join('\n');
        var fullText = (convo ? ('سياق آخر رسائل بالمجموعة:\n' + convo + '\n\n') : '') + 'آخر رسالة توجّهت لك: ' + question;
        callGemini(BOT_BASE_SYSTEM + ' ' + bot.prompt, fullText, res, bot.name);
      });
      return;
  }

    if (req.method === 'POST' && req.url === '/study/add') {
      var chunksSA = [];
      req.on('data', function (c) { chunksSA.push(c); if (Buffer.concat(chunksSA).length > 60000) req.destroy(); });
      req.on('end', function () {
        var body;
        try { body = JSON.parse(Buffer.concat(chunksSA).toString('utf8') || '{}'); } catch (e) {
          res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'صيغة غير صحيحة' })); return;
        }
        if (!process.env.ADMIN_KEY || body.key !== process.env.ADMIN_KEY) {
          res.writeHead(403, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'ممنوع' })); return;
        }
        var subject = String(body.subject || '').trim();
        var text = String(body.text || '').trim().slice(0, 4000);
        if (!STUDY_SUBJECTS[subject] || !text) { res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'بيانات ناقصة' })); return; }
        (body.reset ? clearStudy(subject) : Promise.resolve()).then(function () { return addStudyChunk(subject, text); }).then(function () {
          res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: true }));
        }).catch(function () {
          res.writeHead(500, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'ما قدرنا نحفظ' }));
        });
      });
      return;
    }

    if (req.method === 'POST' && req.url === '/study/ask') {
      var chunksSQ = [];
      req.on('data', function (c) { chunksSQ.push(c); if (Buffer.concat(chunksSQ).length > 12000) req.destroy(); });
      req.on('end', function () {
        var body;
        try { body = JSON.parse(Buffer.concat(chunksSQ).toString('utf8') || '{}'); } catch (e) {
          res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'صيغة غير صحيحة' })); return;
        }
        var subject = String(body.subject || '').trim();
        var question = String(body.question || '').trim().slice(0, 1200);
        if (!STUDY_SUBJECTS[subject]) { res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'مادة غير معروفة' })); return; }
        if (!question) { res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'ما فيه سؤال' })); return; }
        var who = 'study:' + subject;
        var now = Date.now();
        var list = (AQUA_LIMIT[who] = (AQUA_LIMIT[who] || []).filter(function (t) { return now - t < 60000; }));
        if (list.length >= 12) {
          res.writeHead(429, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'أسئلة كثيرة بوقت قصير، انتظر شوي' })); return;
        }
        list.push(now);
        loadStudyChunks(subject).then(function (chunks) {
          var hist = Array.isArray(body.history) ? body.history.slice(-4) : [];
          var prevUser = ''; for (var hi = hist.length - 1; hi >= 0; hi--) { if (hist[hi] && hist[hi].r === 'u') { prevUser = String(hist[hi].t || '').slice(0, 200); break; } }
          var intent = studyIntent(question);
          var qForSearch = studyClean(question);
          if (qForSearch.length < 25 && prevUser) qForSearch = qForSearch + ' ' + studyClean(prevUser);
          var picked = pickRelevantChunks(chunks, qForSearch, intent.n);
          var context = picked.length ? ('مقاطع من كتاب مادة ' + STUDY_SUBJECTS[subject] + ':\n' + picked.map(function (c, i) { return '(' + (i + 1) + ') ' + c; }).join('\n\n') + '\n\n') : '';
          var histTxt = hist.length ? ('المحادثة السابقة:\n' + hist.map(function (h) { return (h && h.r === 'u' ? 'الطالب: ' : 'المساعد: ') + String((h && h.t) || '').slice(0, 700); }).join('\n') + '\n\n') : '';
          var sys = STUDY_BASE_SYSTEM + ' مادتك: ' + STUDY_SUBJECTS[subject] + '.' + (picked.length ? '' : ' ما لقيت مقاطع مرتبطة بالسؤال في الكتاب المخزّن، فاعتذر وقل ما لقيت الجواب في الكتاب واقترح على الطالب يعيد صياغة سؤاله أو يذكر اسم الدرس.');
          callGemini(sys, context + histTxt + 'نوع الطلب: ' + intent.label + '\nسؤال الطالب: ' + question, res, STUDY_SUBJECTS[subject]);
        });
      });
      return;
    }

    if (req.method === 'POST' && req.url === '/report') {
      var chunksR = [];
      req.on('data', function (c) { chunksR.push(c); if (Buffer.concat(chunksR).length > 4000) req.destroy(); });
      req.on('end', function () {
        var body;
        try { body = JSON.parse(Buffer.concat(chunksR).toString('utf8') || '{}'); } catch (e) {
          res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'صيغة غير صحيحة' })); return;
        }
        var reported = String(body.reportedUsername || '').trim().toLowerCase().slice(0, 20);
        if (!reported) { res.writeHead(400, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'بيانات ناقصة' })); return; }
        var report = {
          reportedUsername: reported,
          reporterUsername: String(body.reporterUsername || '؟').trim().toLowerCase().slice(0, 20),
          roomCode: String(body.roomCode || '').slice(0, 80),
          reason: String(body.reason || '').slice(0, 300),
          at: new Date().toISOString()
        };
        addReport(report).then(function () {
          res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: true }));
        }).catch(function () {
          res.writeHead(500, { 'content-type': 'application/json' }); res.end(JSON.stringify({ error: 'ما قدرنا نحفظ البلاغ' }));
        });
      });
      return;
    }

    if (req.url && req.url.indexOf('/admin') === 0) {
      var u = new URL(req.url, 'http://x');
      var key = u.searchParams.get('key');
      if (!process.env.ADMIN_KEY || key !== process.env.ADMIN_KEY) {
        res.writeHead(403, { 'content-type': 'text/plain; charset=utf-8' }); res.end('ممنوع — مفتاح غلط أو ناقص'); return;
      }

      if (req.method === 'GET' && u.pathname === '/admin') {
        Promise.all([loadReports(), loadBanned()]).then(function (results) {
          var reports = results[0], banned = results[1];
          var rows = reports.map(function (r) {
            var already = banned.indexOf(r.reportedUsername) >= 0;
            return '<tr><td>' + r.at.slice(0, 16).replace('T', ' ') + '</td><td>' + r.reportedUsername + '</td><td>' + r.reporterUsername +
              '</td><td>' + r.roomCode + '</td><td>' + (r.reason || '—') + '</td><td>' +
              (already
                ? '<form method="POST" action="/admin/unban?key=' + key + '"><input type="hidden" name="username" value="' + r.reportedUsername + '"><button style="background:#2FBF71">محظور — إلغاء</button></form>'
                : '<form method="POST" action="/admin/ban?key=' + key + '"><input type="hidden" name="username" value="' + r.reportedUsername + '"><button style="background:#D6353F;color:#fff">احظر</button></form>')
              + '</td></tr>';
          }).join('');
          var bannedRows = banned.map(function (b) {
            return '<li>' + b + ' <form style="display:inline" method="POST" action="/admin/unban?key=' + key + '"><input type="hidden" name="username" value="' + b + '"><button>إلغاء الحظر</button></form></li>';
          }).join('');
          res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
          res.end('<!doctype html><html dir="rtl" lang="ar"><meta charset="utf-8"><title>إدارة هلا شات</title>' +
            '<style>body{font-family:sans-serif;padding:16px;background:#111;color:#eee}table{width:100%;border-collapse:collapse;font-size:14px}td,th{padding:8px;border-bottom:1px solid #333;text-align:right}button{padding:6px 12px;border-radius:6px;border:none;cursor:pointer;background:#333;color:#eee}h2{margin-top:30px}</style>' +
            '<h1>لوحة إدارة هلا شات</h1>' +
            '<h2>البلاغات (' + reports.length + ')</h2>' +
            '<table><tr><th>الوقت</th><th>المُبلَّغ عنه</th><th>المُبلِّغ</th><th>الغرفة</th><th>السبب</th><th>إجراء</th></tr>' + (rows || '<tr><td colspan="6">ولا بلاغ لحد الحين</td></tr>') + '</table>' +
            '<h2>المحظورون حالياً (' + banned.length + ')</h2><ul>' + (bannedRows || '<li>ولا حد محظور</li>') + '</ul>');
        });
        return;
      }

      if (req.method === 'POST' && (u.pathname === '/admin/ban' || u.pathname === '/admin/unban')) {
        var chunksB = [];
        req.on('data', function (c) { chunksB.push(c); });
        req.on('end', function () {
          var raw = Buffer.concat(chunksB).toString('utf8');
          var params = new URLSearchParams(raw);
          var username = (params.get('username') || '').trim().toLowerCase();
          if (!username) { res.writeHead(400); res.end('اسم مستخدم ناقص'); return; }
          var action = u.pathname === '/admin/ban' ? banUser(username) : unbanUser(username);
          action.then(function () {
            broadcastBanned();
            res.writeHead(302, { location: '/admin?key=' + key }); res.end();
          });
        });
        return;
      }
      res.writeHead(404); res.end('غير موجود'); return;
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
              console.error('[push] فشل الإرسال:', e && e.message);
              res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: false, reason: 'فشل الإرسال' }));
            });
        });
      });
      return;
    }

    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, service: 'hala-broker', clients: aedes.connectedClients, persistent: !!persistence, aqua: !!(process.env.GEMINI_API_KEY || process.env.GROQ_API_KEY), groqBackup: !!process.env.GROQ_API_KEY, push: PUSH_READY }));
  });

  const wss = new WebSocketServer({ server: httpServer });
  wss.on('connection', (websocket, req) => {
    const stream = createWebSocketStream(websocket);
    websocket.on('error', () => {});
    stream.on('error', () => {});
    aedes.handle(stream, req);
  });

  httpServer.listen(PORT, () => {
    console.log('🫖 بروكر هلا شات شغّال على المنفذ', PORT);
  });

  aedes.on('client', (c) => console.log('دخل:', c.id));
  aedes.on('clientDisconnect', (c) => console.log('طلع:', c.id));
  aedes.on('publish', (packet, c) => {
    if (c && !packet.topic.startsWith('$SYS')) console.log('نشر:', c.id, '←', packet.topic);
  });
}

main();
