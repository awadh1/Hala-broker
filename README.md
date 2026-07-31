# بروكر هلا شات

سيرفر MQTT خاص لتطبيق "هلا شات" — بديل عن السيرفر العام المشترك.

## التشغيل محلياً
```
npm install
npm start
```

## النشر على Render
1. ادخل render.com وسجّل حساب مجاني (إيميل بس، بدون بطاقة)
2. New + ← Web Service ← اربطه بهذا المستودع
3. Build Command: `npm install`
4. Start Command: `npm start`
5. اضغط Create Web Service

بعد الرفع بتاخذ رابط شبيه بـ:
`https://hala-broker-xxxx.onrender.com`

استبدل `https://` بـ `wss://` بس — هذا هو عنوان البروكر اللي تحطه
بملف `hala-online.html` (سيرفرنا يقبل الاتصال على أي مسار، ما يحتاج
إضافة شي آخر آخر الرابط).
