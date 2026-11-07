export default {
  async fetch(req) {
    const u = new URL(req.url);
    const target = new URL(u.pathname + u.search, 'https://api.audius.co');
    const headers = new Headers(req.headers);
    for (const h of ['host', 'cf-connecting-ip', 'x-forwarded-for', 'x-real-ip']) headers.delete(h);
    const hasBody = !['GET', 'HEAD'].includes(req.method);
    return fetch(target, {
      method: req.method,
      headers,
      body: hasBody ? req.body : undefined,
      redirect: 'manual',
    });
  },
};