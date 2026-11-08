export default {
  async fetch(req, env) {
    if (env.PROXY_KEY && req.headers.get('x-proxy-key') !== env.PROXY_KEY) {
      return new Response('forbidden', { status: 403 });
    }
    const u = new URL(req.url);
    const target = new URL(u.pathname + u.search, 'https://api.audius.co');
    const headers = new Headers(req.headers);
    for (const h of ['host', 'cf-connecting-ip', 'x-forwarded-for', 'x-real-ip', 'x-proxy-key']) headers.delete(h);
    const hasBody = !['GET', 'HEAD'].includes(req.method);
    const isStream = /^\/v1\/tracks\/[^/]+\/stream\/?$/.test(u.pathname) && !u.searchParams.has('no_redirect');
    return fetch(target, {
      method: req.method,
      headers,
      body: hasBody ? req.body : undefined,
      redirect: isStream ? 'follow' : 'manual',
    });
  },
};
