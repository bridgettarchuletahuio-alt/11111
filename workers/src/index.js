const jsonHeaders = {
    'content-type': 'application/json; charset=utf-8'
};

const inMemoryLinksCache = new Map();
const DEFAULT_LINKS_CACHE_TTL_MS = 5 * 60 * 1000;
const SUPER_ADMIN_HASH = '467bc294e964ad35a38fa11fd10e0c5c743fc5dfba6b681c3e443167c5379ce0';
const PASSWORD_ACCOUNTS = [
    { code: 'a1', label: '账号 1', hash: '428cacf00833cc176bf2abad9615a6dc14caf8af7e2801668fd980659eca5430' },
    { code: 'b2', label: '账号 2', hash: 'fa6e24c632239de814613bcd81f0d4d90adb4d040798162d29231346d15bbe17' },
    { code: 'c3', label: '账号 3', hash: '2f79509269d8d30f7fae8bc32e4ccd2bf5ef8d6346e1b1a14fe68aaf29510d6f' },
    { code: 'd4', label: '账号 4', hash: '71b4da4702ba44019fc45ed939550d4bd3c328a031250d45bbf4000f4a281297' },
    { code: 'e5', label: '账号 5', hash: '4b290a64059ad5556565d6bdb6c653b10cf18db6d85f6c62b54995e9767ab758' },
    { code: 'f6', label: '账号 6', hash: 'fbf2d683e08e94699d384d6ddd8a3880f4c008d552da5f2907dc2ef6c870ccf3' },
    { code: 'g7', label: '账号 7', hash: '622b90c4d36956503eaf361cebca594eb6ce3b7fce3e0e2b33fe81f25fb7605e' },
    { code: 'h8', label: '账号 8', hash: '5d707bf6891f28422af407617cc133513a5246ef7df5e62cfafd3a6efc5010ca' },
    { code: 'j9', label: '账号 9', hash: '15d8e639ff5bf0c09632619817d4102cb125b7f96d1fd5ea578cde820a3ad731' },
    { code: 'k0', label: '账号 10', hash: '1bad790cf4a1def428529aebfd96cafbf758f90d6847ce0753d375fe7156e4bc' }
];

export default {
    async fetch(request, env, executionCtx) {
        try {
            return await routeRequest(request, env, executionCtx);
        } catch (error) {
            return withCors(
                new Response(JSON.stringify({ error: error.message || 'Internal error' }), {
                    status: error.status || 500,
                    headers: jsonHeaders
                }),
                request,
                env
            );
        }
    }
};

export class LinkRouterDO {
    constructor(state, env) {
        this.state = state;
        this.env = env;
    }

    async fetch(request) {
        const url = new URL(request.url);
        const pathname = normalizePath(url.pathname);

        if (pathname === '/init' && request.method === 'POST') {
            const payload = await readJson(request);
            const id = sanitizeId(payload.id);
            const links = sanitizeLinks(payload.links);
            const currentIndex = normalizeIndex(payload.currentIndex || 0, links.length);

            await this.state.storage.put('set', {
                id,
                links,
                currentIndex
            });

            return new Response(JSON.stringify({ ok: true, id }), {
                status: 200,
                headers: jsonHeaders
            });
        }

        if (pathname === '/next' && request.method === 'GET') {
            const id = sanitizeId(url.searchParams.get('id') || '');
            const data = await this.ensureSetLoaded(id);
            const links = Array.isArray(data.links) ? data.links : [];
            if (links.length === 0) {
                throw httpError(409, 'Link set is empty');
            }

            const currentIndex = normalizeIndex(data.currentIndex || 0, links.length);
            const targetUrl = String(links[currentIndex] || '');
            if (!/^https?:\/\//i.test(targetUrl)) {
                throw httpError(409, 'Stored URL is invalid');
            }

            const nextIndex = (currentIndex + 1) % links.length;
            await this.state.storage.put('set', {
                id,
                links,
                currentIndex: nextIndex
            });

            return new Response(JSON.stringify({
                ok: true,
                id,
                index: currentIndex,
                nextIndex,
                url: targetUrl,
                nextUrl: targetUrl
            }), {
                status: 200,
                headers: jsonHeaders
            });
        }

        return new Response(JSON.stringify({ error: 'Not found' }), {
            status: 404,
            headers: jsonHeaders
        });
    }

    async ensureSetLoaded(id) {
        const cached = await this.state.storage.get('set');
        if (cached && cached.id === id && Array.isArray(cached.links) && cached.links.length > 0) {
            return cached;
        }

        const row = await this.env.DB.prepare('SELECT links_json, current_index FROM link_sets WHERE id = ?').bind(id).first();
        if (!row) {
            throw httpError(404, 'Link set not found');
        }

        const links = safeParseArray(row.links_json);
        if (links.length === 0) {
            throw httpError(409, 'Link set is empty');
        }

        const loaded = {
            id,
            links,
            currentIndex: normalizeIndex(row.current_index || 0, links.length)
        };
        await this.state.storage.put('set', loaded);
        return loaded;
    }
}

async function routeRequest(request, env, executionCtx) {
    const url = new URL(request.url);
    const pathname = normalizePath(url.pathname);
    const adminUrl = String(env.ADMIN_PAGE_URL || '').trim();

    if (request.method === 'OPTIONS') {
        return handleOptions(request, env);
    }

    if (pathname === '/') {
        if (adminUrl) {
            return Response.redirect(adminUrl, 302);
        }
        return json({ ok: true, service: 'link-dispatch-worker' }, 200, request, env);
    }

    // Compatibility redirects for old/bookmarked admin paths.
    if (pathname === '/11111' || pathname === '/11111/' || pathname === '/index.html') {
        if (adminUrl) {
            return Response.redirect(adminUrl, 302);
        }
    }

    if (pathname === '/health') {
        return json({ ok: true, service: 'link-dispatch-worker' }, 200, request, env);
    }

    if (pathname.startsWith('/r/')) {
        const id = pathname.slice(3);
        return handleRedirect(id, request, env, executionCtx);
    }

    if (pathname === '/api' && request.method === 'POST') {
        return handleActionApi(request, env, executionCtx);
    }

    if (pathname === '/api/sets' && request.method === 'GET') {
        const access = await ensureAuthorizedAccess(request, env, url);
        const limit = clampLimit(Number(url.searchParams.get('limit') || '20'));
        const items = await listSets(env, limit, access);
        return json({ items }, 200, request, env);
    }

    if (pathname.startsWith('/api/stats/') && request.method === 'GET') {
        const access = await ensureAuthorizedAccess(request, env, url);
        const id = pathname.slice('/api/stats/'.length).trim();
        if (!id) {
            throw httpError(400, 'Missing set id');
        }
        const stats = await getStats(env, id, access);
        return json({ id, stats }, 200, request, env);
    }

    return json({ error: 'Not found' }, 404, request, env);
}

function normalizePath(pathname) {
    if (!pathname || pathname === '/') return '/';
    return pathname.length > 1 ? pathname.replace(/\/+$/, '') : pathname;
}

function clampLimit(value) {
    if (!Number.isFinite(value) || value <= 0) return 20;
    return Math.min(Math.floor(value), 100);
}

async function handleActionApi(request, env, executionCtx) {
        if (action === 'updateSet') {
            // 参数：id, links
            const id = sanitizeId(payload.id);
            const links = Array.isArray(payload.links) ? payload.links : [];
            if (!id || links.length === 0) {
                throw httpError(400, '缺少id或links');
            }
            // 权限校验
            const access = await ensureAuthorizedAccess(request, env, new URL(request.url), payload);
            ensureSetAccess(id, access);
            // 更新数据库
            const now = new Date().toISOString();
            await env.DB.prepare('UPDATE link_sets SET links_json = ?, updated_at = ? WHERE id = ?')
                .bind(JSON.stringify(links), now, id).run();
            setLinksMemoryCache(env, id, links);
            await setLinksKvCache(env, id, links);
            return json({ ok: true }, 200, request, env);
        }
    const payload = await readJson(request);
    const action = String(payload.action || '').trim();

    if (action === 'login') {
        const access = await resolvePasswordAccess(payload.password);
        if (!access) {
            throw httpError(401, '密码错误');
        }

        const profile = access.mode === 'admin'
            ? { code: 'admin', label: '超级管理员', role: 'admin' }
            : { code: access.owner.code, label: access.owner.label, role: 'owner' };

        return json({
            ok: true,
            owner: profile
        }, 200, request, env);
    }

    if (action === 'nextUrl') {
        const result = await nextUrl(env, payload.id, {
            ua: payload.ua,
            ref: payload.ref,
            ip: request.headers.get('CF-Connecting-IP') || ''
        }, {
            asyncLog: true,
            executionCtx,
            hashIp: false
        });
        return json(result, 200, request, env);
    }

    const access = await ensureAuthorizedAccess(request, env, new URL(request.url), payload);

    if (action === 'createSet') {
        const id = await createSet(env, payload, access);
        return json({
            ok: true,
            id,
            shortUrl: buildShortUrl(env, id)
        }, 200, request, env);
    }

    if (action === 'listSets') {
        const items = await listSets(env, clampLimit(Number(payload.limit || 20)), access);
        return json({ items }, 200, request, env);
    }

    if (action === 'getStats') {
        const stats = await getStats(env, payload.id, access);
        return json({ id: payload.id, stats }, 200, request, env);
    }

    throw httpError(400, 'Unsupported action');
}

async function createSet(env, payload, access) {
    const links = sanitizeLinks(payload.links);
    const name = typeof payload.name === 'string' ? payload.name.trim().slice(0, 120) : '';
    const now = new Date().toISOString();
    const id = access.mode === 'owner'
        ? await createScopedSetId(env, access.owner.code)
        : sanitizeId(payload.id || crypto.randomUUID().slice(0, 8));

    if (access.mode !== 'owner') {
        const exists = await env.DB.prepare('SELECT id FROM link_sets WHERE id = ?').bind(id).first();
        if (exists) {
            throw httpError(409, 'ID already exists, please retry');
        }
    }

    await env.DB.prepare(
        'INSERT INTO link_sets (id, name, links_json, current_index, click_count, created_at, updated_at) VALUES (?, ?, ?, 0, 0, ?, ?)'
    ).bind(id, name, JSON.stringify(links), now, now).run();

    setLinksMemoryCache(env, id, links);
    await setLinksKvCache(env, id, links);
    await initDurableSet(env, id, links, 0);

    return id;
}

async function nextUrl(env, rawId, meta, options = {}) {
    const id = sanitizeId(rawId);
    const asyncLog = options.asyncLog === true;
    const executionCtx = options.executionCtx;
    const hashIp = options.hashIp !== false;

    const fast = await nextFromDurable(env, id);
    const currentIndex = Number(fast.index || 0);
    const nextIndexValue = Number(fast.nextIndex || 0);
    const url = String(fast.url || fast.nextUrl || '');
    const now = new Date().toISOString();
    const logId = crypto.randomUUID();
    const ua = typeof meta.ua === 'string' ? meta.ua.slice(0, 500) : '';
    const ref = typeof meta.ref === 'string' ? meta.ref.slice(0, 1000) : '';
    const updateIndexStmt = env.DB.prepare(
        'UPDATE link_sets SET current_index = ?, updated_at = ? WHERE id = ?'
    ).bind(nextIndexValue, now, id);
    const bumpClickCountStmt = env.DB.prepare(
        'UPDATE link_sets SET click_count = COALESCE(click_count, 0) + 1 WHERE id = ?'
    ).bind(id);

    if (asyncLog && executionCtx && typeof executionCtx.waitUntil === 'function') {
        await updateIndexStmt.run();

        executionCtx.waitUntil((async () => {
            try {
                const ipHash = hashIp ? await sha256(meta.ip || '') : '';
                await env.DB.batch([
                    bumpClickCountStmt,
                    env.DB.prepare(
                    'INSERT INTO click_logs (log_id, set_id, link_index, url, clicked_at, ua, ref, ip_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
                    ).bind(logId, id, currentIndex, url, now, ua, ref, ipHash)
                ]);
            } catch {
                // Log write failures should never block redirect responses.
            }
        })());
    } else {
        const ipHash = hashIp ? await sha256(meta.ip || '') : '';
        await env.DB.batch([
            updateIndexStmt,
            bumpClickCountStmt,
            env.DB.prepare(
                'INSERT INTO click_logs (log_id, set_id, link_index, url, clicked_at, ua, ref, ip_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
            ).bind(logId, id, currentIndex, url, now, ua, ref, ipHash)
        ]);
    }

    return {
        ok: true,
        id,
        index: currentIndex,
        url,
        nextUrl: url,
        clicks: null
    };
}

async function handleRedirect(rawId, request, env, executionCtx) {
    const result = await nextUrl(env, rawId, {
        ua: request.headers.get('user-agent') || '',
        ref: request.headers.get('referer') || '',
        ip: request.headers.get('CF-Connecting-IP') || ''
    }, {
        asyncLog: true,
        executionCtx,
        hashIp: false
    });

    const redirectResponse = Response.redirect(result.url, 302);
    const headers = new Headers(redirectResponse.headers);
    headers.set('cache-control', 'no-store');

    return new Response(null, {
        status: redirectResponse.status,
        statusText: redirectResponse.statusText,
        headers
    });
}

async function nextFromDurable(env, id) {
    if (!env.LINK_ROUTER_DO) {
        throw httpError(500, 'LINK_ROUTER_DO binding is missing');
    }

    const doId = env.LINK_ROUTER_DO.idFromName(id);
    const stub = env.LINK_ROUTER_DO.get(doId);
    const resp = await stub.fetch(`https://do/next?id=${encodeURIComponent(id)}`);
    const payload = await resp.json().catch(() => ({}));

    if (!resp.ok) {
        throw httpError(resp.status || 500, payload.error || 'Failed to resolve next url');
    }

    return payload;
}

async function initDurableSet(env, id, links, currentIndex) {
    if (!env.LINK_ROUTER_DO) {
        return;
    }

    try {
        const doId = env.LINK_ROUTER_DO.idFromName(id);
        const stub = env.LINK_ROUTER_DO.get(doId);
        await stub.fetch('https://do/init', {
            method: 'POST',
            headers: {
                'content-type': 'application/json; charset=utf-8'
            },
            body: JSON.stringify({
                id,
                links,
                currentIndex
            })
        });
    } catch {
        // DO warmup failures should not block createSet.
    }
}

async function listSets(env, limit, access) {
    const { results } = access.mode === 'owner'
        ? await env.DB.prepare(
            'SELECT id, name, links_json, current_index, click_count, created_at, updated_at FROM link_sets WHERE id LIKE ? ORDER BY datetime(created_at) DESC LIMIT ?'
        ).bind(`${access.owner.code}%`, limit).all()
        : await env.DB.prepare(
            'SELECT id, name, links_json, current_index, click_count, created_at, updated_at FROM link_sets ORDER BY datetime(created_at) DESC LIMIT ?'
        ).bind(limit).all();

    return (results || []).map((row) => {
        const links = safeParseArray(row.links_json);
        const ownerCode = extractOwnerCodeFromId(row.id);
        const owner = findOwnerByCode(ownerCode);
        return {
            id: row.id,
            name: row.name || '',
            ownerCode,
            ownerLabel: owner ? owner.label : '历史数据',
            createdAt: row.created_at,
            updatedAt: row.updated_at,
            currentIndex: Number(row.current_index || 0),
            clickCount: Number(row.click_count || 0),
            count: links.length,
            links
        };
    });
}

async function getStats(env, rawId, access) {
    const id = sanitizeId(rawId);
    ensureSetAccess(id, access);
    const setRow = await env.DB.prepare(
        'SELECT links_json FROM link_sets WHERE id = ?'
    ).bind(id).first();

    if (!setRow) {
        throw httpError(404, 'Link set not found');
    }

    const links = safeParseArray(setRow.links_json);
    setLinksMemoryCache(env, id, links);
    await setLinksKvCache(env, id, links);
    const { results } = await env.DB.prepare(
        'SELECT link_index, COUNT(*) AS clicks, MAX(clicked_at) AS last_clicked_at FROM click_logs WHERE set_id = ? GROUP BY link_index ORDER BY link_index ASC'
    ).bind(id).all();

    const counts = new Map((results || []).map((row) => [Number(row.link_index || 0), {
        clicks: Number(row.clicks || 0),
        lastClickedAt: row.last_clicked_at || null
    }]));

    return links.map((url, index) => {
        const stat = counts.get(index) || { clicks: 0, lastClickedAt: null };
        return {
            index,
            url,
            clicks: stat.clicks,
            lastClickedAt: stat.lastClickedAt
        };
    });
}

function extractAdminToken(request, url, payload) {
    const headerToken = request.headers.get('x-admin-token') || request.headers.get('authorization')?.replace(/^Bearer\s+/i, '') || '';
    const queryToken = url.searchParams.get('token') || '';
    const bodyToken = payload && typeof payload.token === 'string' ? payload.token : '';
    return headerToken || queryToken || bodyToken;
}

function ensureAdmin(request, env, url, payload) {
    const token = env.ADMIN_TOKEN;
    if (!token) {
        return;
    }

    const providedToken = extractAdminToken(request, url, payload);

    if (providedToken === token) {
        return;
    }

    throw httpError(401, 'Unauthorized');
}

async function ensureAuthorizedAccess(request, env, url, payload) {
    const token = String(env.ADMIN_TOKEN || '').trim();
    const providedToken = extractAdminToken(request, url, payload);
    if (token && providedToken === token) {
        return { mode: 'admin' };
    }

    const password = extractUserPassword(request, url, payload);
    const access = await resolvePasswordAccess(password);
    if (!access) {
        throw httpError(401, 'Unauthorized');
    }

    return access;
}

function extractUserPassword(request, url, payload) {
    const headerPassword = request.headers.get('x-user-password') || '';
    const queryPassword = url.searchParams.get('password') || '';
    const bodyPassword = payload && typeof payload.password === 'string' ? payload.password : '';
    return headerPassword || queryPassword || bodyPassword;
}

async function resolvePasswordAccess(password) {
    const normalized = String(password || '').trim();
    if (!normalized) {
        return null;
    }

    const passwordHash = await sha256(normalized);
    if (passwordHash === SUPER_ADMIN_HASH) {
        return { mode: 'admin' };
    }

    const owner = PASSWORD_ACCOUNTS.find((item) => item.hash === passwordHash) || null;
    if (!owner) {
        return null;
    }

    return {
        mode: 'owner',
        owner
    };
}

async function createScopedSetId(env, ownerCode) {
    for (let attempt = 0; attempt < 8; attempt += 1) {
        const id = `${ownerCode}${crypto.randomUUID().replace(/-/g, '').slice(0, 6)}`;
        const exists = await env.DB.prepare('SELECT id FROM link_sets WHERE id = ?').bind(id).first();
        if (!exists) {
            return id;
        }
    }

    throw httpError(409, 'Failed to allocate a unique id');
}

function ensureSetAccess(id, access) {
    if (access.mode !== 'owner') {
        return;
    }

    if (!id.startsWith(access.owner.code)) {
        throw httpError(404, 'Link set not found');
    }
}

function extractOwnerCodeFromId(id) {
    const normalized = String(id || '').trim();
    return normalized.slice(0, 2);
}

function findOwnerByCode(code) {
    return PASSWORD_ACCOUNTS.find((item) => item.code === code) || null;
}

async function readJson(request) {
    const text = await request.text();
    if (!text) return {};

    try {
        return JSON.parse(text);
    } catch {
        throw httpError(400, 'Invalid JSON payload');
    }
}

function sanitizeId(value) {
    const id = String(value || '').trim();
    if (!/^[a-zA-Z0-9_-]{4,64}$/.test(id)) {
        throw httpError(400, 'Invalid id');
    }
    return id;
}

function sanitizeLinks(links) {
    if (!Array.isArray(links)) {
        throw httpError(400, 'links must be an array');
    }

    const cleanLinks = links
        .map((item) => String(item || '').trim())
        .filter(Boolean)
        .filter((item) => /^https?:\/\//i.test(item));

    if (cleanLinks.length === 0) {
        throw httpError(400, 'At least one valid http or https link is required');
    }

    if (cleanLinks.length > 500) {
        throw httpError(400, 'Too many links in one set');
    }

    return cleanLinks;
}

async function getLinksForSet(env, id) {
    const memoryCached = getLinksMemoryCache(env, id);
    if (memoryCached) {
        return memoryCached;
    }

    const kvCached = await getLinksKvCache(env, id);
    if (kvCached) {
        setLinksMemoryCache(env, id, kvCached);
        return kvCached;
    }

    const row = await env.DB.prepare('SELECT links_json FROM link_sets WHERE id = ?').bind(id).first();
    if (!row) {
        return null;
    }

    const links = safeParseArray(row.links_json);
    if (links.length > 0) {
        setLinksMemoryCache(env, id, links);
        await setLinksKvCache(env, id, links);
    }

    return links;
}

function getLinksMemoryCache(env, id) {
    const cached = inMemoryLinksCache.get(id);
    if (!cached) {
        return null;
    }

    const ttlMs = getLinksCacheTtlMs(env);
    if (Date.now() - cached.updatedAt > ttlMs) {
        inMemoryLinksCache.delete(id);
        return null;
    }

    return cached.links;
}

function setLinksMemoryCache(env, id, links) {
    if (!Array.isArray(links) || links.length === 0) {
        return;
    }

    inMemoryLinksCache.set(id, {
        links,
        updatedAt: Date.now()
    });
}

async function getLinksKvCache(env, id) {
    if (!env.LINKS_KV || typeof env.LINKS_KV.get !== 'function') {
        return null;
    }

    try {
        const raw = await env.LINKS_KV.get(`links:${id}`);
        if (!raw) {
            return null;
        }
        const parsed = safeParseArray(raw);
        return parsed.length > 0 ? parsed : null;
    } catch {
        return null;
    }
}

async function setLinksKvCache(env, id, links) {
    if (!env.LINKS_KV || typeof env.LINKS_KV.put !== 'function') {
        return;
    }

    try {
        const ttlSeconds = Math.max(60, Math.floor(getLinksCacheTtlMs(env) / 1000));
        await env.LINKS_KV.put(`links:${id}`, JSON.stringify(links), {
            expirationTtl: ttlSeconds
        });
    } catch {
        // KV is optional acceleration; failures should not affect core flow.
    }
}

function getLinksCacheTtlMs(env) {
    const configuredSeconds = Number(env.LINKS_CACHE_TTL_SECONDS || 300);
    if (!Number.isFinite(configuredSeconds) || configuredSeconds <= 0) {
        return DEFAULT_LINKS_CACHE_TTL_MS;
    }
    return configuredSeconds * 1000;
}

function normalizeIndex(value, length) {
    const parsed = Number(value || 0);
    if (!Number.isFinite(parsed) || parsed < 0) return 0;
    return Math.floor(parsed) % length;
}

function safeParseArray(value) {
    try {
        const parsed = JSON.parse(value || '[]');
        return Array.isArray(parsed) ? parsed : [];
    } catch {
        return [];
    }
}

function buildShortUrl(env, id) {
    const base = String(env.PUBLIC_BASE_URL || '').trim().replace(/\/+$/, '');
    return base ? `${base}/r/${id}` : `/r/${id}`;
}

function json(data, status, request, env) {
    return withCors(
        new Response(JSON.stringify(data), {
            status,
            headers: jsonHeaders
        }),
        request,
        env
    );
}

function handleOptions(request, env) {
    return withCors(new Response(null, { status: 204 }), request, env);
}

function withCors(response, request, env) {
    const origin = request.headers.get('origin');
    const allowedOrigin = env.ALLOWED_ORIGIN || '*';
    const headers = new Headers(response.headers);
    headers.set('access-control-allow-origin', allowedOrigin === '*' ? '*' : origin || allowedOrigin);
    headers.set('access-control-allow-methods', 'GET,POST,OPTIONS');
    headers.set('access-control-allow-headers', 'Content-Type,Authorization,X-Admin-Token,X-User-Password');
    headers.set('vary', 'Origin');

    return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers
    });
}

function httpError(status, message) {
    const error = new Error(message);
    error.status = status;
    return error;
}

async function sha256(text) {
    if (!text) return '';
    const bytes = new TextEncoder().encode(text);
    const digest = await crypto.subtle.digest('SHA-256', bytes);
    return [...new Uint8Array(digest)].map((item) => item.toString(16).padStart(2, '0')).join('');
}