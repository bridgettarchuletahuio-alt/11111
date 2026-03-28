const jsonHeaders = {
    'content-type': 'application/json; charset=utf-8'
};

export default {
    async fetch(request, env) {
        try {
            return await routeRequest(request, env);
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

async function routeRequest(request, env) {
    const url = new URL(request.url);
    const pathname = normalizePath(url.pathname);

    if (request.method === 'OPTIONS') {
        return handleOptions(request, env);
    }

    if (pathname === '/') {
        const adminUrl = String(env.ADMIN_PAGE_URL || '').trim();
        if (adminUrl) {
            return Response.redirect(adminUrl, 302);
        }
        return json({ ok: true, service: 'link-dispatch-worker' }, 200, request, env);
    }

    if (pathname === '/health') {
        return json({ ok: true, service: 'link-dispatch-worker' }, 200, request, env);
    }

    if (pathname.startsWith('/r/')) {
        const id = pathname.slice(3);
        return handleRedirect(id, request, env);
    }

    if (pathname === '/api' && request.method === 'POST') {
        return handleActionApi(request, env);
    }

    if (pathname === '/api/sets' && request.method === 'GET') {
        ensureAdmin(request, env, url);
        const limit = clampLimit(Number(url.searchParams.get('limit') || '20'));
        const items = await listSets(env, limit);
        return json({ items }, 200, request, env);
    }

    if (pathname.startsWith('/api/stats/') && request.method === 'GET') {
        ensureAdmin(request, env, url);
        const id = pathname.slice('/api/stats/'.length).trim();
        if (!id) {
            throw httpError(400, 'Missing set id');
        }
        const stats = await getStats(env, id);
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

async function handleActionApi(request, env) {
    const payload = await readJson(request);
    const action = String(payload.action || '').trim();

    if (action === 'nextUrl') {
        const result = await nextUrl(env, payload.id, {
            ua: payload.ua,
            ref: payload.ref,
            ip: request.headers.get('CF-Connecting-IP') || ''
        });
        return json(result, 200, request, env);
    }

    ensureAdmin(request, env, new URL(request.url), payload);

    if (action === 'createSet') {
        const id = await createSet(env, payload);
        return json({
            ok: true,
            id,
            shortUrl: buildShortUrl(env, id)
        }, 200, request, env);
    }

    if (action === 'listSets') {
        const items = await listSets(env, clampLimit(Number(payload.limit || 20)));
        return json({ items }, 200, request, env);
    }

    if (action === 'getStats') {
        const stats = await getStats(env, payload.id);
        return json({ id: payload.id, stats }, 200, request, env);
    }

    throw httpError(400, 'Unsupported action');
}

async function createSet(env, payload) {
    const id = sanitizeId(payload.id || crypto.randomUUID().slice(0, 8));
    const links = sanitizeLinks(payload.links);
    const name = typeof payload.name === 'string' ? payload.name.trim().slice(0, 120) : '';
    const now = new Date().toISOString();

    const exists = await env.DB.prepare('SELECT id FROM link_sets WHERE id = ?').bind(id).first();
    if (exists) {
        throw httpError(409, 'ID already exists, please retry');
    }

    await env.DB.prepare(
        'INSERT INTO link_sets (id, name, links_json, current_index, click_count, created_at, updated_at) VALUES (?, ?, ?, 0, 0, ?, ?)'
    ).bind(id, name, JSON.stringify(links), now, now).run();

    return id;
}

async function nextUrl(env, rawId, meta) {
    const id = sanitizeId(rawId);
    const setRow = await env.DB.prepare(
        'SELECT id, links_json, current_index, click_count FROM link_sets WHERE id = ?'
    ).bind(id).first();

    if (!setRow) {
        throw httpError(404, 'Link set not found');
    }

    const links = JSON.parse(setRow.links_json || '[]');
    if (!Array.isArray(links) || links.length === 0) {
        throw httpError(409, 'Link set is empty');
    }

    const currentIndex = normalizeIndex(setRow.current_index, links.length);
    const url = String(links[currentIndex] || '');
    if (!/^https?:\/\//i.test(url)) {
        throw httpError(409, 'Stored URL is invalid');
    }

    const nextIndexValue = (currentIndex + 1) % links.length;
    const now = new Date().toISOString();
    const logId = crypto.randomUUID();
    const ua = typeof meta.ua === 'string' ? meta.ua.slice(0, 500) : '';
    const ref = typeof meta.ref === 'string' ? meta.ref.slice(0, 1000) : '';
    const ipHash = await sha256(meta.ip || '');

    await env.DB.batch([
        env.DB.prepare(
            'UPDATE link_sets SET current_index = ?, click_count = COALESCE(click_count, 0) + 1, updated_at = ? WHERE id = ?'
        ).bind(nextIndexValue, now, id),
        env.DB.prepare(
            'INSERT INTO click_logs (log_id, set_id, link_index, url, clicked_at, ua, ref, ip_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        ).bind(logId, id, currentIndex, url, now, ua, ref, ipHash)
    ]);

    return {
        ok: true,
        id,
        index: currentIndex,
        url,
        nextUrl: url,
        clicks: Number(setRow.click_count || 0) + 1
    };
}

async function handleRedirect(rawId, request, env) {
    const result = await nextUrl(env, rawId, {
        ua: request.headers.get('user-agent') || '',
        ref: request.headers.get('referer') || '',
        ip: request.headers.get('CF-Connecting-IP') || ''
    });

    const response = Response.redirect(result.url, 302);
    response.headers.set('cache-control', 'no-store');
    return withCors(response, request, env);
}

async function listSets(env, limit) {
    const { results } = await env.DB.prepare(
        'SELECT id, name, links_json, current_index, click_count, created_at, updated_at FROM link_sets ORDER BY datetime(created_at) DESC LIMIT ?'
    ).bind(limit).all();

    return (results || []).map((row) => {
        const links = safeParseArray(row.links_json);
        return {
            id: row.id,
            name: row.name || '',
            createdAt: row.created_at,
            updatedAt: row.updated_at,
            currentIndex: Number(row.current_index || 0),
            clickCount: Number(row.click_count || 0),
            count: links.length,
            links
        };
    });
}

async function getStats(env, rawId) {
    const id = sanitizeId(rawId);
    const setRow = await env.DB.prepare(
        'SELECT links_json FROM link_sets WHERE id = ?'
    ).bind(id).first();

    if (!setRow) {
        throw httpError(404, 'Link set not found');
    }

    const links = safeParseArray(setRow.links_json);
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

function ensureAdmin(request, env, url, payload) {
    const token = env.ADMIN_TOKEN;
    if (!token) {
        return;
    }

    const headerToken = request.headers.get('x-admin-token') || request.headers.get('authorization')?.replace(/^Bearer\s+/i, '') || '';
    const queryToken = url.searchParams.get('token') || '';
    const bodyToken = payload && typeof payload.token === 'string' ? payload.token : '';

    if (headerToken === token || queryToken === token || bodyToken === token) {
        return;
    }

    throw httpError(401, 'Unauthorized');
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
    response.headers.set('access-control-allow-origin', allowedOrigin === '*' ? '*' : origin || allowedOrigin);
    response.headers.set('access-control-allow-methods', 'GET,POST,OPTIONS');
    response.headers.set('access-control-allow-headers', 'Content-Type,Authorization,X-Admin-Token');
    response.headers.set('vary', 'Origin');
    return response;
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