const jsonHeaders = {
    'content-type': 'application/json; charset=utf-8'
};

const inMemoryLinksCache = new Map();
const DEFAULT_LINKS_CACHE_TTL_MS = 5 * 60 * 1000;
const SUPER_ADMIN_HASH = '467bc294e964ad35a38fa11fd10e0c5c743fc5dfba6b681c3e443167c5379ce0';
const PASSWORD_ACCOUNTS = [
    { code: '00', label: '账号 1', hash: '1e835b46c9bd4748abec922ab7f64da9d017cac46a0cfcba287d88348b16dcde' },
    { code: '01', label: '账号 2', hash: '183e63aef8a9781380dcf09b66e395c30b53697d3c2dac9bd0340fa028833f92' },
    { code: '02', label: '账号 3', hash: 'cc66c0543d91ab121cd6eef50508a10d8a7640f693d43274973a6faa7fc4069f' },
    { code: '03', label: '账号 4', hash: '24385743990663a387954d634038c3fc27cd261b0a68272e83e84c858b0160c9' },
    { code: '04', label: '账号 5', hash: '8acbfc81761a21048d2e98c4bae1a070006622a8722cc6993b59651f6cfa62bf' },
    { code: '05', label: '账号 6', hash: '796c61a8ae481577ef885e641741c24ed9666ad99260b7875b81a6cf828f1466' },
    { code: '06', label: '账号 7', hash: '76e66ba2891b903dad8434c6ee3867fb0e66ff07b33e652ceb70cec5a28cfad8' },
    { code: '07', label: '账号 8', hash: 'a8b11600eafbaabe89e10792985a4c2852e00efc994d282ad8ceefc0769239ab' },
    { code: '08', label: '账号 9', hash: '769b51dfde117c32910b1a4a2a6839525104ed7d3bb8a851b8960155594bb341' },
    { code: '09', label: '账号 10', hash: '3e78ba9329efad188fc5e6f102e254b833a0ca2ef56afc81790be64302041966' },
    { code: '10', label: '账号 11', hash: '192fbeb041ea73efc8361dc67592199faf44cad006c6dabb37c2adf4760e9709' },
    { code: '11', label: '账号 12', hash: 'f389a31dc59700df573988ea75af805dc3f90d5f0d8f5b0177490574781ee239' },
    { code: '12', label: '账号 13', hash: '47c907f88ecfb4f5f71c07b505fbc66af00d3122992ab3122ce643dda426244e' },
    { code: '13', label: '账号 14', hash: '7df84e0deba1f7f4b9599e0809e32e6fcb785c30e4ca3b89d4d7de284895a044' },
    { code: '14', label: '账号 15', hash: '0c9b9fb09f8be6c32c4e58d79b7befb8f80a0849ca8203269e661c05025d1321' },
    { code: '15', label: '账号 16', hash: '71767c8750abc2db8827b81641d28d6265e848eeb481e8f36a612472cb64c8d1' },
    { code: '16', label: '账号 17', hash: '84f4e7718677fb1ee707f172fd3f03874c819bab2cc2320dc2b3750462dcde76' },
    { code: '17', label: '账号 18', hash: '4525c29bfed09a74cd8643db412792974632ceba4f24be4e4a8e823e4baa0f47' },
    { code: '18', label: '账号 19', hash: 'd623568cd265397414a0587a4558a5ddce0e17ec309c4a68597d1b92b14f8e10' },
    { code: '19', label: '账号 20', hash: '62c42eb00069b87ec78e43dc4c46f74b904115f740f91c2301bc99a6327c1e9d' },
    { code: '20', label: '账号 21', hash: '558baa45456e6140990513d6e17b34943e8d486ce6a0ecf0628eeb2eb554a242' },
    { code: '21', label: '账号 22', hash: '2d4a23a6b8a23f4fe7cfd638e853840ea6377ca929e5d756c34d3a0f09654602' },
    { code: '22', label: '账号 23', hash: 'f32e95603443abdd048bf07bd2b3a71a3f489265e6eea6bad627a690daf0082b' },
    { code: '23', label: '账号 24', hash: '09981b16bce1b9abab5d70a757ee6194d2642ca1a5f7978ff11c6d3ce45178a2' },
    { code: '24', label: '账号 25', hash: '956615cb17ba2b0897285f9d28cd6ebebc5b68406c9108cd2384005c40768276' },
    { code: '25', label: '账号 26', hash: '2c87ed3e5f8d18ea31de4fa75754c18c5bf9baab5376a298f705eb5920b1bd8d' },
    { code: '26', label: '账号 27', hash: '862e623c4fcabd5b97a5c3c8894c013be1842f37226f187c2c7568ecbca15177' },
    { code: '27', label: '账号 28', hash: 'f9210437f3cf74ae128a00462eb01f9bc5818558585c4da4056cbb409b3e47f4' },
    { code: '28', label: '账号 29', hash: 'a4133a5d9f76d9e97440143d28b758ebab87af9f42fbcb399cb98874f074a6e9' },
    { code: '29', label: '账号 30', hash: '252d2210e4b345fdbddc3896cd7e8c4c9da3b7fabe8a14e66f52cc78dcf46144' },
    { code: '30', label: '账号 31', hash: '63341e2544dd1dbe7ef4ecc5a3b988d44703601cdc42bca89152729fea53f5b7' },
    { code: '31', label: '账号 32', hash: 'fcbed170eb21a6c1d63150b2b714c40d945da07cfaddb8bb9db2e394418a808c' },
    { code: '32', label: '账号 33', hash: '8ce69bcf211abda5febc5d9e1caea2bb9a50f05144087824939f266e93fb8d4d' },
    { code: '33', label: '账号 34', hash: '7b390205ffc7ee9765be552b009934aca4ed0585dba9e1849bb30d5df1ede78a' },
    { code: '34', label: '账号 35', hash: '48cd5d210010a6f8c2a0a48a06c56c390a2c276a32eab27a3ae75e4c8c79fe45' },
    { code: '35', label: '账号 36', hash: 'd1d82f3a1c3b94340eb5b345d747b0b01d99736bb1a991420bfbb5fa31ca6b81' },
    { code: '36', label: '账号 37', hash: 'a2830aa012392833712b0f5f59ff2f48e9db3d7fa21fc96f0a3ee0a4aaa6ed72' },
    { code: '37', label: '账号 38', hash: 'afe6f48c927b5be85d3ccca3d10ceeb1e7bea9c1f7344827e53bd6da3b19a4ea' },
    { code: '38', label: '账号 39', hash: 'c56977de565f4c4364035ba7ccdf639cfdf53a46a64ef67cb7dbd666d0dac30b' },
    { code: '39', label: '账号 40', hash: '63dfc5819d7a9ccc685ff34470622dd06572a9402beac8431498e0368aa5b94b' },
    { code: '40', label: '账号 41', hash: '6c8289256ac6716a6c73fb87a3a7e2c20beeef96a92ea16ddbb6e3301a581408' },
    { code: '41', label: '账号 42', hash: '722a065f40b51433594e10320db3f4245a5820263cbbafe95ed3862b4a139a40' },
    { code: '42', label: '账号 43', hash: '92766f08e938543e6ea8464591e4be0939be098017adf7863fd80156a93d9adc' },
    { code: '43', label: '账号 44', hash: 'db56394520fb8937cf60822077a987482f5019202d46a516ee3074ee03b40c0a' },
    { code: '44', label: '账号 45', hash: 'ff6045ab7d8c8f4f01723fd990fae2b9f77a7fb01c741b40e49c75f251da6f5b' },
    { code: '45', label: '账号 46', hash: '9cb0090b9a60c330506ec1c6ce75c2e5eff33ef2fcb7ee26ef4aabd856df75a5' },
    { code: '46', label: '账号 47', hash: '4fcd5d9830a9cb027e2d04b8a219f34bcf36643c0ee5758e6d5e2854934e314c' },
    { code: '47', label: '账号 48', hash: 'e965cb1016d87dba96e160f90556c081f95cc19169ebc46ab8426ecc6d460370' },
    { code: '48', label: '账号 49', hash: '45f10166de108d73d54c6cdb91706780771faf3ccd9e7c97ecf7a7f7b1faeca2' },
    { code: '49', label: '账号 50', hash: 'e18797756be7c654918a89bed4a3420cb8b7dd0add1843f9e9692fb0ad8c93b0' },
    { code: '50', label: '账号 51', hash: '8921858e8739f5388c27b59e85fad6b36dc5b84b762ce796530164921d35f41c' },
    { code: '51', label: '账号 52', hash: '87ad28f7efdd772d1e977ed9149fda2737afd5076a7b494c81b77ac05570c0af' },
    { code: '52', label: '账号 53', hash: '79be83a01c1f9dae9602a2d9b5f0bb8ecabc0361c9695ba0a080c69fd36fdc36' },
    { code: '53', label: '账号 54', hash: '27f9b94697bff9f55f4f4550c632ec972aad70f9995defaa03d5a495767e72e9' },
    { code: '54', label: '账号 55', hash: 'b3e7cf66979be06ab86c35b707180766ff33aca0496fba6e5d69a8503aad22c1' },
    { code: '55', label: '账号 56', hash: '31fbe417cad83581fbaf69cef3e9423277875ace37234e46b910670b9466e534' },
    { code: '56', label: '账号 57', hash: '4e8a11b607e09e8dace221b711f725d9a2f78b6a39010a95013d17f6f7f5dc9a' },
    { code: '57', label: '账号 58', hash: '48d753c63792777ac4b0e790e6a4e73d156de6fe7b34ac9afd294e829949341c' },
    { code: '58', label: '账号 59', hash: '73fb49055082f537d78cf7675fde192e6506dfdd40b1c9eef23a1bcfaee0c1b0' },
    { code: '59', label: '账号 60', hash: '87748d92007988881f326f083e6e86b15a4f272c424e01bea80cbbb39923da97' },
    { code: '60', label: '账号 61', hash: '1d62201affbff6caae160101367c76861dd7bd316abb64d652d05952ebe00119' },
    { code: '61', label: '账号 62', hash: '19fd48c0dce813e622b301af1505780166a6970c4773667dfae6e5213916bd18' },
    { code: '62', label: '账号 63', hash: '4636eed41934dc3efe5ccbcb2d725f5a3efcd4772eaa12d9d11a56aada45c0b8' },
    { code: '63', label: '账号 64', hash: 'ece354460fb5f3d58640d6c2af895f7d875e35caa24695dfec4a957874e851e5' },
    { code: '64', label: '账号 65', hash: 'aeb8c76913bafb2551d7a5c38b81adf668ec0e4822f21bae635647ba5f81e2fc' },
    { code: '65', label: '账号 66', hash: '925cf60427e85808cf7f9ae86b50eaa3af7742a96c9b330f057d867fd1651757' },
    { code: '66', label: '账号 67', hash: 'b9cb9df1e1dd52cf3baeb2898fdeba2be5cd7599ab4586dcc814d668a9590611' },
    { code: '67', label: '账号 68', hash: '2e586106f8d4048ff808f8a9921c9bd7cc2d430142175a6848679902ec914c3f' },
    { code: '68', label: '账号 69', hash: 'a784dfe81bafad09ce8ebf721bc64e6c2ac2b00f986282c0f1957637428241e7' },
    { code: '69', label: '账号 70', hash: 'edada3dc2d602255921ff4a0da3b43ce34e845b95a0868243d1c5f3e573dd4ab' },
    { code: '70', label: '账号 71', hash: 'f3359c192d6314de3d03cb4ce15f463102094831ada1343b6abcfdeff6053e2a' },
    { code: '71', label: '账号 72', hash: '46c346b6782beab655d6a351a5806a6265f2e8c371556bb17e062edff7942f1a' },
    { code: '72', label: '账号 73', hash: '82ac525d1dd70d25e655a0e5a8d761489bea2ec73d92bd0027dc4f997e254753' },
    { code: '73', label: '账号 74', hash: '8d7cf923af886e14035058a06266932735f080f7bcfb1088bb8e5368d310b70c' },
    { code: '74', label: '账号 75', hash: '19a2414815778c91f46a3bb39b9a9b86fcc22d779e7a8ce9ebc0259a1e86d667' },
    { code: '75', label: '账号 76', hash: 'f5fe60c93173acf905943c18e24d0a9db49e50d0f9ba0776476f2dada2f8f9e6' },
    { code: '76', label: '账号 77', hash: '05d222792e8fe8670d0c3e2dd16ba98b15c007934529449a63230f43b65352c4' },
    { code: '77', label: '账号 78', hash: 'd493ccae516f54674922dde99556628e95b9f950dd54044d4fc119af3fee41cc' },
    { code: '78', label: '账号 79', hash: '2693dcbfe1b508c5f998ea12d757cc0d0f7e4c2e07f4e5a9df00ee32f80b88ac' },
    { code: '79', label: '账号 80', hash: 'f7e1f93c3b0a4a44b48cf4c3970679961eb9426cc1fb5cd826c771baccdd42b3' },
    { code: '80', label: '账号 81', hash: 'c05a02b956dbd2b0b5d95a5105c75b63fb44174fec2580274667238517de2504' },
    { code: '81', label: '账号 82', hash: '320e0a75df960d63c6d00a55cd7ec240c8faaa0becdcc59efbf0392c53c63fae' },
    { code: '82', label: '账号 83', hash: 'f0f9f415f5484af4ff6fa4450d5337de9f3150537e41a79e08afeceee3dfa36b' },
    { code: '83', label: '账号 84', hash: '297c592ea30c8491ea4b53aa732eb652f54f6a9180cee8a3826c0539c86b4379' },
    { code: '84', label: '账号 85', hash: 'ffc92097debffca4f00133cbf2b3c61c666e77e0297aae1eb91d8d5b6a76d869' },
    { code: '85', label: '账号 86', hash: '678ccb48bee5c23b660345235b01dda745964437fe727cd728d7f0994dc11e60' },
    { code: '86', label: '账号 87', hash: 'd075eabb9c89add3da2055828a21448009dc70162362149ec938a29f5d338148' },
    { code: '87', label: '账号 88', hash: '3152d2cb2e6513d600d090de474dbdf45962550738544e6154dc8e3148ab0e45' },
    { code: '88', label: '账号 89', hash: '3d9a95dd4548c02451a76d06f129f7d42228bb81e03eb06ad45fcfb369f32301' },
    { code: '89', label: '账号 90', hash: '6001ab25b53b6b0637801a396a903d17f22cb3e8cf08e2595fe0e47039ff8a39' },
    { code: '90', label: '账号 91', hash: '75342170b37a088edc924e091878b3b4c96b048a6a25ec596a25c7a8d1cdba21' },
    { code: '91', label: '账号 92', hash: '3d45f396ae0d8eb1e1642e1eac5cb7d394b1692eb95ca24a8d185bb0ace45f02' },
    { code: '92', label: '账号 93', hash: '117711024496e7ec9fb943b305b1cd3c95d472a827aeac8b93c5dc22aec40839' },
    { code: '93', label: '账号 94', hash: '921eedfcd7019138071234a7f39538befaf3aab918dc98587880ed58ea4c9e25' },
    { code: '94', label: '账号 95', hash: 'f1d9b4e4a58b940fae0d724e32ae59217649ee37b0dfeabbc4d920aa36e346bd' },
    { code: '95', label: '账号 96', hash: 'd4ac3978f6eef5bb53fc060cdef10283cb1be2062dac4818064c7fc6528617c0' },
    { code: '96', label: '账号 97', hash: 'd034ea2fab0a7fce44ff1614344d61c9eda20f0d277227eb7e9a986d7dc830e8' },
    { code: '97', label: '账号 98', hash: '2316bf290bdd5f38f8583b0802cf3e49557710f255a7379de1ce745bba610757' },
    { code: '98', label: '账号 99', hash: 'c03db0b0784e58b8b674450a2d34b0cbbf4351ffe29a4c1dfc4d46c1f3d7ca1b' },
    { code: '99', label: '账号 100', hash: '47516c9c60c568b0602db57e2b4001f207af678d462d34123a249be25f3d4ede'  }
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

    if (action === 'updateSet') {
        await updateSet(env, payload, access);
        return json({ ok: true, id: payload.id }, 200, request, env);
    }

    throw httpError(400, 'Unsupported action');
}

async function updateSet(env, payload, access) {
    const id = sanitizeId(payload.id);
    if (!id) throw httpError(400, '缺少 id 参数');

    const links = sanitizeLinks(payload.links);
    if (links.length === 0) throw httpError(400, '至少需要一个有效链接');

    const now = new Date().toISOString();

    const row = await env.DB.prepare('SELECT id, owner_code, current_index FROM link_sets WHERE id = ?').bind(id).first();
    if (!row) throw httpError(404, '链接组不存在');

    if (access.mode === 'owner' && row.owner_code !== access.owner.code) {
        throw httpError(403, '无权修改该链接组');
    }

    const keepIndex = Math.min(Number(row.current_index || 0), links.length - 1);

    await env.DB.prepare(
        'UPDATE link_sets SET links_json = ?, current_index = ?, updated_at = ? WHERE id = ?'
    ).bind(JSON.stringify(links), keepIndex, now, id).run();

    setLinksMemoryCache(env, id, links);
    await setLinksKvCache(env, id, links);
    await initDurableSet(env, id, links, keepIndex);
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