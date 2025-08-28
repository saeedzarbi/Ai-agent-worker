import { Hono } from 'hono'
import { cors } from 'hono/cors'
import { prettyJSON } from 'hono/pretty-json'

// ---- Types ----
type Bindings = {
    DB: D1Database
    AGENT_KV: KVNamespace
    EXTRACT_QUEUE: Queue < ExtractMessage >
        // Optional: R2 bucket if you want to store files
        AGENT_BUCKET ? : R2Bucket
        // Secrets / ENV
    API_SECRET_TOKEN ? : string
    SLACK_WEBHOOK_URL ? : string
    CALLBACK_API_URL ? : string
    CALLBACK_API_KEY ? : string
    OPENROUTER_API_KEY ? : string
    GOOGLE_API_KEY ? : string
}

type ExtractMessage = {
    message_id: string
    text: string
    agent: 'chatgpt' | 'gemini' | 'openrouter'
    source ? : string | null
}

// ---- Prompt & mapping (ported from prompt_logic.py) ----
const FIELD_MAP: Record < string, string > = {
    a: 'ad_type',
    t: 'title',
    p: 'property_type',
    ta: 'total_area',
    la: 'land_area',
    lt: 'listing_type',
    pr: 'price',
    ra: 'relative_address',
    tf: 'total_floors',
    f: 'floor',
    d: 'details',
    ma: 'mortgage_amount',
    rn: 'rent_amount',
    ds: 'description',
    con: 'contact_name',
    cop: 'contact_phone',
    r: 'rooms',
}

function extractEstatePrompt(text: string): string {
    return `اگر متن زیر مربوط به آگهی ملک نیست، فقط مقدار "no" را بازگردان.متن زیر به زبان فارسی است و شامل آگهی‌های ملکی است:
در غیر این‌صورت، تمام آگهی‌های موجود در متن را استخراج کن و برای هر آگهی یک شیء JSON با کلیدهای کوتاه‌شده زیر برگردان.
توجه داشته باش که ممکن است در متن چندین آگهی وجود داشته باشد، در این صورت باید همه آگهی‌ها را جداگانه استخراج کنی و در آرایه JSON قرار دهی.

کلیدهای هر آگهی:
- a: نوع آگهی. 1 برای آگهی‌دهنده (offer)، 2 برای درخواست‌کننده (request)
- t: عنوان خلاصه‌شده آگهی. اگر نبود، خودت بساز.
- p: نوع ملک. فقط عدد یکی از این موارد: 1 = آپارتمان، 3 = دفتر کار، 4 = مغازه، 5 = دفاتر صنعتی، 6 = زمین، 7 = سایر
- ta: متراژ ملک به مترمربع. عدد خالص بدون واحد.
- la: متراژ زمین، در صورت ذکر.
- lt: نوع معامله: 1 = اجاره، 2 = فروش
- pr: قیمت کل ملک (در صورت فروش).
- ra: آدرس نسبی/ناحیه در متن.
- tf: تعداد کل طبقات ساختمان.
- r: تعداد اتاق.
- f: طبقه ملک.
- d: ویژگی‌های خاص (full,elevator,deed,per_floor,parking,loan,no_view,no_key)
- ma: مبلغ رهن (ریال) در صورت اجاره.
- rn: مبلغ اجاره (ریال) در صورت اجاره.
- cop: شماره تماس در متن.
- con: نام فرد آگهی‌گذار.

هر آگهی را به صورت یک عنصر در آرایه JSON خروجی برگردان.
متن ورودی:
"""${text}"""`
}

// ---- Utilities ----
const app = new Hono < { Bindings: Bindings } > ()
app.use('*', cors())
app.use('*', prettyJSON())

function jsonDate() { return new Date().toISOString() }

function assertAuth(req: Request, env: Bindings) {
    const key = req.headers.get('X-API-KEY') || ''
    if (!key) return { ok: false, status: 403, body: { detail: 'Forbidden: API key is missing.' } }
    if (!env.API_SECRET_TOKEN || key !== env.API_SECRET_TOKEN) {
        return { ok: false, status: 401, body: { detail: 'Unauthorized: Invalid API Key.' } }
    }
    return { ok: true as
        const }
}

async function d1Run(env: Bindings, sql: string, ...binds: unknown[]) {
    return env.DB.prepare(sql).bind(...binds).run()
}
async function d1First < T = any > (env: Bindings, sql: string, ...binds: unknown[]): Promise < T | null > {
    return (await env.DB.prepare(sql).bind(...binds).first()) as any
}
async function d1All < T = any > (env: Bindings, sql: string, ...binds: unknown[]): Promise < T[] > {
    const res = await env.DB.prepare(sql).bind(...binds).all < T > ()
    return res.results ? ? []
}

// ---- Slack & Callback helpers ----
async function sendToSlack(env: Bindings, msg: string, level: 'info' | 'warning' | 'error' | 'success' = 'info') {
    if (!env.SLACK_WEBHOOK_URL) return
    const colors: Record < string, string > = { info: '#36a64f', warning: '#ffcc00', error: '#ff0000', success: '#36a64f' }
    await fetch(env.SLACK_WEBHOOK_URL, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ attachments: [{ color: colors[level] ? ? '#36a64f', text: msg, ts: String(Date.now() / 1000) }] })
    })
}

async function sendCallback(env: Bindings, payload: any) {
    if (!env.CALLBACK_API_URL || !env.CALLBACK_API_KEY) return
    await fetch(env.CALLBACK_API_URL, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'X-API-Key': env.CALLBACK_API_KEY },
        body: JSON.stringify(payload)
    })
}

// ---- Agent logic (ported) ----
async function callOpenRouter(env: Bindings, text: string) {
    if (!env.OPENROUTER_API_KEY) throw new Error('OPENROUTER_API_KEY missing')
    const prompt = extractEstatePrompt(text)
    const r = await fetch('https://openrouter.ai/api/v1/chat/completions', {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${env.OPENROUTER_API_KEY}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            model: 'deepseek/deepseek-chat-v3-0324:free',
            messages: [{ role: 'user', content: prompt }]
        })
    })
    const data = await r.json < any > ()
    const content = String(data ? .choices ? .[0] ? .message ? .content ? ? '').replace('```json', '').replace('```', '').trim()
    if (content.toLowerCase() === 'no') return { status: 'reject', message: 'No real estate advertisement found' }
    try {
        const ads = JSON.parse(content)
        const mapped = ads.map((ad: any) => {
            const out: Record < string, any > = { description: text.trim() }
            for (const [k, v] of Object.entries(FIELD_MAP)) {
                if (ad[k] !== undefined) out[v] = ad[k]
            }
            return out
        })
        return { status: 'success', data: mapped }
    } catch (e: any) {
        await sendToSlack(env, `❌ OpenRouter JSON parse error: ${e?.message}`, 'error')
        return { status: 'failed', message: `Could not parse JSON response: ${e?.message}` }
    }
}

async function callGemini(env: Bindings, text: string) {
    // Minimal REST call; adjust model if needed
    if (!env.GOOGLE_API_KEY) throw new Error('GOOGLE_API_KEY missing')
    const prompt = extractEstatePrompt(text)
    const r = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${env.GOOGLE_API_KEY}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] })
    })
    const data = await r.json < any > ()
    const content = String(data ? .candidates ? .[0] ? .content ? .parts ? .[0] ? .text ? ? '').replace('```json', '').replace('```', '').trim()
    if (!content) return { status: 'failed', message: 'Empty response from Gemini' }
    if (content.toLowerCase() === 'no') return { status: 'reject', message: 'No real estate advertisement found' }
    try {
        const ads = JSON.parse(content)
        const mapped = ads.map((ad: any) => {
            const out: Record < string, any > = { description: text.trim() }
            for (const [k, v] of Object.entries(FIELD_MAP)) {
                if (ad[k] !== undefined) out[v] = ad[k]
            }
            return out
        })
        return { status: 'success', data: mapped }
    } catch (e: any) {
        await sendToSlack(env, `❌ Gemini JSON parse error: ${e?.message}`, 'error')
        return { status: 'failed', message: `Could not parse JSON response: ${e?.message}` }
    }
}

async function selectAgent(env: Bindings, agent: ExtractMessage['agent'], text: string) {
    switch (agent) {
        case 'openrouter':
        case 'chatgpt':
            return callOpenRouter(env, text)
        case 'gemini':
            return callGemini(env, text)
        default:
            throw new Error(`Invalid agent: ${agent}`)
    }
}

// ---- Routes ----
app.get('/', c => c.json({ ok: true, service: 'agent-worker' }))

app.post('/extract', async(c) => {
    const auth = assertAuth(c.req.raw, c.env)
    if (!('ok' in auth)) return c.json(auth.body, auth.status)

    const body = await c.req.json < { message_id: string, text: string, agent: ExtractMessage['agent'], source ? : string } > ()
    if (!body ? .message_id || !body ? .text || !body ? .agent) {
        return c.json({ detail: 'message_id, text, agent are required' }, 422)
    }
    if (body.text.length < 10) return c.json({ detail: 'text must be at least 10 chars' }, 422)
    if (!['chatgpt', 'gemini', 'openrouter'].includes(body.agent)) return c.json({ detail: 'invalid agent' }, 422)

    await d1Run(c.env, 'DELETE FROM message_records WHERE message_id = ?', body.message_id)
    await d1Run(c.env,
        'INSERT INTO message_records (message_id, request_text, agent_used, status, output_data, source) VALUES (?,?,?,?,?,?)',
        body.message_id, body.text, body.agent, 'queued', JSON.stringify({ message: 'Request queued for processing' }), body.source ? ? null
    )

    // Approx queue size via KV
    const size = Number(await c.env.AGENT_KV.get('queue_size')) || 0
    await c.env.AGENT_KV.put('queue_size', String(size + 1))

    // Enqueue for background processing
    await c.env.EXTRACT_QUEUE.send({ message_id: body.message_id, text: body.text, agent: body.agent, source: body.source ? ? null })

    return c.json({
        message_id: body.message_id,
        status: 'queued',
        data: [],
        source: body.source ? ? null,
        created_at: jsonDate()
    })
})

app.get('/status/:id', async(c) => {
    const auth = assertAuth(c.req.raw, c.env)
    if (!('ok' in auth)) return c.json(auth.body, auth.status)

    const id = c.req.param('id')
    const row = await d1First < any > (c.env, 'SELECT message_id, status, agent_used, output_data, created_at, source FROM message_records WHERE message_id = ?', id)
    if (!row) return c.text('Message not found', 404)
    let output
    try { output = row.output_data ? JSON.parse(row.output_data) : {} } catch { output = row.output_data }
    return c.json({
        message_id: row.message_id,
        status: row.status,
        agent_used: row.agent_used,
        output_data: output,
        created_at: row.created_at,
        source: row.source
    })
})

app.get('/queue/info', async(c) => {
    const auth = assertAuth(c.req.raw, c.env)
    if (!('ok' in auth)) return c.json(auth.body, auth.status)
    const size = Number(await c.env.AGENT_KV.get('queue_size')) || 0
    const active = Number(await c.env.AGENT_KV.get('active_processing')) || 0
    return c.json({ queue_size: size, max_concurrent_processing: 3, available_processing_slots: Math.max(0, 3 - active) })
})

app.post('/media', async(c) => {
    const auth = assertAuth(c.req.raw, c.env)
    if (!('ok' in auth)) return c.json(auth.body, auth.status)
    const body = await c.req.json < { file: string } > ()
    if (!body ? .file) return c.json({ detail: 'file is required' }, 422)

    await d1Run(c.env, 'INSERT INTO whatsapp_media (file_name, url) VALUES (?, ?)', body.file, body.file)

    // Optionally notify external hook (same contract as your s3_utils)
    if (c.env.CALLBACK_API_KEY && c.env.CALLBACK_API_URL) {
        await sendCallback(c.env, { file: body.file })
    }

    // (Optional) R2 upload could go here if you want to accept data URLs / streams
    return c.json({ ok: true })
})

// ---- Queue consumer ----
export default {
    fetch: app.fetch,
    async queue(batch: MessageBatch < ExtractMessage > , env: Bindings, ctx: ExecutionContext) {
        for (const msg of batch.messages) {
            const { message_id, text, agent, source } = msg.body
            try {
                // Track active slots in KV (best-effort)
                const active = Number(await env.AGENT_KV.get('active_processing')) || 0
                await env.AGENT_KV.put('active_processing', String(active + 1))

                // Mark processing
                await d1Run(env, 'UPDATE message_records SET status = ? WHERE message_id = ?', 'processing', message_id)

                // Process
                const result = await selectAgent(env, agent, text)

                // Persist
                await d1Run(env, 'UPDATE message_records SET status = ?, output_data = ?, source = ? WHERE message_id = ?',
                    result.status, JSON.stringify(result), source ? ? null, message_id)

                // Slack & callback
                const okNote = `✅ Completed processing\nMessage ID: ${message_id}\nStatus: ${result.status}`
                await sendToSlack(env, okNote, 'success')
                await sendCallback(env, {
                    message_id,
                    request_text: text,
                    agent_used: agent,
                    status: result.status,
                    output_data: result,
                    source: source ? ? null,
                })

                msg.ack()
            } catch (e: any) {
                await d1Run(env, 'UPDATE message_records SET status = ?, output_data = ? WHERE message_id = ?', 'failed', JSON.stringify({ message: String(e ? .message || e) }), message_id)
                await sendToSlack(env, `❌ Processing failed for ${message_id}: ${e?.message || e}`, 'error')
                msg.retry()
            } finally {
                // decrement approximate counters
                const size = Number(await env.AGENT_KV.get('queue_size')) || 0
                await env.AGENT_KV.put('queue_size', String(Math.max(0, size - 1)))
                const active = Number(await env.AGENT_KV.get('active_processing')) || 0
                await env.AGENT_KV.put('active_processing', String(Math.max(0, active - 1)))
            }
        }
    }
}