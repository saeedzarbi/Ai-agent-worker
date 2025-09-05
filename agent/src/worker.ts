import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { prettyJSON } from 'hono/pretty-json';
import { FIELD_MAP, extractEstatePrompt } from './prompts';

// ---- Constants ----
const MAX_CONCURRENT_JOBS = 5;

// ---- Types ----
type Bindings = {
    DB: D1Database;
    AGENT_KV: KVNamespace;
    EXTRACT_QUEUE: Queue<ExtractMessage>;
    AGENT_BUCKET?: R2Bucket;
    // Secrets / ENV
    API_SECRET_TOKEN?: string;
    SLACK_WEBHOOK_URL?: string;
    CALLBACK_API_URL?: string;
    CALLBACK_API_KEY?: string;
    OPENROUTER_API_KEY?: string;
    GOOGLE_API_KEY?: string;
};

type ExtractMessage = {
    message_id: string;
    text: string;
    agent: 'chatgpt' | 'gemini' | 'openrouter';
    source?: string | null;
};

// ---- Hono App ----
const app = new Hono<{ Bindings: Bindings }>();
app.use('*', cors());
app.use('*', prettyJSON());

// ---- Utilities ----
function jsonDate() {
    return new Date().toISOString();
}

function assertAuth(req: Request, env: Bindings) {
    const key = req.headers.get('X-API-KEY') || '';
    if (!key) {
        return { ok: false, status: 403, body: { detail: 'Forbidden: API key is missing.' } };
    }
    if (!env.API_SECRET_TOKEN || key !== env.API_SECRET_TOKEN) {
        return { ok: false, status: 401, body: { detail: 'Unauthorized: Invalid API Key.' } };
    }
    return { ok: true as const };
}

async function d1Run(env: Bindings, sql: string, ...binds: unknown[]) {
    return env.DB.prepare(sql).bind(...binds).run();
}

async function d1First<T = any>(env: Bindings, sql: string, ...binds: unknown[]): Promise<T | null> {
    return (await env.DB.prepare(sql).bind(...binds).first()) as any;
}

// ---- Slack & Callback Helpers ----
async function sendToSlack(env: Bindings, msg: string, level: 'info' | 'warning' | 'error' | 'success' = 'info') {
    if (!env.SLACK_WEBHOOK_URL) return;
    const colors: Record<string, string> = { info: '#36a64f', warning: '#ffcc00', error: '#ff0000', success: '#36a64f' };
    await fetch(env.SLACK_WEBHOOK_URL, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ attachments: [{ color: colors[level] ?? '#36a64f', text: msg, ts: String(Date.now() / 1000) }] }),
    });
}


async function sendCallback(env: Bindings, payload: any): Promise<{ ok: boolean; status?: number; error?: string }> {
    // Check for missing secrets first
    if (!env.CALLBACK_API_URL || !env.CALLBACK_API_KEY) {
        return { ok: false, error: 'Callback URL or Key not configured' };
    }
    
    try {
        // Attempt the API call
        const response = await fetch(env.CALLBACK_API_URL, {
            method: 'POST',
            headers: { 'content-type': 'application/json', 'X-API-Key': env.CALLBACK_API_KEY },
            body: JSON.stringify(payload),
        });

        // Check if the server responded with an error
        if (!response.ok) {
            return { ok: false, status: response.status, error: `Callback endpoint returned status ${response.status}` };
        }

        // If successful, return a success object
        return { ok: true, status: response.status };

    } catch (e: any) {
        // If the fetch itself fails (e.g., network error), return a failure object
        return { ok: false, error: e.message };
    }
}

// ---- Agent Logic ----
function processApiResponse(content: string, originalText: string, env: Bindings, source: 'Gemini' | 'OpenRouter') {
    if (!content) {
        return { status: 'failed', message: `Empty response from ${source}` };
    }
    if (content.toLowerCase() === 'no') {
        return { status: 'reject', message: 'No real estate advertisement found' };
    }
    try {
        const ads = JSON.parse(content);
        const mapped = ads.map((ad: any) => {
            const out: Record<string, any> = { description: originalText.trim() };
            for (const [k, v] of Object.entries(FIELD_MAP)) {
                if (ad[k] !== undefined) out[v] = ad[k];
            }
            return out;
        });
        return { status: 'success', data: mapped };
    } catch (e: any) {
        sendToSlack(env, `❌ ${source} JSON parse error: ${e?.message}`, 'error');
        return { status: 'failed', message: `Could not parse JSON response: ${e?.message}` };
    }
}

async function callOpenRouter(env: Bindings, text: string) {
    if (!env.OPENROUTER_API_KEY) throw new Error('OPENROUTER_API_KEY missing');
    const prompt = extractEstatePrompt(text);
    const r = await fetch('https://openrouter.ai/api/v1/chat/completions', {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${env.OPENROUTER_API_KEY}`,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            model: 'deepseek/deepseek-chat-v3-0324:free',
            messages: [{ role: 'user', content: prompt }],
        }),
    });
    const data = await r.json<any>();
    const content = String(data?.choices?.[0]?.message?.content ?? '').replace(/```json|```/g, '').trim();
    return processApiResponse(content, text, env, 'OpenRouter');
}

async function callGemini(env: Bindings, text: string) {
    if (!env.GOOGLE_API_KEY) throw new Error('GOOGLE_API_KEY missing');
    const prompt = extractEstatePrompt(text);
    const r = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${env.GOOGLE_API_KEY}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] }),
    });
    const data = await r.json<any>();
    const content = String(data?.candidates?.[0]?.content?.parts?.[0]?.text ?? '').replace(/```json|```/g, '').trim();
    return processApiResponse(content, text, env, 'Gemini');
}

async function selectAgent(env: Bindings, agent: ExtractMessage['agent'], text: string) {
    switch (agent) {
        case 'openrouter':
        case 'chatgpt':
            return callOpenRouter(env, text);
        case 'gemini':
            return callGemini(env, text);
        default:
            throw new Error(`Invalid agent: ${agent}`);
    }
}
// Add this new helper function before the "---- Routes ----" section

async function doBackgroundTasks(env: Bindings, body: ExtractMessage, result: any) {

    const okNote = `✅ Completed processing\nMessage ID: ${body.message_id}\nStatus: ${result.status}`;
    await sendToSlack(env, okNote, 'success');
    

    const callbackResult = await sendCallback(env, {
        message_id: body.message_id,
        request_text: body.text,
        agent_used: body.agent,
        status: result.status,
        output_data: result,
        source: body.source ?? null,
    });


    if (callbackResult.ok) {
        await sendToSlack(env, `✅ Callback sent successfully for message ID: ${body.message_id}`, 'success');
    } else {
        await sendToSlack(env, `❌ Failed to send callback for message ID: ${body.message_id}\nError: ${callbackResult.error}`, 'error');
    }
}
// ---- Routes ----
app.get('/', c => c.json({ ok: true, service: 'agent-worker' }));

app.post('/extract', async c => {
    const auth = assertAuth(c.req.raw, c.env);
    if (!auth.ok) return c.json(auth.body, auth.status);

    const body = await c.req.json<ExtractMessage>();
    if (!body?.message_id || !body?.text || !body?.agent) {
        return c.json({ detail: 'message_id, text, agent are required' }, 422);
    }

    try {
  
        await d1Run(c.env, 'DELETE FROM message_records WHERE message_id = ?', body.message_id);
        await d1Run(c.env, 'INSERT INTO message_records (message_id, request_text, agent_used, status, source) VALUES (?,?,?,?,?)',
            body.message_id, body.text, body.agent, 'processing', body.source ?? null
        );

        const result = await selectAgent(c.env, body.agent, body.text);

        await d1Run(c.env, 'UPDATE message_records SET status = ?, output_data = ? WHERE message_id = ?',
            result.status, JSON.stringify(result), body.message_id
        );

        // --- FIX: USE waitUntil FOR BACKGROUND TASKS ---
        c.executionCtx.waitUntil(doBackgroundTasks(c.env, body, result));
        

        return c.json({
            message_id: body.message_id,
            status: result.status,
            output_data: result,
            source: body.source ?? null,
            created_at: jsonDate(),
        });

    } catch (e: any) {
        await d1Run(c.env, 'UPDATE message_records SET status = ?, output_data = ? WHERE message_id = ?', 'failed', JSON.stringify({ message: e.message }), body.message_id);
        c.executionCtx.waitUntil(sendToSlack(c.env, `❌ Processing failed for ${body.message_id}: ${e?.message || e}`, 'error'));
        return c.json({ detail: "Processing failed", error: e.message }, 500);
    }
});

app.get('/status/:id', async c => {
    const auth = assertAuth(c.req.raw, c.env);
    if (!auth.ok) return c.json(auth.body, auth.status);

    const id = c.req.param('id');
    const row = await d1First<any>(c.env, 'SELECT message_id, status, agent_used, output_data, created_at, source FROM message_records WHERE message_id = ?', id);
    if (!row) return c.text('Message not found', 404);

    let output;
    try {
        output = row.output_data ? JSON.parse(row.output_data) : {};
    } catch {
        output = row.output_data;
    }

    return c.json({
        message_id: row.message_id,
        status: row.status,
        agent_used: row.agent_used,
        output_data: output,
        created_at: row.created_at,
        source: row.source,
    });
});

app.get('/queue/info', async c => {
    const auth = assertAuth(c.req.raw, c.env);
    if (!auth.ok) return c.json(auth.body, auth.status);
    const size = Number(await c.env.AGENT_KV.get('queue_size')) || 0;
    const active = Number(await c.env.AGENT_KV.get('active_processing')) || 0;
    return c.json({
        queue_size: size,
        max_concurrent_processing: MAX_CONCURRENT_JOBS,
        available_processing_slots: Math.max(0, MAX_CONCURRENT_JOBS - active),
    });
});

app.post('/media', async c => {
    const auth = assertAuth(c.req.raw, c.env);
    if (!auth.ok) return c.json(auth.body, auth.status);
    const body = await c.req.json<{ file: string }>();
    if (!body?.file) return c.json({ detail: 'file is required' }, 422);

    await d1Run(c.env, 'INSERT INTO whatsapp_media (file_name, url) VALUES (?, ?)', body.file, body.file);

    if (c.env.CALLBACK_API_KEY && c.env.CALLBACK_API_URL) {
        await sendCallback(c.env, { file: body.file });
    }
    return c.json({ ok: true });
});

// ---- Worker Entrypoint ----
export default {
    fetch: app.fetch,
};
