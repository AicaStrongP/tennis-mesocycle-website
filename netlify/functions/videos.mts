import { getStore } from '@netlify/blobs'
import type { Config } from '@netlify/functions'

// Videos are uploaded in small chunks so each request stays well under the
// function payload limit, then played back with HTTP Range requests that
// return one chunk at a time (which also makes seeking work).
const CHUNK_SIZE = 3 * 1024 * 1024
const MAX_SIZE = 200 * 1024 * 1024
const MAX_CHUNKS = Math.ceil(MAX_SIZE / CHUNK_SIZE)

// Two 3 MB chunks per playback response: fewer round trips, while staying
// well inside the streaming response limit and the function time limit.
const CHUNKS_PER_RESPONSE = 2

// Warm function instances remember manifests so playback skips a lookup.
const manifestCache = new Map<string, Manifest>()

const KEY_RE = /^[a-z0-9-]{1,40}$/
const ID_RE = /^[a-z0-9-]{8,64}$/

type Manifest = {
  id: string
  name: string
  type: string
  size: number
  chunks: number
  chunkSize: number
  uploadedAt: string
}

const store = () => getStore({ name: 'exercise-videos', consistency: 'strong' })

const json = (data: unknown, status = 200) =>
  Response.json(data, { status, headers: { 'Cache-Control': 'no-store' } })

const publicInfo = (key: string, m: Manifest) => ({
  key,
  name: m.name,
  type: m.type,
  size: m.size,
  uploadedAt: m.uploadedAt,
  url: `/api/videos/${key}/stream?v=${m.id}`,
})

async function deleteUpload(id: string) {
  const s = store()
  const { blobs } = await s.list({ prefix: `chunks/${id}/` })
  await Promise.all(blobs.map((b) => s.delete(b.key)))
}

export default async (req: Request) => {
  const url = new URL(req.url)
  const parts = url.pathname.replace(/^\/api\/videos\/?/, '').split('/').filter(Boolean)
  const [key, action] = parts
  const s = store()

  // GET /api/videos -> all saved videos
  if (!key) {
    if (req.method !== 'GET') return json({ error: 'Method not allowed' }, 405)
    const { blobs } = await s.list({ prefix: 'videos/' })
    const out: Record<string, unknown> = {}
    await Promise.all(
      blobs.map(async (b) => {
        const m = (await s.get(b.key, { type: 'json' })) as Manifest | null
        const k = b.key.slice('videos/'.length)
        if (m) out[k] = publicInfo(k, m)
      }),
    )
    return json(out)
  }

  if (!KEY_RE.test(key)) return json({ error: 'Invalid video key' }, 400)
  const manifestKey = `videos/${key}`

  // POST /api/videos/:key/chunk?upload=ID&i=N  (raw bytes)
  if (action === 'chunk' && req.method === 'POST') {
    const id = url.searchParams.get('upload') || ''
    const i = Number(url.searchParams.get('i'))
    if (!ID_RE.test(id) || !Number.isInteger(i) || i < 0 || i >= MAX_CHUNKS) {
      return json({ error: 'Invalid chunk' }, 400)
    }
    const body = await req.arrayBuffer()
    if (body.byteLength === 0 || body.byteLength > CHUNK_SIZE) {
      return json({ error: 'Invalid chunk size' }, 400)
    }
    await s.set(`chunks/${id}/${i}`, body)
    return json({ ok: true })
  }

  // POST /api/videos/:key/complete  {upload, chunks, size, type, name}
  if (action === 'complete' && req.method === 'POST') {
    const body = (await req.json().catch(() => null)) as Record<string, unknown> | null
    const id = String(body?.upload || '')
    const chunks = Number(body?.chunks)
    const size = Number(body?.size)
    const type = String(body?.type || 'video/mp4')
    const name = String(body?.name || 'video').slice(0, 200)
    if (
      !ID_RE.test(id) ||
      !Number.isInteger(chunks) ||
      chunks < 1 ||
      chunks > MAX_CHUNKS ||
      !Number.isFinite(size) ||
      size <= 0 ||
      size > MAX_SIZE ||
      chunks !== Math.ceil(size / CHUNK_SIZE) ||
      !type.startsWith('video/')
    ) {
      return json({ error: 'Invalid upload' }, 400)
    }
    const { blobs } = await s.list({ prefix: `chunks/${id}/` })
    if (blobs.length !== chunks) return json({ error: 'Upload is incomplete, please try again' }, 409)

    const previous = (await s.get(manifestKey, { type: 'json' })) as Manifest | null
    const manifest: Manifest = {
      id,
      name,
      type,
      size,
      chunks,
      chunkSize: CHUNK_SIZE,
      uploadedAt: new Date().toISOString(),
    }
    await s.setJSON(manifestKey, manifest)
    manifestCache.set(key, manifest)
    if (previous && previous.id !== id) await deleteUpload(previous.id)
    return json(publicInfo(key, manifest))
  }

  // DELETE /api/videos/:key
  if (!action && req.method === 'DELETE') {
    const previous = (await s.get(manifestKey, { type: 'json' })) as Manifest | null
    await s.delete(manifestKey)
    manifestCache.delete(key)
    if (previous) await deleteUpload(previous.id)
    return json({ ok: true })
  }

  // GET /api/videos/:key/stream  (supports Range requests)
  if (action === 'stream' && (req.method === 'GET' || req.method === 'HEAD')) {
    const wanted = url.searchParams.get('v') || ''
    let m = manifestCache.get(key)
    if (!m || m.id !== wanted) {
      m = ((await s.get(manifestKey, { type: 'json' })) as Manifest | null) ?? undefined
      if (!m) {
        manifestCache.delete(key)
        return new Response('Not found', { status: 404 })
      }
      manifestCache.set(key, m)
    }

    let start = 0
    let end = m.size - 1
    const range = req.headers.get('range')
    if (range) {
      const match = /^bytes=(\d*)-(\d*)$/.exec(range.trim())
      if (!match || (match[1] === '' && match[2] === '')) {
        return new Response(null, { status: 416, headers: { 'Content-Range': `bytes */${m.size}` } })
      }
      if (match[1] === '') {
        start = Math.max(0, m.size - Number(match[2]))
      } else {
        start = Number(match[1])
        if (match[2] !== '') end = Math.min(Number(match[2]), m.size - 1)
      }
      if (start >= m.size || start > end) {
        return new Response(null, { status: 416, headers: { 'Content-Range': `bytes */${m.size}` } })
      }
    }

    // Serve up to CHUNKS_PER_RESPONSE stored chunks per response, streamed.
    const cs = m.chunkSize
    const first = Math.floor(start / cs)
    end = Math.min(end, (first + CHUNKS_PER_RESPONSE) * cs - 1)
    const last = Math.floor(end / cs)

    const headers = {
      'Content-Type': m.type,
      'Accept-Ranges': 'bytes',
      'Content-Range': `bytes ${start}-${end}/${m.size}`,
      'Content-Length': String(end - start + 1),
      // The URL carries the upload id, so the bytes behind it never change.
      'Cache-Control': m.id === wanted ? 'private, max-age=31536000, immutable' : 'no-cache',
    }
    if (req.method === 'HEAD') return new Response(null, { status: 206, headers })

    // Fetch the needed chunks in parallel, then stream them out in order so
    // the browser gets the first bytes as soon as the first chunk arrives.
    const id = m.id
    const pending: Promise<ArrayBuffer | null>[] = []
    for (let i = first; i <= last; i++) {
      pending.push(s.get(`chunks/${id}/${i}`, { type: 'arrayBuffer' }) as Promise<ArrayBuffer | null>)
    }
    pending.forEach((p) => p.catch(() => null))

    const firstData = await pending[0]
    if (!firstData) return new Response('Video data missing', { status: 500 })

    let n = 0
    const stream = new ReadableStream<Uint8Array>({
      async pull(controller) {
        const i = first + n
        const data = n === 0 ? firstData : await pending[n].catch(() => null)
        if (!data) {
          controller.error(new Error('Video data missing'))
          return
        }
        const chunkStart = i * cs
        const from = Math.max(start, chunkStart) - chunkStart
        const to = Math.min(end, chunkStart + cs - 1) - chunkStart + 1
        controller.enqueue(new Uint8Array(data, from, to - from))
        n++
        if (first + n > last) controller.close()
      },
    })
    return new Response(stream, { status: 206, headers })
  }

  return json({ error: 'Not found' }, 404)
}

export const config: Config = {
  path: ['/api/videos', '/api/videos/*'],
}
