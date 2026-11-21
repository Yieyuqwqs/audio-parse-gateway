/**
 * @name 夜雨 · Rust 解析网关
 * @description 走本地 audio-parse-gateway（Termux 里的 Rust 服务）
 * @version 1.1.0
 * @author 夜雨
 */
const { EVENT_NAMES, request, on, send } = globalThis.lx

// 同一台手机：Termux 里网关默认监听 127.0.0.1:8080
const BASE_URL = 'http://127.0.0.1:8080'
const SUPPORTED_SOURCES = ['kw', 'kg', 'tx', 'wy', 'mg']
const QUALITY_LIST = ['128k', '320k', 'flac', 'flac24bit']

// 网关只起了 mock 上游时会返回这个域名——那种直链是假的，直接报清楚，别让播放器瞎试
const isMockUrl = (url) => typeof url === 'string' && url.includes('lx-mock.example')

const fetchMusicUrl = (params) => new Promise((resolve, reject) => {
  const { src, mid, quality, name, singer } = params
  const qs = new URLSearchParams({ quality })
  if (name) qs.set('name', name)
  if (singer) qs.set('singer', singer)
  // 带上 fallback=1：主源（比如 tx）彻底失败时，让网关拿歌名去网易云二次搜索
  if (name) qs.set('fallback', '1')

  const url = `${BASE_URL}/url/${src}/${encodeURIComponent(mid)}?${qs.toString()}`
  request(url, {}, (err, resp) => {
    if (err) return reject(new Error(`网关不可达：${err.message}`))

    let body
    try {
      body = JSON.parse(resp.body)
    } catch (e) {
      return reject(new Error(`网关返回的不是 JSON（HTTP ${resp.statusCode}）`))
    }

    if (body.code !== 0) return reject(new Error(body.msg || `code=${body.code}`))

    const data = body.data || {}
    if (!data.url) return reject(new Error('响应里没有 url'))
    if (isMockUrl(data.url)) {
      return reject(new Error('网关只有 mock 上游：请检查 Termux 里 ~/audio-parse-gateway/.env（ENABLE_QQ / ENABLE_NETEASE）并重启'))
    }
    resolve(data.url)
  })
})

// 从 LX 的 musicInfo 里挑出各平台对应的 id
const pickMid = (info, src) => {
  if (!info) return undefined
  if (src === 'tx') return info.songmid || info.strMediaMid
  if (src === 'kg') return info.hash
  if (src === 'kw') return info.rid || info.id
  if (src === 'wy') return info.id
  if (src === 'mg') return info.copyrightId
  return info.songmid || info.hash || info.strMediaMid || info.copyrightId || info.id
}

const actions = SUPPORTED_SOURCES.reduce((acc, src) => {
  acc[src] = {
    musicUrl(info, quality) {
      const mid = pickMid(info, src)
      if (!mid) return Promise.reject(new Error(`${src}: 拿不到歌曲 id`))
      return fetchMusicUrl({
        src,
        mid,
        quality,
        name: info.name,
        singer: typeof info.singer === 'string' ? info.singer : (info.singer || []).map?.((s) => s.name).join(' '),
      })
    },
  }
  return acc
}, {})

on(EVENT_NAMES.request, ({ source, action, info }) => {
  if (action !== 'musicUrl') return Promise.reject(new Error(`不支持的操作：${action}`))
  const handler = actions[source]
  if (!handler) return Promise.reject(new Error(`不支持的平台：${source}`))
  return handler.musicUrl(info.musicInfo, info.type)
})

send(EVENT_NAMES.inited, {
  sources: Object.fromEntries(
    SUPPORTED_SOURCES.map((src) => [
      src,
      {
        name: `夜雨-${src}`,
        type: 'music',
        actions: ['musicUrl'],
        qualitys: QUALITY_LIST,
      },
    ])
  ),
})
