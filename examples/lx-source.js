/**
 * @name 夜雨 · Rust 网关
 * @description 走本机 audio-parse-gateway（Termux 里那个 Rust 服务）
 * @version 2.0.0
 * @author 夜雨
 */
var lx = globalThis.lx
var EVENT_NAMES = lx.EVENT_NAMES
var request = lx.request
var on = lx.on
var send = lx.send

// ====== 只改这里就行 ======
var BASE_URL = 'http://127.0.0.1:8080'   // 网关地址
var TIMEOUT_MS = 10000                   // 客户端兜底超时
// ==========================

var SOURCES = ['tx', 'wy', 'kw', 'kg', 'mg']

// 每个平台声明的音质：上限写真实能力，别乱吹（吹了 LX 会一直请求拿不到的档）
// tx：账号权益决定能到哪一档，网关会如实降档并返回能用的链接
// kw / kg / mg 现在都是网关直连（零凭据）：
//   kw 无会员 cookie 实测到 320k，kg 免费曲能到 flac，mg 匿名只有 128k；
//   LX 照下面声明即可，网关会把实际档位放在 data.quality 里如实上报（downgraded 标记）
var QUALITYS = {
  tx: ['128k', '320k', 'flac', 'flac24bit'],
  wy: ['128k', '320k', 'flac', 'flac24bit'],
  kw: ['128k', '320k', 'flac'],
  kg: ['128k', '320k', 'flac'],
  mg: ['128k', '320k'],
}

// 各平台的 id 字段不一样，按优先级挑第一个非空的
function pickMid(source, info) {
  if (!info) return ''
  // 咪咕的试听地址要「版权 id + 内容 id」两段，拼成 copyrightId_contentId 再交给网关
  if (source === 'mg') {
    var copyrightId = String(info.copyrightId || info.id || '').trim()
    var contentId = String(info.contentId || info.strMediaMid || info.id || '').trim()
    if (copyrightId && contentId) return copyrightId + '_' + contentId
    return copyrightId || contentId
  }
  var bySource = {
    tx: [info.songmid, info.strMediaMid],
    wy: [info.id, info.songmid],
    kw: [info.rid, info.id],
    kg: [info.hash, info.songmid],
  }
  var list = bySource[source] || [
    info.songmid, info.hash, info.strMediaMid, info.copyrightId, info.id, info.rid,
  ]
  for (var i = 0; i < list.length; i++) {
    var v = list[i]
    if (v !== undefined && v !== null && String(v).trim() !== '') return String(v).trim()
  }
  return ''
}

function singerOf(info) {
  if (!info || !info.singer) return ''
  var s = info.singer
  if (typeof s === 'string') return s
  if (Object.prototype.toString.call(s) === '[object Array]') {
    var names = []
    for (var i = 0; i < s.length; i++) {
      if (s[i] && s[i].name) names.push(s[i].name)
    }
    return names.join(' ')
  }
  return ''
}

// 统一请求网关：解析 JSON、判 code、拦 mock 假直链
function fetchGateway(url) {
  return new Promise(function (resolve, reject) {
    var settled = false
    var timer = null
    if (typeof setTimeout === 'function') {
      timer = setTimeout(function () {
        if (settled) return
        settled = true
        reject(new Error('网关超时（' + TIMEOUT_MS + 'ms）'))
      }, TIMEOUT_MS)
    }

    request(url, {}, function (err, resp) {
      if (settled) return
      settled = true
      if (timer) clearTimeout(timer)

      if (err) return reject(new Error('网关不可达：' + err.message))

      var body
      try {
        body = JSON.parse(resp.body)
      } catch (e) {
        return reject(new Error('网关返回的不是 JSON（HTTP ' + resp.statusCode + '）'))
      }
      if (!body || body.code !== 0) {
        return reject(new Error((body && body.msg) || ('解析失败 code=' + (body && body.code))))
      }

      var data = body.data || {}
      if (!data.url) return reject(new Error('响应里没有 url'))
      // 网关只有 mock 上游时会给假地址，直接说清楚，别让播放器瞎试
      if (data.mock === true || String(data.url).indexOf('lx-mock.example') >= 0) {
        return reject(new Error('网关只有 mock 上游：检查 Termux 里 ~/audio-parse-gateway/.env 的 ENABLE_QQ / ENABLE_NETEASE'))
      }
      resolve(data)
    })
  })
}

function musicUrl(source, info, quality) {
  var mid = pickMid(source, info)
  if (!mid) return Promise.reject(new Error(source + ': 缺少歌曲 id'))

  var params = ['quality=' + encodeURIComponent(quality)]
  var name = (info && info.name) || ''
  if (name) {
    // 带上歌名/歌手 + fallback=1：主源彻底失败时，网关会拿关键词去网易云二次搜索
    params.push('name=' + encodeURIComponent(name))
    var singer = singerOf(info)
    if (singer) params.push('singer=' + encodeURIComponent(singer))
    params.push('fallback=1')
    // multi=1：主源没权益/挂了，就让网关拿同一个 id 去 kg / kw / mg 找（这几家零凭据就能播）
    params.push('multi=1')
  }

  var url = BASE_URL + '/url/' + source + '/' + encodeURIComponent(mid) + '?' + params.join('&')
  return fetchGateway(url).then(function (data) {
    return data.url
  })
}

on(EVENT_NAMES.request, function (payload) {
  if (!payload || payload.action !== 'musicUrl') {
    return Promise.reject(new Error('不支持的操作：' + (payload && payload.action)))
  }
  var info = payload.info || {}
  if (SOURCES.indexOf(payload.source) < 0) {
    return Promise.reject(new Error('不支持的源：' + payload.source))
  }
  return musicUrl(payload.source, info.musicInfo, info.type)
})

var sources = {}
for (var i = 0; i < SOURCES.length; i++) {
  var s = SOURCES[i]
  sources[s] = {
    name: '夜雨-' + s,
    type: 'music',
    actions: ['musicUrl'],
    qualitys: QUALITYS[s],
  }
}

send(EVENT_NAMES.inited, { sources: sources })
