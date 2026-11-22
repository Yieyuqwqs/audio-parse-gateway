#!/data/data/com.termux/files/usr/bin/bash
# audio-parse-gateway 一键自检
#   bash ~/audio-parse-gateway/check.sh
# 会依次验证：health → 解析 tx/wy → 直链能不能真拉到字节
set -uo pipefail

BASE="${BASE:-http://127.0.0.1:8080}"
say() { printf '\033[36m[check]\033[0m %s\n' "$*"; }
bad() { printf '\033[31m[check]\033[0m %s\n' "$*"; }

command -v curl >/dev/null 2>&1 || { bad '缺少 curl：pkg install curl'; exit 1; }

say "网关健康检查 ${BASE}/health"
health=$(curl -s -m 6 "$BASE/health") || { bad '网关没起来（run.sh 跑了吗？）'; exit 1; }
printf '%s\n' "$health"
case "$health" in
  *'"mock_upstreams":0'*) say '没有 mock 上游 ✅' ;;
  *'"mock_upstreams":0,'*) say '没有 mock 上游 ✅' ;;
  *) bad '⚠️ 仍有 mock 上游：真实音源没配上（检查 ~/audio-parse-gateway/.env 与 run.sh 是否 source env.sh）' ;;
esac
echo

check_one() {
  src="$1"; id="$2"
  say "解析 ${src} 320k"
  resp=$(curl -s -m 10 "$BASE/api/parse?id=${id}&source=${src}&quality=320k")
  printf '  %s\n' "$resp"
  url=$(printf '%s' "$resp" | sed -n 's/.*"url":"\([^"]*\)".*/\1/p')
  if [ -z "$url" ]; then bad '  没有返回 url'; return; fi
  case "$url" in
    *lx-mock.example*)
      bad '  ⚠️ 这是 mock 假直链（不是真音源）'
      return
      ;;
    */stream/*)
      say '  走的是网关代拉（会补 Referer）'
      ;;
  esac

  say '  试拉 2 字节（不带 Referer，模拟播放器）'
  read -r code size <<EOF
$(curl -s -o /dev/null -m 12 -w '%{http_code} %{size_download}' -r 0-1 "$url")
EOF
  printf '  HTTP %s / %s bytes\n' "${code:-?}" "${size:-?}"
  if [ "${size:-0}" = "0" ]; then
    bad '  ⚠️ 拿到 200 但 0 字节：被防盗链挡了，应该返回 /stream 代拉地址'
  else
    say '  直链能拉到数据 ✅'
  fi
  echo
}

check_one tx 002lzYCa14usQ5
check_one wy 347230
