#!/data/data/com.termux/files/usr/bin/bash
# audio-parse-gateway · Termux 一键安装
#
# 用法：在 Termux 里执行
#   bash /sdcard/dsh-deploy/audio-parse-gateway-aarch64-linux-android/install.sh
#   或（从 Release 拉的包）：bash ~/apg/install.sh
set -euo pipefail

APP_DIR="${HOME}/audio-parse-gateway"
BIN="${APP_DIR}/audio-parse-gateway"
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"

say() { printf '\033[36m[termux]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[termux]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

[ -n "${PREFIX:-}" ] || die "请用 Termux 自带的 bash 运行（\$PREFIX 未设置）"

say "创建目录 ${APP_DIR}"
mkdir -p "$APP_DIR"

# 共享存储（/sdcard）是 FUSE 挂载，exec 位不一定带得过来，所以只判「文件存在」
if [ -f "${SELF_DIR}/audio-parse-gateway" ]; then
  say "使用安装包自带的二进制"
  cp "${SELF_DIR}/audio-parse-gateway" "$BIN"
else
  command -v curl >/dev/null 2>&1 || die "缺少 curl：pkg install curl 后再跑"
  VER="${APG_VERSION:-latest}"
  if [ "$VER" = "latest" ]; then
    URL="https://github.com/Yieyuqwqs/audio-parse-gateway/releases/latest/download/audio-parse-gateway-aarch64-linux-android.tar.gz"
  else
    URL="https://github.com/Yieyuqwqs/audio-parse-gateway/releases/download/${VER}/audio-parse-gateway-aarch64-linux-android.tar.gz"
  fi
  say "下载 ${URL}"
  curl -fL --retry 3 -o "${APP_DIR}/pkg.tgz" "$URL"
  tar -xzf "${APP_DIR}/pkg.tgz" -C "${APP_DIR}" --strip-components=1
  rm -f "${APP_DIR}/pkg.tgz"
fi

chmod +x "$BIN"
cp "${SELF_DIR}/check.sh" "${APP_DIR}/check.sh" 2>/dev/null || true
[ -f "${APP_DIR}/check.sh" ] && chmod +x "${APP_DIR}/check.sh"
say "二进制：$(ls -lh "$BIN" | awk '{print $5}')"

# ---------- 凭据 ----------
if [ -f "${SELF_DIR}/env" ]; then
  cp "${SELF_DIR}/env" "${APP_DIR}/.env"
  chmod 600 "${APP_DIR}/.env"
  say "已写入 .env（QQ / 网易云凭据）"
  case "${SELF_DIR}" in
    /sdcard/*|/storage/emulated/0/*)
      rm -f "${SELF_DIR}/env"
      say "已删除共享存储里的凭据副本"
      ;;
  esac
elif [ -f "${APP_DIR}/.env" ]; then
  say "沿用已有的 .env"
else
  warn "没有 .env：网关只会起两个 mock 上游，返回的直链是假的（播放器会「换源失败」）"
fi

# .env -> env.sh（值用单引号包起来，cookie 里有空格和分号也不会炸）
if [ -f "${APP_DIR}/.env" ]; then
  : > "${APP_DIR}/env.sh"
  while IFS= read -r line; do
    case "$line" in ''|'#'*) continue ;; esac
    case "$line" in *=*) ;; *) continue ;; esac
    key="${line%%=*}"
    val="${line#*=}"
    val=$(printf '%s' "$val" | sed "s/'/'\\\\''/g")
    printf "export %s='%s'\n" "$key" "$val" >> "${APP_DIR}/env.sh"
  done < "${APP_DIR}/.env"
  chmod 600 "${APP_DIR}/env.sh"
  say "已生成 env.sh（run.sh 会自动 source）"
fi

# ---------- 启动脚本 ----------
cat > "${APP_DIR}/run.sh" <<'RUNEOF'
#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
cd "$(dirname "$0")"

# 关键：把凭据喂给进程，否则网关会回退到 mock 上游（假直链 -> 播放器「换源失败」）
if [ -f ./env.sh ]; then
  . ./env.sh
fi

export BIND_ADDR="${BIND_ADDR:-127.0.0.1:8080}"

# 启动前把关键环境变量打出来（凭据只显示长度），排查「为什么全是 mock」一眼就能看到
if [ -f ./env.sh ]; then
  printf '\033[36m[run]\033[0m env.sh 已加载\n'
else
  printf '\033[33m[run]\033[0m 没有 env.sh（凭据未加载）\n'
fi
printf '\033[36m[run]\033[0m env: ENABLE_QQ=%s ENABLE_NETEASE=%s TX_UIN=%s TX_COOKIE=%s NETEASE_COOKIE=%s\n' \
  "${ENABLE_QQ:-unset}" "${ENABLE_NETEASE:-unset}" "${TX_UIN:-unset}" \
  "${TX_COOKIE:+set(${#TX_COOKIE}chars)}" "${NETEASE_COOKIE:+set}"
if [ -z "${ENABLE_QQ:-}${ENABLE_NETEASE:-}${UPSTREAM_A_URL:-}" ]; then
  printf '\033[33m[run]\033[0m 警告：没有检测到真实上游（ENABLE_QQ / ENABLE_NETEASE / UPSTREAM_A_URL 都为空）\n'
  printf '\033[33m[run]\033[0m       现在只会返回 mock 假直链，播放器一定「换源失败」\n'
fi

# 没有 redis 就不用设 REDIS_URL：启动探测一次失败后 L2 会永久关闭，请求路径零等待
exec ./audio-parse-gateway
RUNEOF
chmod +x "${APP_DIR}/run.sh"

# ---------- 开机自启（可选） ----------
if [ -d "${PREFIX}/etc" ]; then
  BOOT_DIR="${HOME}/.termux/boot"
  mkdir -p "$BOOT_DIR"
  cat > "${BOOT_DIR}/start-audio-gateway" <<'BOOTEOF'
#!/data/data/com.termux/files/usr/bin/sh
termux-wake-lock
cd "$HOME/audio-parse-gateway" || exit 1
if [ -f ./env.sh ]; then . ./env.sh; fi
export BIND_ADDR="${BIND_ADDR:-127.0.0.1:8080}"
nohup ./audio-parse-gateway >> ./gateway.log 2>&1 &
BOOTEOF
  chmod +x "${BOOT_DIR}/start-audio-gateway"
  say "已安装开机自启脚本（需要 termux-boot 才生效）：${BOOT_DIR}/start-audio-gateway"
fi

# ---------- 自检：启动 2 秒后被杀 = 能跑 ----------
say "验证二进制能不能跑…"
set +e
timeout 2 "$BIN" > "${APP_DIR}/boot.log" 2>&1
rc=$?
set -e
if [ "$rc" -eq 124 ]; then
  say "二进制可执行 ✅（跑满 2 秒被 timeout 正常结束）"
else
  warn "二进制启动异常（exit=${rc}），前 10 行输出："
  head -10 "${APP_DIR}/boot.log" || true
fi

cat <<EOF

========================================
 安装完成
 启动： bash ${APP_DIR}/run.sh
 测试： curl 'http://127.0.0.1:8080/health'
        curl 'http://127.0.0.1:8080/api/parse?id=002lzYCa14usQ5&source=tx&quality=320k'
 日志： 每请求一行（08:53:38 | [200] tx ... | 780ms）
 停止： Ctrl+C 或 pkill -f audio-parse-gateway
========================================
EOF
