#!/data/data/com.termux/files/usr/bin/bash
# audio-parse-gateway · Termux 一键安装
#
# 用法：在 Termux 里执行
#   bash "$HOME/install.sh"
#   或：bash /sdcard/dsh-deploy/install.sh
set -euo pipefail

APP_DIR="${HOME}/audio-parse-gateway"
BIN="${APP_DIR}/audio-parse-gateway"
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"

say() { printf '\033[36m[termux]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

[ -n "${PREFIX:-}" ] || die "请用 Termux 自带的 bash 运行（$PREFIX 未设置）"

say "创建目录 ${APP_DIR}"
mkdir -p "$APP_DIR"

if [ -x "${SELF_DIR}/audio-parse-gateway" ]; then
  say "使用安装包自带的二进制"
  cp "${SELF_DIR}/audio-parse-gateway" "$BIN"
else
  # 从 Release 拉 arm64-android 产物（需要 curl；没有就告诉用户装）
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
say "二进制：$(ls -lh "$BIN" | awk '{print $5}')"

# 凭据：从之前放置的 env 文件抄过去（只在存在时才拷，避免把空密钥写进去）
if [ -f "${SELF_DIR}/env" ]; then
  cp "${SELF_DIR}/env" "${APP_DIR}/.env"
  chmod 600 "${APP_DIR}/.env"
  say "已写入 .env（qq/netease 凭据）"
  # 如果安装包是从共享存储（/sdcard）拷来的，装完立刻把凭据副本删掉，
  # 免得别的 App 也能读到 cookie
  case "${SELF_DIR}" in
    /sdcard/*|/storage/emulated/0/*)
      rm -f "${SELF_DIR}/env"
      say "已删除共享存储里的凭据副本（${SELF_DIR}/env）"
      ;;
  esac
fi

# 启动脚本
cat > "${APP_DIR}/run.sh" <<'RUNEOF'
#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
export BIND_ADDR="${BIND_ADDR:-127.0.0.1:8080}"
# 没有 redis 时不设 REDIS_URL，网关启动探测一次就会关掉 L2，请求路径零等待
# RUST_LOG=info,audio_parse_gateway=debug   # 需要排障时打开
exec ./audio-parse-gateway
RUNEOF
chmod +x "${APP_DIR}/run.sh"

# dotenv 解析：把 KEY=VALUE 变成 export KEY=VALUE（供 loginctl/termux-services 用）
if [ -f "${APP_DIR}/.env" ]; then
  while IFS= read -r line; do
    case "$line" in ''|'#'*) continue ;; esac
    case "$line" in *=*) printf 'export %s\n' "${line%%=*}=$(printf '%s' "${line#*=}" | sed "s/^'//;s/'$//")" ;; esac
  done < "${APP_DIR}/.env" > "${APP_DIR}/env.sh"
  chmod 600 "${APP_DIR}/env.sh"
fi

# 开机自启（可选）：有 termux-boot 就装一个开机脚本
if [ -d "${PREFIX}/etc/termux-boot" ] || command -v termux-boot >/dev/null 2>&1; then
  BOOT_DIR="${HOME}/.termux/boot"
  mkdir -p "$BOOT_DIR"
  cat > "${BOOT_DIR}/start-audio-gateway" <<'BOOTEOF'
#!/data/data/com.termux/files/usr/bin/sh
termux-wake-lock
cd "$HOME/audio-parse-gateway"
if [ -f ./env.sh ]; then . ./env.sh; fi
nohup ./audio-parse-gateway >> ./gateway.log 2>&1 &
BOOTEOF
  chmod +x "${BOOT_DIR}/start-audio-gateway"
  say "已安装开机自启脚本：${BOOT_DIR}/start-audio-gateway"
fi

# 验证：启动 2 秒后被 timeout 杀掉 = 二进制能跑（它不支持 --version，启动即进监听）
say "验证二进制能不能跑…"
set +e
timeout 2 "$BIN" > "${APP_DIR}/boot.log" 2>&1
rc=$?
set -e
if [ "$rc" -eq 124 ]; then
  say "二进制可执行 ✅（跑满 2 秒，被 timeout 正常结束）"
else
  say "二进制启动异常（exit=${rc}），输出如下："
  head -10 "${APP_DIR}/boot.log" || true
fi

cat <<EOF

========================================
 安装完成
 启动： bash ${APP_DIR}/run.sh
 测试： curl 'http://127.0.0.1:8080/health'
        curl 'http://127.0.0.1:8080/api/parse?id=002lzYCa14usQ5&source=tx&quality=320k'
 日志： 只有一行访问日志（08:53:38 | [200] ...）
 停止：  Ctrl+C 或 pkill -f audio-parse-gateway
========================================
EOF
