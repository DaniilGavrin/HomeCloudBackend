#!/usr/bin/env bash
set -Eeuo pipefail

OWNER="DaniilGavrin"
REPO="HomeCloudBackend"
BRANCH="main"
DEST="/usr/local/bin/homecloud"
SOURCE="https://raw.githubusercontent.com/$OWNER/$REPO/$BRANCH/scripts/homecloud"

if [[ "$EUID" -ne 0 ]]; then
  echo "Запусти установщик от root: sudo bash install.sh" >&2
  exit 1
fi
command -v curl >/dev/null 2>&1 || { echo "Требуется curl." >&2; exit 1; }
command -v install >/dev/null 2>&1 || { echo "Требуется install." >&2; exit 1; }

tmp="$(mktemp)"
cleanup() { rm -f "$tmp"; }
trap cleanup EXIT

echo "[homecloud] Загружаю CLI из публичного репозитория..."
curl --fail --silent --show-error --location --retry 2 "$SOURCE" -o "$tmp"
head -n 1 "$tmp" | grep -q '^#!/usr/bin/env bash$' || {
  echo "[homecloud] Полученный файл не похож на Bash CLI; установка отменена." >&2
  exit 1
}
grep -q 'HomeCloud local updater' "$tmp" || {
  echo "[homecloud] Не удалось подтвердить содержимое CLI; установка отменена." >&2
  exit 1
}

install -D -o root -g root -m 0755 "$tmp" "$DEST"
"$DEST" --help >/dev/null
echo "[homecloud] CLI установлен: $DEST"
echo "[homecloud] Команды: homecloud update, homecloud status, homecloud version"

# Self-delete only when this installer was executed as a regular file.
if [[ -f "${BASH_SOURCE[0]}" && "${BASH_SOURCE[0]}" != /dev/stdin ]]; then
  rm -f -- "${BASH_SOURCE[0]}"
  echo "[homecloud] Установщик удалён."
fi
