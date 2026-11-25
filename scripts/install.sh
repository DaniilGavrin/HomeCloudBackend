#!/usr/bin/env bash
set -Eeuo pipefail

OWNER="DaniilGavrin"
REPO="HomeCloudBackend"
# Bootstrap from an immutable, reviewed commit rather than a mutable branch.
CLI_REV="8445e4ccab104d0420c41a8ee55a929160ca8f95"
DEST="/usr/local/bin/homecloud"
SOURCE="https://raw.githubusercontent.com/$OWNER/$REPO/$CLI_REV/scripts/homecloud"

if [[ "$EUID" -ne 0 ]]; then
  echo "Запусти установщик от root: sudo bash install.sh" >&2
  exit 1
fi
command -v curl >/dev/null 2>&1 || { echo "Требуется curl." >&2; exit 1; }
command -v install >/dev/null 2>&1 || { echo "Требуется install." >&2; exit 1; }
command -v bash >/dev/null 2>&1 || { echo "Требуется bash." >&2; exit 1; }

tmp="$(mktemp)"
backup=""
cleanup() {
  rm -f "$tmp"
  [[ -z "$backup" ]] || rm -f "$backup"
}
trap cleanup EXIT

echo "[homecloud] Загружаю CLI из commit $CLI_REV..."
curl --fail --silent --show-error --location --retry 2 --retry-delay 1 "$SOURCE" -o "$tmp"
bash -n "$tmp"
head -n 1 "$tmp" | grep -q '^#!/usr/bin/env bash$' || {
  echo "[homecloud] Полученный файл не похож на Bash CLI; установка отменена." >&2
  exit 1
}
grep -q 'HomeCloud local updater' "$tmp" || {
  echo "[homecloud] Не удалось подтвердить содержимое CLI; установка отменена." >&2
  exit 1
}

if [[ -e "$DEST" ]]; then
  backup="$(mktemp)"
  cp -p -- "$DEST" "$backup"
fi

install -D -o root -g root -m 0755 "$tmp" "$DEST"
if ! "$DEST" --help >/dev/null; then
  echo "[homecloud] Проверка установленного CLI не пройдена; восстанавливаю предыдущий файл." >&2
  if [[ -n "$backup" ]]; then
    install -o root -g root -m 0755 "$backup" "$DEST"
  else
    rm -f -- "$DEST"
  fi
  exit 1
fi

echo "[homecloud] CLI установлен: $DEST"
echo "[homecloud] Команды: homecloud update, homecloud status, homecloud version"

if [[ -f "${BASH_SOURCE[0]}" && "${BASH_SOURCE[0]}" != /dev/stdin ]]; then
  rm -f -- "${BASH_SOURCE[0]}"
  echo "[homecloud] Установщик удалён."
fi
