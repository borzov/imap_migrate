#!/bin/bash
# ===========================================================================
# run.sh — Запуск IMAP Migration Tool (macOS / Linux)
# ===========================================================================
#
# Использование:
#   chmod +x run.sh
#   ./run.sh              — создать venv и установить зависимости
#   ./run.sh run           — запустить миграцию
#   ./run.sh dry-run       — пробный прогон
#   ./run.sh list-folders  — показать папки
#   ./run.sh verify        — перенос + верификация
#   ./run.sh clean         — удалить venv
#
# ===========================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"
PYTHON="$VENV_DIR/bin/python"
CONFIG="${IMAP_CONFIG:-$SCRIPT_DIR/config.yaml}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ---------------------------------------------------------------------------
# Проверка Python 3.10+
# ---------------------------------------------------------------------------
check_python() {
    local py=""

    # Ищем Python в порядке приоритета
    for candidate in python3.12 python3.11 python3.10 python3 python; do
        if command -v "$candidate" &>/dev/null; then
            local ver
            ver=$("$candidate" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || true)
            local major minor
            major=$(echo "$ver" | cut -d. -f1)
            minor=$(echo "$ver" | cut -d. -f2)
            if [[ "$major" -ge 3 && "$minor" -ge 10 ]]; then
                py="$candidate"
                break
            fi
        fi
    done

    if [[ -z "$py" ]]; then
        error "Python 3.10+ не найден."
        echo ""
        echo "  Установите через Homebrew:"
        echo "    brew install python@3.12"
        echo ""
        echo "  Или скачайте с https://www.python.org/downloads/"
        exit 1
    fi

    echo "$py"
}

# ---------------------------------------------------------------------------
# Создание venv
# ---------------------------------------------------------------------------
setup_venv() {
    if [[ -d "$VENV_DIR" ]]; then
        ok "Virtual environment уже существует: $VENV_DIR"
        return
    fi

    info "Ищу Python 3.10+..."
    local py
    py=$(check_python)
    local ver
    ver=$("$py" --version 2>&1)
    ok "Найден: $ver ($py)"

    info "Создаю virtual environment..."
    "$py" -m venv "$VENV_DIR"
    ok "venv создан: $VENV_DIR"

    info "Устанавливаю зависимости..."
    "$VENV_DIR/bin/pip" install --upgrade pip --quiet
    "$VENV_DIR/bin/pip" install -r "$SCRIPT_DIR/requirements.txt" --quiet
    ok "Зависимости установлены (pyyaml, tqdm)"

    echo ""
    ok "Готово! Теперь:"
    echo "  1. Отредактируйте config.yaml"
    echo "  2. Запустите: ./run.sh list-folders"
    echo "  3. Затем:     ./run.sh dry-run"
    echo "  4. И наконец: ./run.sh run"
}

# ---------------------------------------------------------------------------
# Проверка конфига
# ---------------------------------------------------------------------------
check_config() {
    if [[ ! -f "$CONFIG" ]]; then
        error "Файл конфигурации не найден: $CONFIG"
        echo ""
        echo "  Создайте конфиг из примера и отредактируйте:"
        echo "    cp config.yaml.example config.yaml"
        echo "    nano config.yaml"
        echo "  Либо укажите свой файл: IMAP_CONFIG=config.yaml ./run.sh run"
        exit 1
    fi

    # Проверяем, что конфиг заполнен (не содержит placeholder)
    if grep -q "your-email@example.com\|your-app-password\|mail.yourdomain.com\|your-password-here" "$CONFIG" 2>/dev/null; then
        warn "config.yaml содержит незаполненные значения (placeholder)."
        echo "  Отредактируйте $CONFIG перед запуском."
        echo ""
    fi
}

# ---------------------------------------------------------------------------
# Команды
# ---------------------------------------------------------------------------
cmd_setup() {
    setup_venv
}

cmd_run() {
    check_config
    info "Запуск миграции..."
    "$PYTHON" "$SCRIPT_DIR/imap_migrate.py" --config "$CONFIG" "$@"
}

cmd_dry_run() {
    check_config
    info "Пробный прогон (dry-run)..."
    "$PYTHON" "$SCRIPT_DIR/imap_migrate.py" --config "$CONFIG" --dry-run "$@"
}

cmd_list_folders() {
    check_config
    "$PYTHON" "$SCRIPT_DIR/imap_migrate.py" --config "$CONFIG" --list-folders
}

cmd_verify() {
    check_config
    info "Запуск миграции с верификацией..."
    "$PYTHON" "$SCRIPT_DIR/imap_migrate.py" --config "$CONFIG" --verify "$@"
}

cmd_clean() {
    if [[ -d "$VENV_DIR" ]]; then
        info "Удаляю venv..."
        rm -rf "$VENV_DIR"
        ok "venv удалён"
    else
        warn "venv не найден"
    fi
}

cmd_help() {
    echo ""
    echo -e "${BOLD}IMAP Migration — run.sh${NC}"
    echo ""
    echo "Использование:"
    echo "  ./run.sh              Создать venv и установить зависимости"
    echo "  ./run.sh run          Запустить миграцию"
    echo "  ./run.sh dry-run      Пробный прогон без записи"
    echo "  ./run.sh list-folders Показать папки на источнике"
    echo "  ./run.sh verify       Перенос + верификация"
    echo "  ./run.sh clean        Удалить venv"
    echo ""
    echo "Переменные окружения:"
    echo "  IMAP_CONFIG=path/to/config.yaml  Путь к конфигу (по умолчанию: config.yaml)"
    echo ""
    echo "Дополнительные аргументы передаются скрипту напрямую:"
    echo "  ./run.sh run --folders INBOX,Sent --verbose"
    echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
case "${1:-}" in
    run)          shift; setup_venv; cmd_run "$@" ;;
    dry-run)      shift; setup_venv; cmd_dry_run "$@" ;;
    list-folders) shift; setup_venv; cmd_list_folders "$@" ;;
    verify)       shift; setup_venv; cmd_verify "$@" ;;
    clean)        cmd_clean ;;
    help|--help|-h) cmd_help ;;
    "")           setup_venv ;;
    *)            error "Неизвестная команда: $1"; cmd_help; exit 1 ;;
esac
