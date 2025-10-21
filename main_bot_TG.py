#!/usr/bin/env python3
# telegram_payroll_hosting_sqlite_fixed.py
# Упрощённая версия бота с SQLite (персональные файлы для каждой строки vk_id).
# Добавлена команда/кнопка для рассылки уведомлений кураторам (VK) —
# берутся уникальные vk_id из vedomosti_users и отправляется сообщение через VK API.

import os
import re
import time
import json
import logging
import shutil
import threading
import sqlite3
import random
import string
import uuid
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd

from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
from telegram import Update
from telegram.ext import filters
# try load config.py if exists
try:
    import config
except Exception:
    config = None

TELEGRAM_TOKEN = getattr(config, 'TELEGRAM_TOKEN', os.environ.get('TELEGRAM_TOKEN'))
SERVICE_ACCOUNT_FILE = getattr(config, 'SERVICE_ACCOUNT_FILE', os.environ.get('SERVICE_ACCOUNT_FILE', 'isu_groups.json'))
UPLOADS_DIR = getattr(config, 'UPLOADS_DIR', os.environ.get('UPLOADS_DIR', 'uploads'))
DRY_RUN = getattr(config, 'DRY_RUN', os.environ.get('DRY_RUN', 'False') in ('True', 'true', '1'))

HOSTING_ROOT = getattr(config, 'HOSTING_ROOT', os.environ.get('HOSTING_ROOT', 'hosting'))
OPEN_DIRNAME = getattr(config, 'OPEN_DIRNAME', os.environ.get('OPEN_DIRNAME', 'open'))
ARCHIVE_DIRNAME = getattr(config, 'ARCHIVE_DIRNAME', os.environ.get('ARCHIVE_DIRNAME', 'archive'))
HOSTING_INDEX = getattr(config, 'HOSTING_INDEX', os.environ.get('HOSTING_INDEX', 'hosting_index.json'))
# keep archive config variables for compatibility but archiver not started
ARCHIVE_DELAY_HOURS = float(getattr(config, 'ARCHIVE_DELAY_HOURS', os.environ.get('ARCHIVE_DELAY_HOURS', 24.0)))
ARCHIVE_CHECK_INTERVAL = float(getattr(config, 'ARCHIVE_CHECK_INTERVAL', os.environ.get('ARCHIVE_CHECK_INTERVAL', 60.0)))

CURRENT_SHEETS_FILE = getattr(config, 'CURRENT_SHEETS_FILE', 'current_sheets.json')
ALLOWED_EXCEL_EXT = {'xlsx', 'xls', 'csv'}
DB_PATH = getattr(config, 'DB_PATH', os.environ.get('DB_PATH', 'hosting.db'))

# VK-related config (must be provided in config.py or env)
VK_TOKEN = getattr(config, 'VK_TOKEN', os.environ.get('VK_TOKEN', None))
GROUP_ID = getattr(config, 'GROUP_ID', os.environ.get('GROUP_ID', None))
# Optional: default notification text
NOTIFY_TEXT = getattr(config, 'NOTIFY_TEXT', os.environ.get('NOTIFY_TEXT', 'Пожалуйста, проверьте новую ведомость — она опубликована на хостинге.'))

# admin ids: from config.ADMIN_IDS or env ADMIN_IDS comma-separated
_raw_admins = getattr(config, 'ADMIN_IDS', None) or os.environ.get('ADMIN_IDS', '')
ADMIN_IDS = set()
if _raw_admins:
    try:
        ADMIN_IDS = set(int(x.strip()) for x in str(_raw_admins).split(',') if x.strip())
    except Exception:
        ADMIN_IDS = set()

# persistent admins/users files (can be overridden via config or env)
ADMINS_FILE = getattr(config, 'ADMINS_FILE', os.environ.get('ADMINS_FILE', 'admins.json'))
USERS_FILE = getattr(config, 'USERS_FILE', os.environ.get('USERS_FILE', 'users.json'))

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger('tg_payroll_hosting')

# ----------------- simple JSON helpers -----------------

def load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        log.exception('Failed to load json %s', path)
        return {}

def save_json(path: str, data):
    try:
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        log.exception('Failed to save json %s', path)


# ----------------- sqlite: vedomosti users (simplified schema) -----------------

def init_db():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        
        # Включаем WAL режим для лучшей конкурентности
        c.execute('PRAGMA journal_mode=WAL')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS vedomosti_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vk_id TEXT,
                personal_path TEXT,
                original_filename TEXT,
                state TEXT DEFAULT ''
            )
        ''')
        conn.commit()
        conn.close()
        log.info('SQLite initialized with WAL mode (%s)', DB_PATH)
    except Exception:
        log.exception('Failed to initialize sqlite DB')


def ensure_vedomosti_status_columns():
    """Ensure columns status, disagree_reason, confirmed_at, created_at, archive_at exist in vedomosti_users."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        c.execute("PRAGMA table_info(vedomosti_users)")
        cols = [r[1] for r in c.fetchall()]
        if 'status' not in cols:
            c.execute("ALTER TABLE vedomosti_users ADD COLUMN status TEXT DEFAULT ''")
        if 'disagree_reason' not in cols:
            c.execute("ALTER TABLE vedomosti_users ADD COLUMN disagree_reason TEXT DEFAULT ''")
        if 'confirmed_at' not in cols:
            c.execute("ALTER TABLE vedomosti_users ADD COLUMN confirmed_at INTEGER")
        if 'created_at' not in cols:
            c.execute("ALTER TABLE vedomosti_users ADD COLUMN created_at INTEGER DEFAULT 0")
        if 'archive_at' not in cols:
            c.execute("ALTER TABLE vedomosti_users ADD COLUMN archive_at INTEGER DEFAULT 0")
        # Track archive warning dispatch
        if 'warning_sent' not in cols:
            c.execute("ALTER TABLE vedomosti_users ADD COLUMN warning_sent INTEGER DEFAULT 0")
        if 'warning_sent_at' not in cols:
            c.execute("ALTER TABLE vedomosti_users ADD COLUMN warning_sent_at INTEGER DEFAULT 0")
        conn.commit()
        conn.close()
        log.info('SQLite columns ensured: status/disagree_reason/confirmed_at/created_at/archive_at/warning_sent')
    except Exception:
        log.exception('Failed to ensure vedomosti status columns')

def insert_vedomosti_user(vk_id: str, personal_path: str, original_filename: str, state: str = ''):
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        now = int(time.time())
        archive_time = now + (36 * 3600)  # 36 часов в секундах
        c.execute('INSERT INTO vedomosti_users(vk_id, personal_path, original_filename, state, created_at, archive_at) VALUES (?,?,?,?,?,?)',
                  (str(vk_id), personal_path, original_filename, state, now, archive_time))
        conn.commit()
        conn.close()
        log.info('Inserted vedomosti user %s with archive time %s', vk_id, archive_time)
    except Exception:
        log.exception('Failed to insert vedomosti user %s', vk_id)


# ----------------- hosting index (optional, compatibility) -----------------
# NOTE: we keep load/save but do not actively use them (can be removed)
def load_hosting_index() -> dict:
    return load_json(HOSTING_INDEX) or {}

def save_hosting_index(idx: dict):
    try:
        save_json(HOSTING_INDEX, idx)
    except Exception:
        log.exception('Failed to persist hosting index %s', HOSTING_INDEX)


# ----------------- users/admins persistence -----------------

def load_admins_from_file() -> set:
    global ADMIN_IDS
    if os.path.exists(ADMINS_FILE):
        try:
            data = load_json(ADMINS_FILE)
            if isinstance(data, list):
                loaded = set(int(x) for x in data)
                ADMIN_IDS = loaded
                log.info('Loaded admins from %s: %s', ADMINS_FILE, list(ADMIN_IDS))
                return ADMIN_IDS
        except Exception:
            log.exception('Failed to load admins file %s', ADMINS_FILE)
    return ADMIN_IDS

def save_admins_to_file():
    try:
        save_json(ADMINS_FILE, sorted(list(int(x) for x in ADMIN_IDS)))
        log.info('Saved admins to %s: %s', ADMINS_FILE, list(ADMIN_IDS))
    except Exception:
        log.exception('Failed to save admins to %s', ADMINS_FILE)

def load_users() -> dict:
    return load_json(USERS_FILE) or {}

def save_users(users: dict):
    try:
        save_json(USERS_FILE, users)
        log.info('Saved users to %s', USERS_FILE)
    except Exception:
        log.exception('Failed to save users file %s', USERS_FILE)

def save_user_entry(user):
    try:
        users = load_users()
        uid = str(user.id)
        username = (user.username or '').strip()
        if username:
            users[username] = int(uid)
            users[uid] = username
        else:
            users[uid] = users.get(uid, '')
        save_users(users)
        log.info('Saved user entry: id=%s username=%s', uid, username)
    except Exception:
        log.exception('Failed to save user entry for %s', getattr(user, 'id', None))

# initialize admins set from file if present
ADMIN_IDS = set(load_admins_from_file())


# ----------------- current state per user -----------------

def save_current_for_user(user_id: int, file_path: str = '', awaiting_meta: bool = False):
    d = load_json(CURRENT_SHEETS_FILE) or {}
    d[str(user_id)] = {'file_path': file_path or '', 'awaiting_meta': bool(awaiting_meta)}
    save_json(CURRENT_SHEETS_FILE, d)
    log.info('Saved current for %s -> file=%s awaiting_meta=%s', user_id, file_path, awaiting_meta)

def load_current_for_user(user_id: int) -> dict:
    d = load_json(CURRENT_SHEETS_FILE) or {}
    return d.get(str(user_id), {})


# ----------------- file conversion & publishing -----------------

def ensure_csv(path: str) -> Optional[str]:
    if not path:
        return None
    lower = path.lower()
    if lower.endswith('.csv'):
        return path
    try:
        df = pd.read_excel(path)
    except Exception as e:
        log.exception('Failed to read Excel %s: %s', path, e)
        return None
    csv_name = os.path.splitext(path)[0] + '.csv'
    try:
        df.to_csv(csv_name, index=False, encoding='utf-8')
        log.info('Converted %s -> %s', path, csv_name)
        return csv_name
    except Exception as e:
        log.exception('Failed to write CSV %s: %s', csv_name, e)
        return None


def publish_to_hosting(csv_path: str, subject: str, course_type: str, block: str, uploaded_by: int) -> Optional[str]:
    if not os.path.exists(csv_path):
        log.warning('publish: file not found %s', csv_path)
        return None

    subject_safe = subject if subject else 'unknown_subject'
    course_safe = course_type if course_type else 'unknown_course'
    block_safe = block if block else 'unknown_block'

    dest_dir = os.path.join(HOSTING_ROOT, OPEN_DIRNAME, subject_safe, course_safe, block_safe)
    try:
        os.makedirs(dest_dir, exist_ok=True)
    except Exception:
        log.exception('Failed to create dest dir %s', dest_dir)
        return None

    fname = f"{subject_safe}_{course_safe}_{block_safe}.csv"  # Новое имя файла
    dest_path = os.path.join(dest_dir, fname)

    try:
        shutil.move(csv_path, dest_path)
    except Exception as e:
        log.exception('Failed to move file to hosting: %s', e)
        return None

    log.info('Published to hosting: %s (subject=%s course_type=%s block=%s)', dest_path, subject, course_type, block)

    # импортируем пользователей и создаём персональные файлы
    try:
        import_users_from_csv(dest_path, original_filename=os.path.basename(dest_path))
    except Exception:
        log.exception('Failed to import users from %s', dest_path)

    return dest_path


# ----------------- import users from CSV (reliable per-row files) -----------------

def _safe_filename_component(s: str) -> str:
    s = (s or '').strip()
    s = re.sub(r'[\\/:\x00-\x1f<>\"|?*]+', '_', s)
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'__+', '_', s)
    return s[:120] or 'unknown'

def import_users_from_csv(dest_path: str, original_filename: str):
    """Прочитать CSV и для каждой строки с колонкой vk_id создать индивидуальный файл и запись в sqlite."""
    if not os.path.exists(dest_path):
        log.warning('import: file not found %s', dest_path)
        return

    try:
        df = pd.read_csv(dest_path, dtype=str)
    except Exception:
        try:
            df = pd.read_csv(dest_path, encoding='cp1251', dtype=str)
        except Exception:
            log.exception('Failed to read CSV for import %s', dest_path)
            return

    # поиск колонки vk_id case-insensitive
    vk_col = None
    for col in df.columns:
        if col and str(col).strip().lower() == 'vk_id':
            vk_col = col
            break

    if not vk_col:
        log.info('CSV %s не содержит столбца vk_id — импорт пропущен', dest_path)
        return

    users_dir = os.path.join(os.path.dirname(dest_path), 'users')
    os.makedirs(users_dir, exist_ok=True)

    # --------------------------
    # Очищаем и сокращаем original_filename, чтобы не плодить технические префиксы в имени
    orig = os.path.basename(original_filename or '')
    orig = re.sub(r'^(?:\d+_){1,3}', '', orig)
    orig_noext = os.path.splitext(orig)[0]
    base_original = _safe_filename_component(orig_noext)[:50]
    # --------------------------

    timestamp = int(time.time())
    count = 0
    
    # Оптимизация: группируем операции с БД
    db_operations = []

    for idx, row in df.iterrows():
        try:
            vk_val = row.get(vk_col, '')
            if pd.isna(vk_val) or str(vk_val).strip() == '':
                continue
            vk_str = str(vk_val).strip()
            vk_safe = _safe_filename_component(vk_str)

            # Формируем читаемое и короткое имя:
            personal_name = f"{vk_safe}_{timestamp}_{idx}_{base_original}.csv"
            personal_path = os.path.join(users_dir, personal_name)

            # extract single-row dataframe preserving columns/headers
            try:
                one = df.loc[[idx]]
                one.to_csv(personal_path, index=False, encoding='utf-8')
            except Exception:
                try:
                    one.to_csv(personal_path, index=False, encoding='cp1251')
                except Exception:
                    log.exception('Failed to write personal file for %s', vk_str)
                    continue

            # Добавляем операцию в очередь вместо немедленного выполнения
            db_operations.append((vk_str, personal_path, original_filename))
            count += 1
            log.info('Created personal file for vk_id=%s -> %s', vk_str, personal_path)
        except Exception:
            log.exception('Error processing row %s in %s', idx, dest_path)

    # Выполняем все операции с БД одним пакетом
    if db_operations:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30)
            c = conn.cursor()
            now = int(time.time())
            archive_time = now + (36 * 3600)  # 36 часов в секундах

            # Для КАЖДОЙ строки создаём уникальный идентификатор состояния, чтобы не
            # было одного общего payment_id для всех пользователей одной ведомости.
            for vk_str, personal_path, original_filename in db_operations:
                unique_state = f"imported:{uuid.uuid4()}"
                log.info('Creating unique state for vk_id=%s: %s', vk_str, unique_state)
                c.execute(
                    'INSERT INTO vedomosti_users(vk_id, personal_path, original_filename, state, created_at, archive_at) VALUES (?,?,?,?,?,?)',
                    (str(vk_str), personal_path, original_filename, unique_state, now, archive_time)
                )

            conn.commit()
            conn.close()
            log.info('Bulk inserted %d vedomosti users to database with unique states', len(db_operations))
        except Exception:
            log.exception('Failed to bulk insert vedomosti users')

    log.info('Imported %s users from %s (vk_col=%s)', count, dest_path, vk_col)


# ----------------- helpers -----------------

def is_admin(user_id: int) -> bool:
    try:
        return int(user_id) in ADMIN_IDS
    except Exception:
        return False

def _rand_letters(n: int = 6) -> str:
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(n))

# ----------------- VK messaging -----------------

def send_vk_message(user_vk_id: str, text: str, keyboard_json: Optional[str] = None, max_retries: int = 3) -> bool:
    """
    Отправляет сообщение user_vk_id через VK API (community token) с повторными попытками.
    keyboard_json — JSON строка клавиатуры (как возвращает chat_bottom_keyboard()), если None — клавиатура не прикрепляется.
    """
    if not VK_TOKEN or not GROUP_ID:
        log.error('VK_TOKEN or GROUP_ID not configured; cannot send VK messages.')
        return False

    try:
        uid = int(str(user_vk_id).strip())
    except Exception:
        log.warning('Invalid vk id (not numeric): %s', user_vk_id)
        return False

    url = 'https://api.vk.com/method/messages.send'
    version = '5.131'

    for attempt in range(max_retries):
        try:
            random_id = int(time.time() * 1000) & 0x7fffffff
            params = {
                'access_token': VK_TOKEN,
                'v': version,
                'user_id': uid,
                'message': text,
                'random_id': random_id,
                'group_id': GROUP_ID
            }
            if keyboard_json:
                # keyboard_json уже должна быть строкой (json.dumps(..., ensure_ascii=False))
                params['keyboard'] = keyboard_json

            resp = requests.post(url, data=params, timeout=10)
            try:
                j = resp.json()
            except Exception:
                j = {}

            if resp.status_code == 200 and 'error' not in j:
                return True

            if 'error' in j:
                error_code = j['error'].get('error_code', 0)
                if error_code == 6:  # Rate limit
                    wait_time = 2 ** attempt
                    log.warning('Rate limit hit for user %s, waiting %s seconds (attempt %d)', user_vk_id, wait_time, attempt + 1)
                    time.sleep(wait_time)
                    continue
                elif error_code in [7, 9]:
                    log.warning('Permission denied or flood control for user %s: %s', user_vk_id, j['error'])
                    return False
                else:
                    log.warning('VK API error for user %s: %s', user_vk_id, j['error'])
                    if attempt == max_retries - 1:
                        return False
                    time.sleep(1)
            else:
                log.warning('HTTP error for user %s: status %s', user_vk_id, resp.status_code)
                if attempt == max_retries - 1:
                    return False
                time.sleep(1)

        except requests.exceptions.Timeout:
            log.warning('Timeout sending message to user %s (attempt %d)', user_vk_id, attempt + 1)
            if attempt == max_retries - 1:
                return False
            time.sleep(1)
        except Exception as e:
            log.exception('Exception sending VK message to %s (attempt %d): %s', user_vk_id, attempt + 1, e)
            if attempt == max_retries - 1:
                return False
            time.sleep(1)

    return False

# ----------------- Telegram handlers (minor changes) -----------------

def start(update: Update, context: CallbackContext):
    user = update.message.from_user
    uid = int(user.id)
    save_user_entry(user)
    if not ADMIN_IDS:
        ADMIN_IDS.add(int(uid))
        try:
            save_admins_to_file()
        except Exception:
            log.exception('Failed to persist admin after auto-adding creator')
        update.message.reply_text('Ты автоматически зарегистрирован(а) как админ (поскольку список админов был пуст).')
        log.info('Auto-added admin: %s (%s)', uid, user.username)

    update.message.reply_text(
        'Привет! Это бот для отправки файлов на хостинг.\n\n'
        'Сценарий работы:\n\n'
        '1) Пришли файл XLSX как вложение.\n'
        '2) Отправь команду:\n'
        '/send <предмет> <тип курса> <блок>\n'
        'Пример: /send Русский ОГЭ ПГК\n'
        '3) После публикации ведомости можно разослать уведомления участникам по vk_id командой /notify <название ведомости>.\n'
        'Пример: /notify Русский ОГЭ ПГК\n\n'
        'Команды для админов:\n'
        '/notify <название ведомости> — рассылка уведомлений пользователям конкретной ведомости\n'
        '/liststatements — показать список всех открытых и архивных ведомостей\n'
        '/archive <название ведомости> - переместить ведомость в архив сразу\n'
        '/delete <название ведомости> - удалить ведомость (и из архива тоже)\n'
    )

def description(update: Update, context: CallbackContext):
    update.message.reply_text(
        'Привет! Это бот для отправки файлов на хостинг.\n\n'
        'Сценарий работы:\n\n'
        '1) Пришли файл XLSX как вложение.\n'
        '2) Отправь команду:\n'
        '/send <предмет> <тип курса> <блок>\n'
        'Пример: /send Русский ОГЭ ПГК\n'
        '3) После публикации ведомости можно разослать уведомления участникам по vk_id командой /notify <название ведомости>.\n'
        'Пример: /notify Русский ОГЭ ПГК\n\n'
        'Команды для админов:\n'
        '/notify <название ведомости> — рассылка уведомлений пользователям конкретной ведомости\n'
        '/liststatements — показать список всех открытых и архивных ведомостей\n'
        '/archive <название ведомости> - переместить ведомость в архив сразу\n'
        '/delete <название ведомости> - удалить ведомость (и из архива тоже)\n'
    )

def handle_document(update: Update, context: CallbackContext):
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring document from non-admin %s', from_id)
        msg.reply_text('Только админы могут загружать файлы.')
        return

    doc = msg.document
    if not doc:
        return
    filename = doc.file_name or f'document_{doc.file_id}'
    ext = os.path.splitext(filename)[1].lstrip('.').lower()
    if ext not in ALLOWED_EXCEL_EXT:
        msg.reply_text(f'Неподдерживаемое расширение .{ext}. Поддерживаемые: .xlsx, .xls, .csv')
        return

    os.makedirs(UPLOADS_DIR, exist_ok=True)
    safe_title = (filename)
    local_name = safe_title
    local_path = os.path.join(UPLOADS_DIR, local_name)

    try:
        file_obj = context.bot.get_file(doc.file_id)
        file_obj.download(custom_path=local_path)
        log.info('Downloaded file to %s', local_path)
    except Exception as e:
        log.exception('Failed to download file: %s', e)
        msg.reply_text('Не удалось загрузить файл. Попробуйте ещё раз.')
        return

    save_current_for_user(from_id, file_path=local_path, awaiting_meta=True)

    reply = ('Файл сохранён, ожидает публикации.\n'
             'Отправьте инфу командой: /send <предмет> <тип курса> <блок>\n'
             'Пример: /send Русский ОГЭ ПГК 1')
    if not DRY_RUN:
        msg.reply_text(reply)
    else:
        log.info('[DRY RUN] %s', reply)

# ----------------- notify handlers -----------------


def notify_command(update: Update, context: CallbackContext):
    user = update.message.from_user
    if not is_admin(user.id):
        update.message.reply_text('Только админы могут отправлять рассылки.')
        return

    # Получаем название ведомости, если оно указано
    if context.args:
        subject = ' '.join(context.args).strip()  # Объединяем все аргументы в одну строку
    else:
        update.message.reply_text('Не указано название ведомости.')
        return

    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        
        subject_normalized = subject.replace(' ', '_')
        
        c.execute('SELECT DISTINCT vk_id FROM vedomosti_users WHERE original_filename = ?', (subject_normalized + '.csv',))
        rows = c.fetchall()
        
        if not rows:
            flexible_pattern = subject.replace(' ', '[_ ]').replace('_', '[_ ]')
            c.execute('SELECT DISTINCT vk_id FROM vedomosti_users WHERE original_filename REGEXP ?', (flexible_pattern,))
            rows = c.fetchall()
            
            if not rows:
                c.execute('SELECT DISTINCT vk_id FROM vedomosti_users WHERE REPLACE(original_filename, "_", " ") LIKE ?', 
                         ('%' + subject.replace('_', ' ') + '%',))
                rows = c.fetchall()
        
        conn.close()
    except Exception:
        log.exception('Failed to read vedomosti_users for notify')
        update.message.reply_text('Ошибка при чтении БД для рассылки. Смотри логи.')
        return

    vk_ids = [r[0] for r in rows if r and str(r[0]).strip()]
    vk_ids = list(dict.fromkeys(vk_ids))  # unique preserving order

    total = len(vk_ids)
    
    if total == 0:
        update.message.reply_text(f'Не найдено пользователей для ведомости "{subject}".\nПроверьте правильность названия ведомости.')
        return
    
    # Сообщаем сколько пользователей найдено
    update.message.reply_text(f'Начинаю рассылку для ведомости "{subject}".\nНайдено пользователей: {total}')
    
    sent = 0
    failed = 0
    failed_list = []

    # Формируем текст. Используем NOTIFY_TEXT, если есть.
    text_plain = NOTIFY_TEXT or "У Вас появилась новая выплата на согласование. Нажмите на кнопку 'К списку выплат'. "
    # Если у тебя есть PAYMENTS_URL и хочешь добавить ссылку в текст, можно:
    try:
        PAYMENTS_URL  # может быть не объявлена в этом модуле
    except Exception:
        PAYMENTS_URL = None
    if PAYMENTS_URL:
        text_with_link_in_text = f"{text_plain}\n\nСписок: {PAYMENTS_URL}"
    else:
        text_with_link_in_text = text_plain

    # Формируем JSON клавиатуры (не-inline) идентичной chat_bottom_keyboard()
    def _chat_bottom_keyboard_json():
        kb = {
            "one_time": False,
            "inline": False,
            "buttons": [
                [
                    {
                        "action": {
                            "type": "text",
                            "payload": json.dumps({"cmd": "to_list"}, ensure_ascii=False),
                            "label": "К списку выплат"
                        },
                        "color": "primary"
                    }
                ]
            ]
        }
        return json.dumps(kb, ensure_ascii=False)

    keyboard_json = _chat_bottom_keyboard_json()

    # Отправка одному пользователю через VK API (с retry)
    def _send_vk_with_keyboard(vk_id: str, message: str, keyboard_json: str, max_retries: int = 3) -> bool:
        if not VK_TOKEN or not GROUP_ID:
            log.error('VK_TOKEN or GROUP_ID not configured; cannot send VK messages.')
            return False
        try:
            uid = int(str(vk_id).strip())
        except Exception:
            log.warning('Invalid vk id (not numeric): %s', vk_id)
            return False

        url = 'https://api.vk.com/method/messages.send'
        version = '5.131'

        for attempt in range(max_retries):
            try:
                params = {
                    'access_token': VK_TOKEN,
                    'v': version,
                    'user_id': uid,
                    'message': message,
                    'random_id': int(time.time() * 1000) & 0x7fffffff,
                    'group_id': GROUP_ID,
                    'keyboard': keyboard_json
                }
                resp = requests.post(url, data=params, timeout=10)
                try:
                    j = resp.json()
                except Exception:
                    j = {}

                if resp.status_code == 200 and 'error' not in j:
                    return True

                # обработка ошибок VK API
                if 'error' in j:
                    code = j['error'].get('error_code', 0)
                    if code == 6:
                        wait_time = 2 ** attempt
                        log.warning('VK rate limit for %s, sleeping %s s (attempt %d)', vk_id, wait_time, attempt + 1)
                        time.sleep(wait_time)
                        continue
                    elif code in [7, 9]:
                        log.warning('VK permission/flood for %s: %s', vk_id, j['error'])
                        return False
                    else:
                        log.warning('VK API error for %s: %s', vk_id, j['error'])
                        if attempt == max_retries - 1:
                            return False
                        time.sleep(1)
                else:
                    log.warning('HTTP error for %s: status %s', vk_id, resp.status_code)
                    if attempt == max_retries - 1:
                        return False
                    time.sleep(1)

            except requests.exceptions.Timeout:
                log.warning('Timeout sending message to %s (attempt %d)', vk_id, attempt + 1)
                if attempt == max_retries - 1:
                    return False
                time.sleep(1)
            except Exception:
                log.exception('Exception while sending VK message to %s (attempt %d)', vk_id, attempt + 1)
                if attempt == max_retries - 1:
                    return False
                time.sleep(1)
        return False

    # Оптимизированная рассылка с ThreadPoolExecutor
    def send_single_message(vk_id):
        try:
            ok = _send_vk_with_keyboard(vk_id, text_with_link_in_text, keyboard_json)
            return vk_id, ok
        except Exception:
            log.exception('Exception while sending to %s', vk_id)
            return vk_id, False

    max_workers = 5  # оставляем как у тебя
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(send_single_message, vk_id) for vk_id in vk_ids]
        for future in futures:
            vk_id, success = future.result()
            if success:
                sent += 1
            else:
                failed += 1
                failed_list.append(vk_id)

    summary = f'Рассылка завершена. Всего: {total}, успешно: {sent}, неудач: {failed}.'
    if failed_list:
        # выводим айдишники столбиком, чтобы удобнее читать
        sample_ids = list(map(str, failed_list[:20]))
        more_suffix = f"\n...(+{len(failed_list)-20} ещё)" if len(failed_list) > 20 else ""
        column = "\n".join(sample_ids)
        summary += f"\nНе доставлено:\n{column}{more_suffix}"

    update.message.reply_text(summary)
# ----------------- other command handlers (unchanged) -----------------

def _process_send_command(from_id: int, text: str, msg_reply_func):
    text = (text or '').strip()
    
    # Убираем /send из начала
    if text.lower().startswith('/send'):
        text = text[5:].strip()
    
    # Разбиваем по пробелам и берем первые 3 части
    parts = text.split()
    if len(parts) < 3:
        msg_reply_func('Недостаточно параметров. Используйте: /send <предмет> <тип курса> <блок>')
        return

    subject = parts[0].strip()
    course_type = parts[1].strip()
    block = parts[2].strip()

    cur = load_current_for_user(from_id)
    file_path = cur.get('file_path', '')
    if not file_path:
        reply = 'Нет ожидающего файла. Сначала пришлите файл (CSV/XLSX).'
        if not DRY_RUN:
            msg_reply_func(reply)
        else:
            log.info('[DRY RUN] %s', reply)
        return

    csv_path = ensure_csv(file_path)
    if not csv_path:
        reply = f'Не удалось обработать файл {file_path} (чтение/конвертация).'
        if not DRY_RUN:
            msg_reply_func(reply)
        else:
            log.info('[DRY RUN] %s', reply)
        return

    dest = publish_to_hosting(csv_path, subject, course_type, block, uploaded_by=from_id)
    if dest:
        save_current_for_user(from_id, file_path='', awaiting_meta=False)
        reply = (f'Ведомость опубликована: {dest}\n'
         f'Название ведомости: {os.path.basename(dest)}')
    else:
        reply = 'Публикация не удалась.'

    if not DRY_RUN:
        msg_reply_func(reply)
    else:
        log.info('[DRY RUN] %s', reply)

def send_command(update: Update, context: CallbackContext):
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /send from non-admin %s', from_id)
        msg.reply_text('Только админы могут публиковать файлы.')
        return
    _process_send_command(from_id, msg.text or '', msg.reply_text)

def addadmin_command(update: Update, context: CallbackContext):
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /addadmin from non-admin %s', from_id)
        msg.reply_text('Только админы могут добавлять админов.')
        return

    target_id = None
    text = (msg.text or '').strip()

    if msg.reply_to_message and msg.reply_to_message.from_user:
        target_id = int(msg.reply_to_message.from_user.id)
        log.info('Adding admin via reply: %s', target_id)
    else:
        m = re.match(r'^\s*/addadmin\s+(.+)$', text, flags=re.IGNORECASE)
        if not m:
            msg.reply_text('Использование: /addadmin <username_or_id>')
            return
        target = m.group(1).strip()
        if re.fullmatch(r'\d+', target):
            target_id = int(target)
        else:
            if target.startswith('@'):
                target = target[1:]
            username = target
            try:
                chat = context.bot.get_chat(f"@{username}")
                target_id = int(chat.id)
                log.info('Resolved @%s via get_chat -> id=%s', username, target_id)
            except Exception as e:
                log.info('get_chat failed for @%s: %s — пытаемся локальную базу', username, e)
                users = load_users()
                if username in users:
                    try:
                        target_id = int(users[username])
                        log.info('Resolved @%s via local users.json -> id=%s', username, target_id)
                    except Exception:
                        target_id = None
                else:
                    target_id = None

    if not target_id:
        msg.reply_text(
            'Не удалось определить пользователя. Убедитесь, что:\n'
            '- вы ответили на сообщение пользователя и вызвали /addadmin, или\n'
            '- указали числовой id, или\n'
            "- указали username и этот пользователь раньше запускал бота (выполнял /start).\n"
            'Если у вас есть numeric id — используйте /addadmin 123456789.'
        )
        return

    if int(target_id) in ADMIN_IDS:
        msg.reply_text(f'Пользователь id={target_id} уже является админом.')
        return

    ADMIN_IDS.add(int(target_id))
    try:
        save_admins_to_file()
    except Exception:
        log.exception('Failed to persist admins after adding %s', target_id)
    msg.reply_text(f'Пользователь id={target_id} добавлен в админы.')

def deladmin_command(update: Update, context: CallbackContext):
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /deladmin from non-admin %s', from_id)
        msg.reply_text('Только админы могут удалять админов.')
        return

    target_id = None
    text = (msg.text or '').strip()

    if msg.reply_to_message and msg.reply_to_message.from_user:
        target_id = int(msg.reply_to_message.from_user.id)
        log.info('Deleting admin via reply: %s', target_id)
    else:
        m = re.match(r'^\s*/deladmin\s+(.+)$', text, flags=re.IGNORECASE)
        if not m:
            msg.reply_text('Использование: /deladmin <username_or_id>')
            return
        target = m.group(1).strip()
        if re.fullmatch(r'\d+', target):
            target_id = int(target)
        else:
            if target.startswith('@'):
                target = target[1:]
            username = target
            try:
                chat = context.bot.get_chat(f"@{username}")
                target_id = int(chat.id)
                log.info('Resolved @%s via get_chat -> id=%s', username, target_id)
            except Exception as e:
                log.info('get_chat failed for @%s: %s — пытаемся локальную базу', username, e)
                users = load_users()
                if username in users:
                    try:
                        target_id = int(users[username])
                        log.info('Resolved @%s via local users.json -> id=%s', username, target_id)
                    except Exception:
                        target_id = None
                else:
                    target_id = None

    if not target_id:
        msg.reply_text(
            'Не удалось определить пользователя. Убедитесь, что:\n'
            '- вы ответили на сообщение пользователя и вызвали /deladmin, или\n'
            '- указали числовой id, или\n'
            "- указали username и этот пользователь раньше запускал бота (выполнял /start)."
        )
        return

    if int(target_id) not in ADMIN_IDS:
        msg.reply_text(f'Пользователь id={target_id} не является админом.')
        return

    if len(ADMIN_IDS) <= 1:
        msg.reply_text('Нельзя удалить последнего админа — сначала добавьте другого админа.')
        return

    ADMIN_IDS.discard(int(target_id))
    try:
        save_admins_to_file()
    except Exception:
        log.exception('Failed to persist admins after removing %s', target_id)
    msg.reply_text(f'Пользователь id={target_id} удалён из админов.')

def listadmins_command(update: Update, context: CallbackContext):
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /listadmins from non-admin %s', from_id)
        msg.reply_text('Только админы могут просматривать список админов.')
        return

    if not ADMIN_IDS:
        msg.reply_text('Список админов пуст.')
        return

    users = load_users()
    lines = []
    for aid in sorted(ADMIN_IDS):
        uname = users.get(str(aid)) or ''
        if uname:
            lines.append(f'@{uname} ({aid})')
        else:
            lines.append(str(aid))

    text = 'Текущий список админов:\n' + '\n'.join(lines)
    msg.reply_text(text)

def liststatements_command(update: Update, context: CallbackContext):
    """Команда /liststatements — показывает все открытые и архивные ведомости."""
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /liststatements from non-admin %s', from_id)
        msg.reply_text('Только админы могут просматривать список ведомостей.')
        return

    try:
        # Собираем открытые ведомости из папок
        open_statements = []
        open_path = os.path.join(HOSTING_ROOT, OPEN_DIRNAME)
        
        if os.path.exists(open_path):
            # Проходим по всем папкам и ищем CSV файлы (не в папке users)
            for root, dirs, files in os.walk(open_path):
                # Пропускаем папку users
                if 'users' in root:
                    continue
                    
                for file in files:
                    if file.endswith('.csv'):
                        file_path = os.path.join(root, file)
                        # Получаем время архивации из БД
                        archive_time = get_archive_time_for_file(file)
                        open_statements.append({
                            'name': file,
                            'path': file_path,
                            'archive_at': archive_time
                        })

        # Собираем архивные ведомости из папок
        archive_statements = []
        archive_path = os.path.join(HOSTING_ROOT, ARCHIVE_DIRNAME)
        
        if os.path.exists(archive_path):
            for root, dirs, files in os.walk(archive_path):
                # Пропускаем папку users
                if 'users' in root:
                    continue
                    
                for file in files:
                    if file.endswith('.csv'):
                        archive_statements.append(file)

        # Формируем ответ
        response_lines = []
        
        # Открытые ведомости
        response_lines.append('ОТКРЫТЫЕ ВЕДОМОСТИ:')
        if open_statements:
            for stmt in open_statements:
                name = stmt['name']
                archive_at = stmt['archive_at']
                
                if archive_at and archive_at > 0:
                    now = int(time.time())
                    hours_left = max(0, (archive_at - now) // 3600)
                    if hours_left > 0:
                        response_lines.append(f'  • {name} (архивация через {hours_left}ч)')
                    else:
                        response_lines.append(f'  • {name} (готова к архивации)')
                else:
                    response_lines.append(f'  • {name} (время архивации не установлено)')
        else:
            response_lines.append('  (нет открытых ведомостей)')
        
        response_lines.append('')
        
        # Архивные ведомости
        response_lines.append('АРХИВНЫЕ ВЕДОМОСТИ:')
        if archive_statements:
            for name in sorted(archive_statements):
                response_lines.append(f'  • {name}')
        else:
            response_lines.append('  (нет архивных ведомостей)')
        
        response = '\n'.join(response_lines)
        
        # Telegram ограничивает длину сообщений
        if len(response) > 4000:
            response = response[:4000] + '\n...(список обрезан)'
        
        msg.reply_text(response)
        
    except Exception as e:
        log.exception('Error in liststatements_command')
        msg.reply_text(f'Ошибка при получении списка ведомостей: {str(e)}')


def get_archive_time_for_file(filename: str) -> int:
    """Получает время архивации для файла из БД."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        c.execute('SELECT archive_at FROM vedomosti_users WHERE original_filename = ? LIMIT 1', (filename,))
        row = c.fetchone()
        conn.close()
        
        if row and row[0]:
            return int(row[0])
        return 0
    except Exception:
        log.exception('Failed to get archive time for file %s', filename)
        return 0


def archive_command(update: Update, context: CallbackContext):
    """Команда для ручной архивации конкретной ведомости: /archive <название ведомости>"""
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /archive from non-admin %s', from_id)
        msg.reply_text('Только админы могут архивировать ведомости.')
        return

    # Получаем название ведомости
    if not context.args:
        msg.reply_text(
            'Укажите название ведомости для архивации.\n'
            'Использование: /archive <название ведомости>\n'
            'Пример: /archive Русский ОГЭ ПГК\n\n'
            'Для просмотра доступных ведомостей используйте /liststatements'
        )
        return
    
    statement_name = ' '.join(context.args).strip()
    
    try:
        # Нормализуем название (как в notify_command)
        statement_normalized = statement_name.replace(' ', '_')
        target_filename = statement_normalized + '.csv'
        
        # Ищем ведомость в открытых папках
        statement_folder = find_statement_folder(target_filename)
        if not statement_folder:
            # Пробуем гибкий поиск
            statement_folder = find_statement_folder_flexible(statement_name)
            if statement_folder:
                target_filename = os.path.basename([f for f in os.listdir(statement_folder) if f.endswith('.csv')][0])
        
        if not statement_folder:
            msg.reply_text(f'Ведомость "{statement_name}" не найдена в открытых папках.\nИспользуйте /liststatements для просмотра доступных ведомостей.')
            return
        
        # Проверяем есть ли пользователи этой ведомости в БД
        users_count = count_users_in_statement(target_filename)
        
        msg.reply_text(f'Начинаю архивацию ведомости "{statement_name}".\nНайдено пользователей в БД: {users_count}')
        
        # Выполняем архивацию
        success = archive_statement_manually(target_filename, statement_folder)
        
        if success:
            # Удаляем пользователей из БД
            removed_count = remove_users_from_statement(target_filename)
            msg.reply_text(
                f'Ведомость "{statement_name}" успешно заархивирована.\n'
                f'Папка перемещена в архив.\n'
                f'Удалено записей из БД: {removed_count}'
            )
        else:
            msg.reply_text(f'Ошибка при архивации ведомости "{statement_name}". Проверьте логи.')
            
    except Exception as e:
        log.exception('Error in manual archive command')
        msg.reply_text(f'Ошибка при архивации: {str(e)}')


def find_statement_folder(filename: str) -> str:
    """Находит папку содержащую указанный файл ведомости."""
    open_path = os.path.join(HOSTING_ROOT, OPEN_DIRNAME)
    
    if not os.path.exists(open_path):
        return None
    
    for root, dirs, files in os.walk(open_path):
        # Пропускаем папки users
        if 'users' in root:
            continue
        
        if filename in files:
            return root
    
    return None


def find_statement_folder_flexible(statement_name: str) -> str:
    """Гибкий поиск папки ведомости (игнорирует различия пробелов и подчеркиваний)."""
    open_path = os.path.join(HOSTING_ROOT, OPEN_DIRNAME)
    
    if not os.path.exists(open_path):
        return None
    
    # Нормализуем искомое название
    normalized_search = statement_name.replace(' ', '_').replace('_', ' ').lower()
    
    for root, dirs, files in os.walk(open_path):
        if 'users' in root:
            continue
        
        for file in files:
            if file.endswith('.csv'):
                # Нормализуем найденное название
                file_normalized = file.replace('.csv', '').replace('_', ' ').lower()
                
                if normalized_search in file_normalized or file_normalized in normalized_search:
                    return root
    
    return None


def find_statement_folder_in_archive(filename: str) -> str:
    """Находит папку в архиве, содержащую указанный файл ведомости."""
    archive_path = os.path.join(HOSTING_ROOT, ARCHIVE_DIRNAME)
    if not os.path.exists(archive_path):
        return None
    for root, dirs, files in os.walk(archive_path):
        if 'users' in root:
            continue
        if filename in files:
            return root
    return None


def find_statement_folder_flexible_in_archive(statement_name: str) -> str:
    """Гибкий поиск в архиве (игнорирует различия пробелов и подчёркиваний)."""
    archive_path = os.path.join(HOSTING_ROOT, ARCHIVE_DIRNAME)
    if not os.path.exists(archive_path):
        return None
    normalized_search = statement_name.replace(' ', '_').replace('_', ' ').lower()
    for root, dirs, files in os.walk(archive_path):
        if 'users' in root:
            continue
        for file in files:
            if file.endswith('.csv'):
                file_normalized = file.replace('.csv', '').replace('_', ' ').lower()
                if normalized_search in file_normalized or file_normalized in normalized_search:
                    return root
    return None


def delete_statement_folder(folder_path: str) -> bool:
    """Удаляет папку ведомости (со всеми файлами)."""
    try:
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            log.info('Deleted statement folder: %s', folder_path)
            return True
        log.warning('Statement folder to delete not found: %s', folder_path)
        return False
    except Exception:
        log.exception('Failed to delete statement folder: %s', folder_path)
        return False


def delete_command(update: Update, context: CallbackContext):
    """Команда /delete <название> — удаляет ведомость из открытых или архивных папок и записи из БД."""
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /delete from non-admin %s', from_id)
        msg.reply_text('Только админы могут удалять ведомости.')
        return

    if not context.args:
        msg.reply_text(
            'Укажите название ведомости для удаления.\n'
            'Использование: /delete <название ведомости>\n'
            'Пример: /delete Русский ОГЭ ПГК'
        )
        return

    statement_name = ' '.join(context.args).strip()

    try:
        # Сначала пробуем точное имя файла в стиле open: "Имя_Тип_Блок.csv"
        normalized = statement_name.replace(' ', '_')
        target_filename = normalized + '.csv'

        # 1) Ищем в открытых папках
        folder = find_statement_folder(target_filename)
        location = 'open'

        # 2) Если не нашли — гибкий поиск в открытых
        if not folder:
            folder = find_statement_folder_flexible(statement_name)
            if folder:
                # Выберем реальное имя csv из найденной папки
                try:
                    any_csv = [f for f in os.listdir(folder) if f.endswith('.csv')]
                    if any_csv:
                        target_filename = any_csv[0]
                except Exception:
                    pass

        # 3) Если в открытых не нашли — ищем в архиве (точно, затем гибко)
        if not folder:
            location = 'archive'
            folder = find_statement_folder_in_archive(target_filename)
            if not folder:
                folder = find_statement_folder_flexible_in_archive(statement_name)
                if folder:
                    try:
                        any_csv = [f for f in os.listdir(folder) if f.endswith('.csv')]
                        if any_csv:
                            target_filename = any_csv[0]
                    except Exception:
                        pass

        if not folder:
            msg.reply_text(
                f'Ведомость "{statement_name}" не найдена ни в открытых, ни в архивных папках.\n'
                'Проверьте название или используйте /liststatements для просмотра списка.'
            )
            return

        # Удаляем папку
        ok = delete_statement_folder(folder)
        # Чистим БД (на всякий случай — даже если уже была заархивирована)
        removed_db = remove_users_from_statement(target_filename)

        if ok:
            where = 'из открытых' if location == 'open' else 'из архива'
            msg.reply_text(
                f'Ведомость "{target_filename}" успешно удалена {where}.\n'
                f'Удалено записей из БД: {removed_db}'
            )
        else:
            msg.reply_text(f'Не удалось удалить папку ведомости для "{target_filename}". Проверьте логи.')

    except Exception as e:
        log.exception('Error in delete command for %s', statement_name)
        msg.reply_text(f'Ошибка при удалении ведомости: {str(e)}')

def count_users_in_statement(filename: str) -> int:
    """Подсчитывает количество пользователей ведомости в БД."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM vedomosti_users WHERE original_filename = ?', (filename,))
        count = c.fetchone()[0]
        conn.close()
        return count
    except Exception:
        log.exception('Failed to count users for statement %s', filename)
        return 0


def archive_statement_manually(filename: str, statement_folder: str) -> bool:
    """Архивирует ведомость вручную (перемещает всю папку в архив)."""
    try:
        archive_path = os.path.join(HOSTING_ROOT, ARCHIVE_DIRNAME)
        open_path = os.path.join(HOSTING_ROOT, OPEN_DIRNAME)
        
        # Создаем структуру архива
        os.makedirs(archive_path, exist_ok=True)
        
        # Определяем относительный путь от open до папки с ведомостью
        relative_path = os.path.relpath(statement_folder, open_path)
        archive_folder = os.path.join(archive_path, relative_path)
        
        # Создаем родительские папки в архиве
        os.makedirs(os.path.dirname(archive_folder), exist_ok=True)
        
        # Перемещаем всю папку с ведомостью в архив
        if os.path.exists(statement_folder):
            shutil.move(statement_folder, archive_folder)
            log.info('Manually moved statement folder to archive: %s -> %s', statement_folder, archive_folder)
            return True
        else:
            log.warning('Statement folder not found: %s', statement_folder)
            return False
            
    except Exception:
        log.exception('Failed to manually archive statement %s from folder %s', filename, statement_folder)
        return False


def remove_users_from_statement(filename: str) -> int:
    """Удаляет всех пользователей указанной ведомости из БД."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        c.execute('DELETE FROM vedomosti_users WHERE original_filename = ?', (filename,))
        affected = c.rowcount
        conn.commit()
        conn.close()
        log.info('Removed %d users for statement %s from database', affected, filename)
        return affected
    except Exception:
        log.exception('Failed to remove users from DB for statement %s', filename)
        return 0

def unknown(update: Update, context: CallbackContext):
    update.message.reply_text('Неизвестная команда. Используйте /start, пришлите файл или /send <предмет> <тип курса> <блок>.')

# ----------------- archive functions -----------------

def get_vedomosti_to_archive():
    """Получить список ведомостей, которые нужно архивировать."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        now = int(time.time())
        c.execute('''
            SELECT DISTINCT original_filename, personal_path 
            FROM vedomosti_users 
            WHERE archive_at > 0 AND archive_at <= ? AND state LIKE 'imported:%'
        ''', (now,))
        rows = c.fetchall()
        conn.close()
        return rows
    except Exception:
        log.exception('Failed to get vedomosti to archive')
        return []

def archive_vedomosti_folder(filename: str, personal_path: str):
    """Архивировать всю папку с ведомостью (переместить из open в archive)."""
    try:
        # Определяем корневую папку ведомости по personal_path
        # personal_path выглядит как: hosting/open/Предмет/Тип/Блок/users/user_file.csv
        # Нужно найти папку с ведомостью: hosting/open/Предмет/Тип/Блок/
        
        open_path = os.path.join(HOSTING_ROOT, OPEN_DIRNAME)
        archive_path = os.path.join(HOSTING_ROOT, ARCHIVE_DIRNAME)
        
        # Находим папку с ведомостью, которая содержит этот файл
        vedomosti_folder = None
        for root, dirs, files in os.walk(open_path):
            if filename in files:
                vedomosti_folder = root
                break
        
        if not vedomosti_folder:
            log.warning('Vedomosti folder not found for file: %s', filename)
            return False
        
        # Создаем структуру папок в архиве
        os.makedirs(archive_path, exist_ok=True)
        
        # Определяем относительный путь от open до папки с ведомостью
        relative_path = os.path.relpath(vedomosti_folder, open_path)
        archive_folder = os.path.join(archive_path, relative_path)
        
        # Перемещаем всю папку с ведомостью в архив
        if os.path.exists(vedomosti_folder):
            shutil.move(vedomosti_folder, archive_folder)
            log.info('Moved entire vedomosti folder to archive: %s -> %s', vedomosti_folder, archive_folder)
            return True
        else:
            log.warning('Vedomosti folder not found: %s', vedomosti_folder)
            return False
            
    except Exception:
        log.exception('Failed to archive vedomosti folder for file %s', filename)
        return False

def remove_vedomosti_from_db(filename: str):
    """Удалить записи ведомости из базы данных."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        c.execute('DELETE FROM vedomosti_users WHERE original_filename = ?', (filename,))
        affected = c.rowcount
        conn.commit()
        conn.close()
        log.info('Removed %d records for filename %s from database', affected, filename)
        return affected
    except Exception:
        log.exception('Failed to remove vedomosti from DB for filename %s', filename)
        return 0

def get_users_to_warn(filename: str):
    """Получить список уникальных vk_id для предупреждения о скорой архивации.
    Исключает пользователей со статусом 'agreed' и тех, кому уже отправлено предупреждение по этой ведомости.
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        c.execute('''
            SELECT DISTINCT vk_id
            FROM vedomosti_users
            WHERE original_filename = ?
              AND state LIKE 'imported:%'
              AND IFNULL(status, '') <> 'agreed'
              AND IFNULL(warning_sent, 0) = 0
        ''', (filename,))
        rows = c.fetchall()
        conn.close()
        return [str(r[0]) for r in rows if r and str(r[0]).strip()]
    except Exception:
        log.exception('Failed to get users to warn for filename %s', filename)
        return []

def send_archive_warning(vk_id: str, filename: str, archive_at: int):
    """Отправить предупреждение пользователю о скорой архивации (через REST VK API)."""
    try:
        if not VK_TOKEN or not GROUP_ID:
            log.warning('VK credentials not configured, cannot send warning to %s', vk_id)
            return False

        base_filename = filename[:-4] if filename.endswith('.csv') else filename
        hours_left = max(0, (archive_at - int(time.time())) // 3600)
        message = (
            f"Внимание! Ведомость '{base_filename}' будет заархивирована через {hours_left} часов. "
            f"Пожалуйста, подтвердите или оспорьте выплату до этого времени."
        )

        ok = send_vk_message(str(vk_id), message)
        if ok:
            log.info('Sent archive warning to user %s for filename %s', vk_id, filename)
        return ok
    except Exception:
        log.exception('Failed to send archive warning to user %s', vk_id)
        return False

def process_archive():
    """Основная функция архивации - проверяет и архивирует ведомости."""
    try:
        vedomosti_to_archive = get_vedomosti_to_archive()
        if not vedomosti_to_archive:
            log.debug('No vedomosti to archive')
            return
            
        log.info('Found %d vedomosti to archive', len(vedomosti_to_archive))
        
        for filename, personal_path in vedomosti_to_archive:
            log.info('Processing archive for filename: %s', filename)
            
            # 1. Архивируем всю папку с ведомостью
            if archive_vedomosti_folder(filename, personal_path):
                # 2. Удаляем из базы данных
                removed_count = remove_vedomosti_from_db(filename)
                log.info('Successfully archived folder and removed %d records for filename: %s', removed_count, filename)
            else:
                log.error('Failed to archive folder for filename: %s', filename)
                
    except Exception:
        log.exception('Error in process_archive')

def process_warnings():
    """Проверяет ведомости, которым нужно отправить предупреждения (единоразово за 8 часов до архивации)."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        now = int(time.time())
        
        # Предупреждаем за 8 часов до архивации (точное время)
        warning_time = now + (8 * 3600)
        
        # Ищем ведомости, которые нужно предупредить (в течение 1 часа от времени предупреждения)
        warning_start = warning_time - 1800  # 30 минут до времени предупреждения
        warning_end = warning_time + 1800    # 30 минут после времени предупреждения
        
        c.execute('''
            SELECT DISTINCT original_filename, archive_at 
            FROM vedomosti_users 
            WHERE archive_at >= ? AND archive_at <= ? AND state LIKE 'imported:%'
        ''', (warning_start, warning_end))
        rows = c.fetchall()
        conn.close()
        
        for filename, archive_at in rows:
            # Получаем только тех пользователей, кому ещё не отправляли и кто не agreed
            vk_ids = get_users_to_warn(filename)

            for vk_id in vk_ids:
                sent_ok = send_archive_warning(vk_id, filename, int(archive_at))
                if sent_ok:
                    try:
                        conn2 = sqlite3.connect(DB_PATH, timeout=30)
                        c2 = conn2.cursor()
                        # Помечаем все строки этой ведомости для данного vk_id
                        c2.execute(
                            'UPDATE vedomosti_users SET warning_sent = 1, warning_sent_at = ? WHERE original_filename = ? AND vk_id = ?',
                            (int(time.time()), filename, str(vk_id))
                        )
                        conn2.commit()
                        conn2.close()
                    except Exception:
                        log.exception('Failed to mark warning_sent for vk_id=%s (file=%s)', vk_id, filename)
                time.sleep(2)  # Пауза 2 секунды между отправками (сервер)
                
    except Exception:
        log.exception('Error in process_warnings')

def archive_worker():
    """Фоновый поток для архивации."""
    log.info('Archive worker started')
    while True:
        try:
            process_warnings()  # Сначала предупреждения
            process_archive()   # Потом архивация
        except Exception:
            log.exception('Error in archive worker')
        
        # Проверяем каждые 30 минут
        time.sleep(30 * 60)

# ----------------- run -----------------

def run_bot():
    if not TELEGRAM_TOKEN:
        log.error('Set TELEGRAM_TOKEN in config.py or env')
        return

    # initialize sqlite and ensure columns for statuses
    init_db()
    ensure_vedomosti_status_columns()

    # Start archive worker thread
    archive_thread = threading.Thread(target=archive_worker, daemon=True)
    archive_thread.start()
    log.info('Archive worker thread started')
    updater = Updater(token=TELEGRAM_TOKEN, use_context=True)
    dp = updater.dispatcher

    try:
        commands = [
            BotCommand('start', 'Знакомство'),
            BotCommand('description', 'Показать описание процесса загрузки'),
            BotCommand('send', 'Отправить файл на хостинг: /send <предмет> <тип курса> <блок>'),
            BotCommand('notify', 'Разослать уведомление vk_id из БД'),
            BotCommand('liststatements', 'Показать список открытых и архивных ведомостей'),
            BotCommand('addadmin', 'Добавить админа: /addadmin <username_or_id>'),
            BotCommand('deladmin', 'Удалить админа: /deladmin <username_or_id>'),
            BotCommand('listadmins', 'Показать список текущих админов'),
            BotCommand('archive', 'Переместить ведомость в архив: /archive <название>'),
            BotCommand('delete', 'Удалить ведомость (в т.ч. из архива): /delete <название>')
        ]
        updater.bot.set_my_commands(commands)
        log.info('Bot commands (menu) set: %s', [c.command for c in commands])
    except Exception as e:
        log.exception('Failed to set bot commands: %s', e)

    dp.add_handler(CommandHandler('start', start))
    dp.add_handler(CommandHandler('description', description))
    dp.add_handler(CommandHandler('send', send_command))
    dp.add_handler(CommandHandler('notify', notify_command))
    dp.add_handler(CommandHandler('liststatements', liststatements_command))
    dp.add_handler(CommandHandler('addadmin', addadmin_command))
    dp.add_handler(CommandHandler('deladmin', deladmin_command))
    dp.add_handler(CommandHandler('listadmins', listadmins_command))
    dp.add_handler(CommandHandler('archive', archive_command))
    dp.add_handler(CommandHandler('delete', delete_command))
    dp.add_handler(MessageHandler(Filters.document, handle_document))
    dp.add_handler(MessageHandler(Filters.command, unknown))

    log.info('Telegram payroll hosting bot started (DRY_RUN=%s)', DRY_RUN)
    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    run_bot()
