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
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes
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


def publish_to_hosting(csv_path: str, subject: str, course_type: str, block: str, uploaded_by: int, excel_path: str = None) -> Optional[str]:
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

    # Копируем Excel файл в ту же папку (нужен для расчёта RR - там хранятся min/max)
    if excel_path and os.path.exists(excel_path) and excel_path.lower().endswith(('.xlsx', '.xls')):
        excel_ext = os.path.splitext(excel_path)[1]
        excel_fname = f"{subject_safe}_{course_safe}_{block_safe}{excel_ext}"
        excel_dest = os.path.join(dest_dir, excel_fname)
        try:
            shutil.copy2(excel_path, excel_dest)
            log.info('Copied Excel file to hosting: %s', excel_dest)
        except Exception as e:
            log.warning('Failed to copy Excel file to hosting: %s', e)

    # импортируем пользователей и создаём персональные файлы
    try:
        import_users_from_csv(dest_path, original_filename=os.path.basename(dest_path))
    except Exception:
        log.exception('Failed to import users from %s', dest_path)

    return dest_path


def publish_to_hosting_repet(csv_path: str, subject: str, course_type: str, block: str, uploaded_by: int, excel_path: str = None) -> Optional[str]:
    """Публикует файл для репетиторов на хостинг."""
    if not os.path.exists(csv_path):
        log.warning('publish_repet: file not found %s', csv_path)
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

    fname = f"{subject_safe}_{course_safe}_{block_safe}.csv"
    dest_path = os.path.join(dest_dir, fname)

    try:
        shutil.move(csv_path, dest_path)
    except Exception as e:
        log.exception('Failed to move file to hosting: %s', e)
        return None

    log.info('Published repet to hosting: %s (subject=%s course_type=%s block=%s)', dest_path, subject, course_type, block)

    # Копируем Excel файл в ту же папку (если есть)
    if excel_path and os.path.exists(excel_path) and excel_path.lower().endswith(('.xlsx', '.xls')):
        excel_ext = os.path.splitext(excel_path)[1]
        excel_fname = f"{subject_safe}_{course_safe}_{block_safe}{excel_ext}"
        excel_dest = os.path.join(dest_dir, excel_fname)
        try:
            shutil.copy2(excel_path, excel_dest)
            log.info('Copied Excel file to hosting: %s', excel_dest)
        except Exception as e:
            log.warning('Failed to copy Excel file to hosting: %s', e)

    # импортируем пользователей-репетиторов и создаём персональные файлы
    try:
        import_users_from_csv_repet(dest_path, original_filename=os.path.basename(dest_path))
    except Exception:
        log.exception('Failed to import repet users from %s', dest_path)

    return dest_path


# ----------------- import users from CSV (reliable per-row files) -----------------

def _safe_filename_component(s: str) -> str:
    s = (s or '').strip()
    s = re.sub(r'[\\/:\x00-\x1f<>\"|?*]+', '_', s)
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'__+', '_', s)
    return s[:120] or 'unknown'

def extract_vk_id(vk_value: str) -> Optional[str]:
    """
    Извлекает числовой VK ID из строки.
    Поддерживает форматы:
    - Просто число: "295942550"
    - Полная ссылка: "https://vk.com/id295942550"
    - Короткая ссылка: "vk.com/id295942550"
    - С пробелами: "https://vk.com/id 295942550"
    
    Возвращает числовой ID как строку или None если не удалось извлечь.
    """
    if not vk_value:
        return None
    
    vk_str = str(vk_value).strip()
    if not vk_str:
        return None
    
    # Пробуем извлечь ID из ссылки vk.com/idXXXXX
    match = re.search(r'(?:vk\.com/id|id)(\d{5,})', vk_str, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Если это просто число, возвращаем его
    if re.match(r'^\d{5,}$', vk_str):
        return vk_str
    
    # Пробуем найти любое длинное число (5+ цифр) в строке
    match = re.search(r'(\d{5,})', vk_str)
    if match:
        return match.group(1)
    
    return None

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
            
            # Извлекаем числовой ID из значения (может быть ссылка или число)
            vk_id_extracted = extract_vk_id(str(vk_val))
            if not vk_id_extracted:
                log.warning('Could not extract vk_id from value: %s (row %s)', vk_val, idx)
                continue
            
            vk_str = vk_id_extracted
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


def import_users_from_csv_repet(dest_path: str, original_filename: str):
    """Прочитать CSV для репетиторов и для каждой строки с колонкой ВК создать индивидуальный файл и запись в sqlite."""
    if not os.path.exists(dest_path):
        log.warning('import_repet: file not found %s', dest_path)
        return

    try:
        df = pd.read_csv(dest_path, dtype=str)
    except Exception:
        try:
            df = pd.read_csv(dest_path, encoding='cp1251', dtype=str)
        except Exception:
            log.exception('Failed to read CSV for import_repet %s', dest_path)
            return

    # поиск колонки ВК (содержит ссылки вида https://vk.com/id123456)
    vk_col = None
    for col in df.columns:
        col_lower = str(col).strip().lower()
        if col_lower in ['вк', 'vk']:
            vk_col = col
            break

    if not vk_col:
        log.info('CSV %s не содержит столбца ВК — импорт пропущен', dest_path)
        return

    users_dir = os.path.join(os.path.dirname(dest_path), 'users')
    os.makedirs(users_dir, exist_ok=True)

    # --------------------------
    # Очищаем и сокращаем original_filename
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
            vk_link = row.get(vk_col, '')
            if pd.isna(vk_link) or str(vk_link).strip() == '':
                continue
            
            # Парсим VK ID из ссылки (https://vk.com/id123456)
            vk_link_str = str(vk_link).strip()
            match = re.search(r'vk\.com/id(\d+)', vk_link_str)
            if match:
                vk_str = match.group(1)
            elif re.fullmatch(r'\d+', vk_link_str):
                # Если это просто число
                vk_str = vk_link_str
            else:
                log.warning('Cannot parse VK ID from: %s', vk_link_str)
                continue
            
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
            log.info('Created personal file for repet vk_id=%s -> %s', vk_str, personal_path)
        except Exception:
            log.exception('Error processing row %s in %s', idx, dest_path)

    # Выполняем все операции с БД одним пакетом
    if db_operations:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30)
            c = conn.cursor()
            now = int(time.time())
            archive_time = now + (36 * 3600)  # 36 часов в секундах

            # Для КАЖДОЙ строки создаём уникальный идентификатор состояния с префиксом repet_
            for vk_str, personal_path, orig_fn in db_operations:
                unique_state = f"repet_imported:{uuid.uuid4()}"
                log.info('Creating unique state for repet vk_id=%s: %s', vk_str, unique_state)
                c.execute(
                    'INSERT INTO vedomosti_users(vk_id, personal_path, original_filename, state, created_at, archive_at) VALUES (?,?,?,?,?,?)',
                    (str(vk_str), personal_path, orig_fn, unique_state, now, archive_time)
                )

            conn.commit()
            conn.close()
            log.info('Bulk inserted %d repet vedomosti users to database with unique states', len(db_operations))
        except Exception:
            log.exception('Failed to bulk insert repet vedomosti users')

    log.info('Imported %s repet users from %s (vk_col=%s)', count, dest_path, vk_col)


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
    Поддерживает как числовой ID, так и ссылки вида https://vk.com/id295942550
    """
    if not VK_TOKEN or not GROUP_ID:
        log.error('VK_TOKEN or GROUP_ID not configured; cannot send VK messages.')
        return False

    # Извлекаем числовой ID из значения (может быть ссылка или число)
    vk_id_extracted = extract_vk_id(str(user_vk_id))
    if not vk_id_extracted:
        log.warning('Invalid vk id (could not extract): %s', user_vk_id)
        return False
    
    try:
        uid = int(vk_id_extracted)
    except Exception:
        log.warning('Invalid vk id (not numeric after extraction): %s (extracted: %s)', user_vk_id, vk_id_extracted)
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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    uid = int(user.id)
    save_user_entry(user)
    if not ADMIN_IDS:
        ADMIN_IDS.add(int(uid))
        try:
            save_admins_to_file()
        except Exception:
            log.exception('Failed to persist admin after auto-adding creator')
        await update.message.reply_text('Ты автоматически зарегистрирован(а) как админ (поскольку список админов был пуст).')
        log.info('Auto-added admin: %s (%s)', uid, user.username)

    await update.message.reply_text(
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
        '/update <название ведомости> — обновить данные в существующей ведомости (заменить файл и уведомить пользователей с изменениями)\n'
        '/liststatements — показать список всех открытых и архивных ведомостей\n'
        '/find <VK ID или ссылка> — поиск ведомостей по VK ID пользователя\n'
        'Пример: /find https://vk.com/id160898445\n'
        '/archive <название ведомости> - ведомость переместится в архивную сразу же, она исчезнет у Кураторов в интерфейсе ВК\n'
        '/delete <название1> <название2> ... - удалить одну или несколько архивных ведомостей\n'
    )

async def description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
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
        '/update <название ведомости> — обновить данные в существующей ведомости (заменить файл и уведомить пользователей с изменениями)\n'
        '/liststatements — показать список всех открытых и архивных ведомостей\n'
        '/find <VK ID или ссылка> — поиск ведомостей по VK ID пользователя\n'
        'Пример: /find https://vk.com/id160898445\n'
        '/archive <название ведомости> - ведомость переместится в архивную сразу же, она исчезнет у Кураторов в интерфейсе ВК\n'
        '/delete <название1> <название2> ... - удалить одну или несколько архивных ведомостей\n'
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring document from non-admin %s', from_id)
        await msg.reply_text('Только админы могут загружать файлы.')
        return

    doc = msg.document
    if not doc:
        return
    filename = doc.file_name or f'document_{doc.file_id}'
    ext = os.path.splitext(filename)[1].lstrip('.').lower()
    if ext not in ALLOWED_EXCEL_EXT:
        await msg.reply_text(f'Неподдерживаемое расширение .{ext}. Поддерживаемые: .xlsx, .xls, .csv')
        return

    os.makedirs(UPLOADS_DIR, exist_ok=True)
    # Санитизируем имя файла (убираем проблемные символы, но сохраняем расширение)
    name_part, ext_part = os.path.splitext(filename)
    safe_name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', name_part)  # Убираем запрещённые символы
    safe_title = safe_name + ext_part
    local_name = safe_title
    local_path = os.path.join(UPLOADS_DIR, local_name)

    try:
        file_obj = await context.bot.get_file(doc.file_id)
        await file_obj.download_to_drive(custom_path=local_path)
        log.info('Downloaded file to %s', local_path)
    except Exception as e:
        log.exception('Failed to download file: %s', e)
        await msg.reply_text('Не удалось загрузить файл. Попробуйте ещё раз.')
        return

    save_current_for_user(from_id, file_path=local_path, awaiting_meta=True)

    reply = ('Файл сохранён, ожидает публикации.\n'
             'Отправьте инфу командой: /send <предмет> <тип курса> <блок>\n'
             'Пример: /send Русский ОГЭ ПГК\n'
             'Для репетиторов: /send_repet <предмет> <тип курса> <блок>\n'
             'Пример: /send_repet Физика ОГЭ 1\n\n'
             'Для обновления существующей ведомости используйте: /update <название ведомости>')
    if not DRY_RUN:
        await msg.reply_text(reply)
    else:
        log.info('[DRY RUN] %s', reply)

# ----------------- notify handlers -----------------


async def notify_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    if not is_admin(user.id):
        await update.message.reply_text('Только админы могут отправлять рассылки.')
        return

    # Получаем название ведомости, если оно указано
    if context.args:
        subject = ' '.join(context.args).strip()  # Объединяем все аргументы в одну строку
    else:
        await update.message.reply_text('Не указано название ведомости.')
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
        await update.message.reply_text('Ошибка при чтении БД для рассылки. Смотри логи.')
        return

    # Извлекаем числовые VK ID из значений (могут быть ссылки или числа)
    vk_ids_raw = [r[0] for r in rows if r and str(r[0]).strip()]
    vk_ids = []
    for raw_id in vk_ids_raw:
        extracted = extract_vk_id(str(raw_id))
        if extracted:
            vk_ids.append(extracted)
        else:
            log.warning('Could not extract numeric vk_id from: %s', raw_id)
    vk_ids = list(dict.fromkeys(vk_ids))  # unique preserving order

    total = len(vk_ids)
    
    if total == 0:
        await update.message.reply_text(f'Не найдено пользователей для ведомости "{subject}".\nПроверьте правильность названия ведомости.')
        return
    
    # Сообщаем сколько пользователей найдено
    await update.message.reply_text(f'Начинаю рассылку для ведомости "{subject}".\nНайдено пользователей: {total}')
    
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

    await update.message.reply_text(summary)


async def notify_repet_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /notify_repet — рассылка для репетиторов, берёт VK из столбца 'ВК' (ссылки)."""
    user = update.message.from_user
    if not is_admin(user.id):
        await update.message.reply_text('Только админы могут отправлять рассылки.')
        return

    if context.args:
        subject = ' '.join(context.args).strip()
    else:
        await update.message.reply_text('Не указано название ведомости.\nИспользование: /notify_repet <название ведомости>')
        return

    try:
        # Нормализуем название
        subject_normalized = subject.replace(' ', '_')
        target_filename = subject_normalized + '.csv'
        
        # Ищем CSV файл ведомости
        csv_path = None
        open_path = os.path.join(HOSTING_ROOT, OPEN_DIRNAME)
        
        if os.path.exists(open_path):
            for root, dirs, files in os.walk(open_path):
                if 'users' in root:
                    continue
                for file in files:
                    if file == target_filename:
                        csv_path = os.path.join(root, file)
                        break
                    # Гибкий поиск
                    file_normalized = file.replace('.csv', '').replace('_', ' ').lower()
                    subject_check = subject.replace('_', ' ').lower()
                    if subject_check in file_normalized or file_normalized in subject_check:
                        csv_path = os.path.join(root, file)
                        break
                if csv_path:
                    break
        
        if not csv_path or not os.path.exists(csv_path):
            await update.message.reply_text(f'Файл ведомости "{subject}" не найден.\nИспользуйте /liststatements для просмотра доступных ведомостей.')
            return
        
        # Читаем CSV и извлекаем VK ID из столбца "ВК"
        try:
            df = pd.read_csv(csv_path, dtype=str)
        except Exception:
            try:
                # Часто файлы в Windows-1251
                df = pd.read_csv(csv_path, encoding='cp1251', dtype=str)
            except Exception:
                # В крайнем случае читаем с игнорированием битых символов
                df = pd.read_csv(csv_path, encoding='utf-8', encoding_errors='ignore', dtype=str)
        
        # Ищем столбец с VK ссылками (может называться "ВК", "VK", "vk")
        vk_column = None
        for col in df.columns:
            col_lower = col.strip().lower()
            if col_lower in ['вк', 'vk']:
                vk_column = col
                break
        
        if not vk_column:
            await update.message.reply_text(f'Столбец "ВК" не найден в файле {target_filename}.\nДоступные столбцы: {", ".join(df.columns[:10])}')
            return
        
        # Извлекаем VK ID из ссылок
        vk_ids = []
        for vk_link in df[vk_column].dropna():
            vk_link = str(vk_link).strip()
            if not vk_link:
                continue
            # Парсим ссылку типа https://vk.com/id123456 или vk.com/id123456
            match = re.search(r'vk\.com/id(\d+)', vk_link)
            if match:
                vk_ids.append(match.group(1))
            elif re.fullmatch(r'\d+', vk_link):
                # Если это просто число
                vk_ids.append(vk_link)
        
        vk_ids = list(dict.fromkeys(vk_ids))  # unique
        
    except Exception:
        log.exception('Failed to read CSV for notify_repet')
        await update.message.reply_text('Ошибка при чтении файла ведомости. Смотри логи.')
        return

    total = len(vk_ids)
    
    if total == 0:
        await update.message.reply_text(f'Не найдено VK пользователей в ведомости "{subject}".\nПроверьте столбец "ВК" в файле.')
        return
    
    await update.message.reply_text(f'Начинаю рассылку для ведомости репетиторов "{subject}".\nНайдено VK пользователей: {total}')
    
    sent = 0
    failed = 0
    failed_list = []

    text_plain = NOTIFY_TEXT or "У вас появилась новая выплата, проверьте список выплат."

    for vk_id in vk_ids:
        try:
            resp = requests.get(
                'https://api.vk.com/method/messages.send',
                params={
                    'access_token': VK_TOKEN,
                    'user_id': vk_id,
                    'random_id': 0,
                    'message': text_plain,
                    'v': '5.131'
                },
                timeout=10
            )
            data = resp.json()
            if 'response' in data:
                sent += 1
                log.info('VK notify_repet sent to %s', vk_id)
            else:
                failed += 1
                failed_list.append(vk_id)
                log.warning('VK notify_repet failed to %s: %s', vk_id, data)
        except Exception as e:
            log.exception('VK notify_repet error for %s: %s', vk_id, e)
            failed += 1
            failed_list.append(vk_id)

    summary = f'Рассылка репетиторам завершена. Всего: {total}, успешно: {sent}, неудач: {failed}.'
    if failed_list:
        sample_ids = list(map(str, failed_list[:20]))
        more_suffix = f"\n...(+{len(failed_list)-20} ещё)" if len(failed_list) > 20 else ""
        column = "\n".join(sample_ids)
        summary += f"\nНе доставлено:\n{column}{more_suffix}"

    await update.message.reply_text(summary)


async def send_keyboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /send_keyboard <vk_id> — отправляет клавиатуру с кнопкой 'К списку выплат' конкретному пользователю."""
    user = update.message.from_user
    if not is_admin(user.id):
        await update.message.reply_text('Только админы могут использовать эту команду.')
        return

    if not context.args:
        await update.message.reply_text('Не указан VK ID.\nИспользование: /send_keyboard <vk_id>\nПример: /send_keyboard 123456789')
        return

    vk_id_raw = context.args[0].strip()
    
    # Извлекаем числовой VK ID (может быть ссылка или число)
    vk_id = extract_vk_id(vk_id_raw)
    if not vk_id:
        await update.message.reply_text(f'Неверный формат VK ID: {vk_id_raw}\nУкажите числовой ID или ссылку на профиль.')
        return

    # Формируем клавиатуру
    keyboard_json = json.dumps({
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
    }, ensure_ascii=False)

    # Отправляем сообщение с клавиатурой
    try:
        resp = requests.post(
            'https://api.vk.com/method/messages.send',
            data={
                'access_token': VK_TOKEN,
                'v': '5.131',
                'user_id': vk_id,
                'message': 'Нажмите на кнопку "К списку выплат" чтобы увидеть ваши ведомости.',
                'random_id': int(time.time() * 1000) & 0x7fffffff,
                'group_id': GROUP_ID,
                'keyboard': keyboard_json
            },
            timeout=10
        )
        data = resp.json()
        
        if 'response' in data:
            await update.message.reply_text(f'✅ Клавиатура успешно отправлена пользователю VK ID: {vk_id}')
            log.info('Keyboard sent to VK user %s', vk_id)
        else:
            error_msg = data.get('error', {}).get('error_msg', 'Неизвестная ошибка')
            await update.message.reply_text(f'❌ Ошибка отправки: {error_msg}')
            log.warning('Failed to send keyboard to %s: %s', vk_id, data)
    except Exception as e:
        log.exception('Error sending keyboard to %s', vk_id)
        await update.message.reply_text(f'❌ Ошибка: {e}')


# ----------------- other command handlers (unchanged) -----------------

async def _process_send_command(from_id: int, text: str, msg_reply_func):
    text = (text or '').strip()
    
    # Убираем /send из начала
    if text.lower().startswith('/send'):
        text = text[5:].strip()
    
    # Разбиваем по пробелам и берем первые 3 части
    parts = text.split()
    if len(parts) < 3:
        await msg_reply_func('Недостаточно параметров. Используйте: /send <предмет> <тип курса> <блок>')
        return

    subject = parts[0].strip()
    course_type = parts[1].strip()
    block = parts[2].strip()

    cur = load_current_for_user(from_id)
    file_path = cur.get('file_path', '')
    if not file_path:
        reply = 'Нет ожидающего файла. Сначала пришлите файл (CSV/XLSX).'
        if not DRY_RUN:
            await msg_reply_func(reply)
        else:
            log.info('[DRY RUN] %s', reply)
        return

    # Сохраняем путь к оригинальному Excel файлу (если это Excel)
    original_excel_path = file_path if file_path.lower().endswith(('.xlsx', '.xls')) else None

    csv_path = ensure_csv(file_path)
    if not csv_path:
        reply = f'Не удалось обработать файл {file_path} (чтение/конвертация).'
        if not DRY_RUN:
            await msg_reply_func(reply)
        else:
            log.info('[DRY RUN] %s', reply)
        return

    dest = publish_to_hosting(csv_path, subject, course_type, block, uploaded_by=from_id, excel_path=original_excel_path)
    if dest:
        save_current_for_user(from_id, file_path='', awaiting_meta=False)
        reply = (f'Ведомость опубликована: {dest}\n'
         f'Название ведомости: {os.path.basename(dest)}')
    else:
        reply = 'Публикация не удалась.'

    if not DRY_RUN:
        await msg_reply_func(reply)
    else:
        log.info('[DRY RUN] %s', reply)

async def send_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /send from non-admin %s', from_id)
        await msg.reply_text('Только админы могут публиковать файлы.')
        return
    
    async def reply_func(text):
        await msg.reply_text(text)
    await _process_send_command(from_id, msg.text or '', reply_func)


async def _process_send_repet_command(from_id: int, text: str, msg_reply_func):
    """Обработка команды /send_repet для репетиторов."""
    text = (text or '').strip()
    
    # Убираем /send_repet из начала
    if text.lower().startswith('/send_repet'):
        text = text[11:].strip()
    
    # Разбиваем по пробелам и берем первые 3 части
    parts = text.split()
    if len(parts) < 3:
        await msg_reply_func('Недостаточно параметров. Используйте: /send_repet <предмет> <тип курса> <блок>')
        return

    subject = parts[0].strip()
    course_type = parts[1].strip()
    block = parts[2].strip()

    cur = load_current_for_user(from_id)
    file_path = cur.get('file_path', '')
    if not file_path:
        reply = 'Нет ожидающего файла. Сначала пришлите файл (CSV/XLSX).'
        if not DRY_RUN:
            await msg_reply_func(reply)
        else:
            log.info('[DRY RUN] %s', reply)
        return

    # Сохраняем путь к оригинальному Excel файлу (если это Excel)
    original_excel_path = file_path if file_path.lower().endswith(('.xlsx', '.xls')) else None

    csv_path = ensure_csv(file_path)
    if not csv_path:
        reply = f'Не удалось обработать файл {file_path} (чтение/конвертация).'
        if not DRY_RUN:
            await msg_reply_func(reply)
        else:
            log.info('[DRY RUN] %s', reply)
        return

    dest = publish_to_hosting_repet(csv_path, subject, course_type, block, uploaded_by=from_id, excel_path=original_excel_path)
    if dest:
        save_current_for_user(from_id, file_path='', awaiting_meta=False)
        reply = (f'Ведомость для репетиторов опубликована: {dest}\n'
                 f'Название ведомости: {os.path.basename(dest)}')
    else:
        reply = 'Публикация не удалась.'

    if not DRY_RUN:
        await msg_reply_func(reply)
    else:
        log.info('[DRY RUN] %s', reply)


async def send_repet_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /send_repet для публикации ведомостей репетиторов."""
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /send_repet from non-admin %s', from_id)
        await msg.reply_text('Только админы могут публиковать файлы.')
        return
    await _process_send_repet_command(from_id, msg.text or '', msg.reply_text)


async def addadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /addadmin from non-admin %s', from_id)
        await msg.reply_text('Только админы могут добавлять админов.')
        return

    target_id = None
    text = (msg.text or '').strip()

    if msg.reply_to_message and msg.reply_to_message.from_user:
        target_id = int(msg.reply_to_message.from_user.id)
        log.info('Adding admin via reply: %s', target_id)
    else:
        m = re.match(r'^\s*/addadmin\s+(.+)$', text, flags=re.IGNORECASE)
        if not m:
            await msg.reply_text('Использование: /addadmin <username_or_id>')
            return
        target = m.group(1).strip()
        if re.fullmatch(r'\d+', target):
            target_id = int(target)
        else:
            if target.startswith('@'):
                target = target[1:]
            username = target
            try:
                chat = await context.bot.get_chat(f"@{username}")
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
        await msg.reply_text(
            'Не удалось определить пользователя. Убедитесь, что:\n'
            '- вы ответили на сообщение пользователя и вызвали /addadmin, или\n'
            '- указали числовой id, или\n'
            "- указали username и этот пользователь раньше запускал бота (выполнял /start).\n"
            'Если у вас есть numeric id — используйте /addadmin 123456789.'
        )
        return

    if int(target_id) in ADMIN_IDS:
        await msg.reply_text(f'Пользователь id={target_id} уже является админом.')
        return

    ADMIN_IDS.add(int(target_id))
    try:
        save_admins_to_file()
    except Exception:
        log.exception('Failed to persist admins after adding %s', target_id)
    await msg.reply_text(f'Пользователь id={target_id} добавлен в админы.')

async def deladmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /deladmin from non-admin %s', from_id)
        await msg.reply_text('Только админы могут удалять админов.')
        return

    target_id = None
    text = (msg.text or '').strip()

    if msg.reply_to_message and msg.reply_to_message.from_user:
        target_id = int(msg.reply_to_message.from_user.id)
        log.info('Deleting admin via reply: %s', target_id)
    else:
        m = re.match(r'^\s*/deladmin\s+(.+)$', text, flags=re.IGNORECASE)
        if not m:
            await msg.reply_text('Использование: /deladmin <username_or_id>')
            return
        target = m.group(1).strip()
        if re.fullmatch(r'\d+', target):
            target_id = int(target)
        else:
            if target.startswith('@'):
                target = target[1:]
            username = target
            try:
                chat = await context.bot.get_chat(f"@{username}")
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
        await msg.reply_text(
            'Не удалось определить пользователя. Убедитесь, что:\n'
            '- вы ответили на сообщение пользователя и вызвали /deladmin, или\n'
            '- указали числовой id, или\n'
            "- указали username и этот пользователь раньше запускал бота (выполнял /start)."
        )
        return

    if int(target_id) not in ADMIN_IDS:
        await msg.reply_text(f'Пользователь id={target_id} не является админом.')
        return

    if len(ADMIN_IDS) <= 1:
        await msg.reply_text('Нельзя удалить последнего админа — сначала добавьте другого админа.')
        return

    ADMIN_IDS.discard(int(target_id))
    try:
        save_admins_to_file()
    except Exception:
        log.exception('Failed to persist admins after removing %s', target_id)
    await msg.reply_text(f'Пользователь id={target_id} удалён из админов.')

async def listadmins_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /listadmins from non-admin %s', from_id)
        await msg.reply_text('Только админы могут просматривать список админов.')
        return

    if not ADMIN_IDS:
        await msg.reply_text('Список админов пуст.')
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
    await msg.reply_text(text)

async def liststatements_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /liststatements — показывает все открытые и архивные ведомости."""
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /liststatements from non-admin %s', from_id)
        await msg.reply_text('Только админы могут просматривать список ведомостей.')
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
        
        await msg.reply_text(response)
        
    except Exception as e:
        log.exception('Error in liststatements_command')
        await msg.reply_text(f'Ошибка при получении списка ведомостей: {str(e)}')


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


async def archive_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для ручной архивации конкретной ведомости: /archive <название ведомости>"""
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /archive from non-admin %s', from_id)
        await msg.reply_text('Только админы могут архивировать ведомости.')
        return

    # Получаем название ведомости
    if not context.args:
        await msg.reply_text(
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
            await msg.reply_text(f'Ведомость "{statement_name}" не найдена в открытых папках.\nИспользуйте /liststatements для просмотра доступных ведомостей.')
            return
        
        # Проверяем есть ли пользователи этой ведомости в БД
        users_count = count_users_in_statement(target_filename)
        
        await msg.reply_text(f'Начинаю архивацию ведомости "{statement_name}".\nНайдено пользователей в БД: {users_count}')
        
        # Выполняем архивацию
        success = archive_statement_manually(target_filename, statement_folder)
        
        if success:
            # Удаляем пользователей из БД
            removed_count = remove_users_from_statement(target_filename)
            await msg.reply_text(
                f'Ведомость "{statement_name}" успешно заархивирована.\n'
                f'Папка перемещена в архив.\n'
                f'Удалено записей из БД: {removed_count}'
            )
        else:
            await msg.reply_text(f'Ошибка при архивации ведомости "{statement_name}". Проверьте логи.')
            
    except Exception as e:
        log.exception('Error in manual archive command')
        await msg.reply_text(f'Ошибка при архивации: {str(e)}')


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
    """Удаляет всех пользователей указанной ведомости из БД. Ищет по точному совпадению и без учета регистра."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        
        # Сначала пробуем точное совпадение
        c.execute('DELETE FROM vedomosti_users WHERE original_filename = ?', (filename,))
        affected = c.rowcount
        
        # Если не нашли, пробуем без учета регистра
        if affected == 0:
            c.execute('DELETE FROM vedomosti_users WHERE LOWER(original_filename) = LOWER(?)', (filename,))
            affected = c.rowcount
        
        # Если все еще не нашли, пробуем найти по частичному совпадению (без расширения)
        if affected == 0:
            filename_base = os.path.splitext(filename)[0]
            c.execute('DELETE FROM vedomosti_users WHERE LOWER(original_filename) LIKE LOWER(?)', (f'%{filename_base}%',))
            affected = c.rowcount
        
        conn.commit()
        conn.close()
        log.info('Removed %d users for statement %s from database', affected, filename)
        return affected
    except Exception:
        log.exception('Failed to remove users from DB for statement %s', filename)
        return 0

def update_statement_data(statement_folder: str, target_filename: str, new_csv_path: str) -> tuple[bool, list]:
    """Обновляет данные ведомости новым CSV файлом и возвращает список пользователей с изменениями."""
    try:
        def _normalize_value(val):
            if pd.isna(val) or val is None:
                return ''
            s = str(val).strip()
            # Нормализуем пустые строки и "0"
            if not s or s.lower() in ('nan', 'none', '-', '—'):
                return ''
            # Убираем лишние пробелы
            s = ' '.join(s.split())
            return s

        def _get_field(row_dict: dict, field_name: str):
            # Маппинг английских названий на русские альтернативы
            field_aliases = {
                'name': ['Куратор', 'ФИО', 'fio', 'curator', 'ФИО, которое указано в консоли'],
                'type': ['Тип куратора', 'type', 'curator_type'],
                'email': ['Почта', 'mail', 'Email'],
                'phone': ['Телефон', 'phone', 'Номер телефона', 'Номер телефона, который указан в консоли'],
                'console': ['Console', 'console', 'ФИО, которое указано в консоли'],
                'groups': ['Группы', 'groups', 'group'],
                'stud_all': ['Всего учеников', 'Всего детей', 'total_children', 'stud_all'],
                'stud_gk': ['Всего детей - ГК', 'Всего учеников - ГК', 'stud_gk'],
                'stud_gkp': ['Всего учеников - ГК+', 'Всего детей - ГК+', 'stud_gkp'],
                'stud_rep': ['Колво учеников с тарифом с репетитором', 'with_tutor', 'stud_rep'],
                'rep_salary': ['Доплата за учеников с репетитором', 'rep_salary'],
                'base': ['Оклад за ученика', 'salary_per_student', 'base'],
                'stud_salary': ['Сумма оклада', 'salary_sum', 'stud_salary'],
                'stud_salary_gk': ['Сумма оклада ГК', 'stud_salary_gk', 'salary_gk'],
                'stud_salary_gkp': ['Сумма оклада ГК+', 'stud_salary_gkp', 'salary_gkp'],
                'class': ['class', 'Класс', 'курс'],
                'slivs': ['Кол-во сливов', 'slivs'],
                'slivs_gk': ['Кол-во сливов/киков в прошлом блоке ГК', 'slivs_gk'],
                'slivs_gkp': ['Кол-во сливов/киков в прошлом блоке ГК+', 'slivs_gkp'],
                'rr': ['retention', 'Retention', 'rr'],
                'rr_salary': ['Оплата за retention', 'retention_pay', 'rr_salary'],
                'rr_gk': ['retention ГК', 'rr_gk'],
                'rr_salary_gk': ['Оплата за retention ГК', 'rr_salary_gk'],
                'rr_gkp': ['retention ГК+', 'rr_gkp'],
                'rr_salary_gkp': ['Оплата за retention ГК+', 'rr_salary_gkp'],
                'okk': ['okk', 'OKK', 'ОКК'],
                'okk_salary': ['Оплата за okk', 'okk_pay', 'okk_salary'],
                'okk_gk': ['okk ГК', 'OKK ГК', 'okk_gk'],
                'okk_salary_gk': ['Оплата за okk ГК', 'okk_salary_gk'],
                'okk_gkp': ['okk ГК+', 'OKK ГК+', 'okk_gkp'],
                'okk_salary_gkp': ['Оплата за okk ГК+', 'okk_salary_gkp'],
                'kpi_total': ['Сумма КПИ', 'Сумма КПИ (okk+retention)', 'kpi_sum', 'kpi_total'],
                'checks_all': ['Сумма за проверки за всё время', 'checks_calc', 'checks_all'],
                'checks_prev': ['Сумма за проверки в прошлом периоде', 'checks_prev'],
                'checks_salary': ['Сумма к оплате за проверки', 'checks_sum', 'checks_salary'],
                'dop_checks': ['Доп. проверки', 'extra_checks', 'dop_checks'],
                'up': ['УП', 'support', 'up'],
                'chats': ['Чаты', 'chats'],
                'webs': ['Вебы', 'webinars', 'webs'],
                'meth': ['Стол заказов', 'orders_table', 'meth'],
                'dop_sk': ['Премия от СК', 'bonus', 'dop_sk'],
                'callsg': ['Групповые созв.', 'group_calls', 'callsg'],
                'callsp': ['Индивидуальные созвоны', 'individual_calls', 'callsp'],
                'fines': ['Все штрафы', 'Штрафы', 'penalties', 'fines'],
                'total': ['Всего к выплате', 'Итого', 'total', 'Total'],
                'comment': ['Комментарий', 'comment', 'Comment'],
            }
            
            # Прямое совпадение
            if field_name in row_dict:
                return row_dict.get(field_name)
            
            # Поиск по альтернативным названиям
            aliases = field_aliases.get(field_name, [])
            for alias in aliases:
                if alias in row_dict:
                    return row_dict.get(alias)
                # Поиск без учета регистра
                for key in row_dict.keys():
                    if str(key).strip().lower() == alias.lower():
                        return row_dict.get(key)
            
            # Поиск по частичному совпадению названия поля
            target = field_name.strip().lower()
            for key, value in row_dict.items():
                if str(key).strip().lower() == target:
                    return value
            
            return ''

        # Читаем новый CSV файл
        try:
            new_df = pd.read_csv(new_csv_path, dtype=str)
        except Exception:
            try:
                new_df = pd.read_csv(new_csv_path, encoding='cp1251', dtype=str)
            except Exception:
                log.exception('Failed to read new CSV file %s', new_csv_path)
                return False, []
        
        if new_df is None or new_df.empty or 'vk_id' not in new_df.columns:
            log.error('New CSV file is empty or missing vk_id column')
            return False, []
        
        # Находим старый CSV файл в папке ведомости
        old_csv_path = None
        for file in os.listdir(statement_folder):
            if file.endswith('.csv') and 'users' not in file:
                old_csv_path = os.path.join(statement_folder, file)
                break
        
        if not old_csv_path:
            log.error('Old CSV file not found in statement folder %s', statement_folder)
            return False, []
        
        # Читаем старый CSV файл
        try:
            old_df = pd.read_csv(old_csv_path, dtype=str)
        except Exception:
            try:
                old_df = pd.read_csv(old_csv_path, encoding='cp1251', dtype=str)
            except Exception:
                log.exception('Failed to read old CSV file %s', old_csv_path)
                return False, []
        
        if old_df is None or old_df.empty or 'vk_id' not in old_df.columns:
            log.error('Old CSV file is empty or missing vk_id column')
            return False, []
        
        # Создаем словари для быстрого поиска по уникальному ключу (vk_id + groups)
        # Это позволяет поддерживать несколько строк для одного vk_id (например, куратор на разных предметах)
        def make_unique_key(row):
            raw_vk_id = str(row.get('vk_id', '')).strip()
            vk_id = extract_vk_id(raw_vk_id)
            if not vk_id:
                return None
            groups = str(row.get('groups', '')).strip()
            # Используем vk_id + groups как уникальный ключ
            return f"{vk_id}_{groups}" if groups else vk_id
        
        # Функция для нормализации groups (убираем лишние пробелы вокруг запятых)
        def normalize_groups(groups_str):
            if not groups_str:
                return ''
            # Разбиваем по запятой, убираем пробелы, соединяем обратно
            return ','.join([g.strip() for g in str(groups_str).split(',') if g.strip()])
        
        old_data = {}
        vk_id_counters_old = {}  # Счётчик для случаев без groups
        for idx, row in old_df.iterrows():
            raw_vk_id = str(row.get('vk_id', '')).strip()
            vk_id = extract_vk_id(raw_vk_id)
            if not vk_id:
                continue
            groups = str(row.get('groups', '')).strip()
            groups_normalized = normalize_groups(groups)
            if groups_normalized:
                unique_key = f"{vk_id}_{groups_normalized}"
            else:
                # Если groups пустой, используем счётчик
                counter = vk_id_counters_old.get(vk_id, 0)
                unique_key = f"{vk_id}_idx{counter}"
                vk_id_counters_old[vk_id] = counter + 1
            old_data[unique_key] = {'vk_id': vk_id, 'row': row.to_dict(), 'groups': groups}
        
        new_data = {}
        vk_id_counters_new = {}
        for idx, row in new_df.iterrows():
            raw_vk_id = str(row.get('vk_id', '')).strip()
            vk_id = extract_vk_id(raw_vk_id)
            if not vk_id:
                continue
            groups = str(row.get('groups', '')).strip()
            groups_normalized = normalize_groups(groups)
            if groups_normalized:
                unique_key = f"{vk_id}_{groups_normalized}"
            else:
                counter = vk_id_counters_new.get(vk_id, 0)
                unique_key = f"{vk_id}_idx{counter}"
                vk_id_counters_new[vk_id] = counter + 1
            new_data[unique_key] = {'vk_id': vk_id, 'row': row.to_dict(), 'groups': groups}
        
        log.info('Update comparison: old_csv=%s, new_csv=%s', old_csv_path, new_csv_path)
        log.info('Update comparison: old_data has %d entries, new_data has %d entries', len(old_data), len(new_data))
        log.info('Update comparison: old_data keys=%s', list(old_data.keys())[:5])
        log.info('Update comparison: new_data keys=%s', list(new_data.keys())[:5])
        
        # Находим пользователей с изменениями
        updated_users = []  # Список vk_id с изменениями (может содержать дубликаты если у vk_id несколько строк)
        updated_entries = []  # Список (vk_id, groups, new_row) для обновления personal файлов
        users_dir = os.path.join(statement_folder, 'users')
            
            # Сравниваем ключевые поля (исключаем служебные поля)
        key_fields = [
            'name', 'type', 'class', 'email', 'phone', 'console', 'comment',
            'stud_all', 'stud_gk', 'stud_gkp', 'stud_rep', 'rep_salary', 'base', 
            'stud_salary', 'stud_salary_gk', 'stud_salary_gkp',
            'slivs', 'slivs_gk', 'slivs_gkp',
            'rr', 'rr_salary', 'rr_gk', 'rr_salary_gk', 'rr_gkp', 'rr_salary_gkp',
            'okk', 'okk_salary', 'okk_gk', 'okk_salary_gk', 'okk_gkp', 'okk_salary_gkp',
            'kpi_total', 'checks_all', 'checks_prev', 'checks_salary', 'dop_checks',
            'up', 'chats', 'webs', 'meth', 'dop_sk', 'callsg', 'callsp',
            'fines', 'total'
        ]
        
        for unique_key, new_entry in new_data.items():
            vk_id = new_entry['vk_id']
            new_row = new_entry['row']
            groups = new_entry['groups']
            
            old_entry = old_data.get(unique_key)
            
            # Если записи нет в старом файле по unique_key, пробуем найти по vk_id
            if not old_entry:
                # Ищем любую запись с таким же vk_id (на случай если изменились только groups)
                for old_key, old_val in old_data.items():
                    if old_val.get('vk_id') == vk_id:
                        old_entry = old_val
                        log.info('Update: key=%s vk_id=%s found by vk_id match (old_key=%s, new_key=%s)', 
                                unique_key, vk_id, old_key, unique_key)
                        break
            
            # Если записи нет в старом файле - это новая запись, пропускаем
            if not old_entry:
                log.debug('Update: key=%s vk_id=%s is NEW (not in old file), skipping', unique_key, vk_id)
                continue
            
            old_row = old_entry.get('row', {})
            
            # Если old_row пустой, пропускаем
            if not old_row:
                log.debug('Update: key=%s vk_id=%s has empty old_row, skipping', unique_key, vk_id)
                continue
            
            has_changes = False
            changed_field = None
            for field in key_fields:
                old_val = _normalize_value(_get_field(old_row, field))
                new_val = _normalize_value(_get_field(new_row, field))
                if old_val != new_val:
                    has_changes = True
                    changed_field = field
                    log.info('Update: key=%s vk_id=%s field=%s changed: old="%s" new="%s"', unique_key, vk_id, field, old_val[:50] if old_val else '', new_val[:50] if new_val else '')
                    break
            
            if not has_changes and unique_key in list(new_data.keys())[:2]:
                # Логируем первые 2 записи для отладки
                log.info('Update: key=%s vk_id=%s NO changes detected. Sample old_row keys: %s', unique_key, vk_id, list(old_row.keys())[:10])
                log.info('Update: key=%s vk_id=%s Sample new_row keys: %s', unique_key, vk_id, list(new_row.keys())[:10])
                sample_fields = ['total', 'comment', 'stud_gk', 'kpi_total', 'checks_salary']
                for sf in sample_fields:
                    old_v = _normalize_value(_get_field(old_row, sf))
                    new_v = _normalize_value(_get_field(new_row, sf))
                    log.info('Update: key=%s field=%s: old="%s" new="%s" match=%s', unique_key, sf, old_v[:30] if old_v else '', new_v[:30] if new_v else '', old_v == new_v)
            
            if has_changes:
                if vk_id not in updated_users:
                    updated_users.append(vk_id)
                updated_entries.append((vk_id, groups, new_row))
            else:
                log.debug('Update: key=%s vk_id=%s NO changes', unique_key, vk_id)
        
        log.info('Update comparison: checked %d entries, found %d with changes, %d unique users to notify', 
                 len(new_data), len(updated_entries), len(updated_users))
        
        # Обновляем персональные файлы для каждой изменённой записи
        # Также собираем информацию о personal_path для обновления в БД
        updated_personal_paths = []  # Список (vk_id, personal_path) для обновления в БД
        
        for vk_id, groups, new_row in updated_entries:
            # Ищем персональный файл для этой конкретной записи (по vk_id и groups)
            matched_file = None
            all_files_for_vk = []
            
            for file in os.listdir(users_dir):
                    if file.startswith(f"{vk_id}_") and file.endswith('.csv'):
                        file_path = os.path.join(users_dir, file)
                        all_files_for_vk.append(file_path)
                    # Проверяем содержимое файла чтобы найти нужную группу
                    try:
                        df = pd.read_csv(file_path, dtype=str)
                        if not df.empty:
                            file_groups = str(df.iloc[0].get('groups', '')).strip()
                            if file_groups == groups:
                                matched_file = file_path
                                break
                    except Exception:
                        pass
            
            if not matched_file and all_files_for_vk:
                # Если не нашли по группе, берём самый новый файл этого vk_id
                all_files_for_vk.sort(key=lambda x: os.path.getctime(x), reverse=True)
                matched_file = all_files_for_vk[0]
                log.warning('Could not find personal file by groups=%s for vk_id=%s, using newest file', groups, vk_id)
            
            if matched_file:
                    new_personal_df = pd.DataFrame([new_row])
                    try:
                        new_personal_df.to_csv(matched_file, index=False, encoding='utf-8')
                        log.info('Updated personal file for vk_id=%s groups=%s: %s', vk_id, groups, matched_file)
                        updated_personal_paths.append((vk_id, matched_file))
                    except Exception as e:
                        log.exception('Failed to update personal file for vk_id=%s groups=%s', vk_id, groups)
        
        # Заменяем основной CSV файл
        try:
            shutil.copy2(new_csv_path, old_csv_path)
            log.info('Replaced main CSV file: %s', old_csv_path)
        except Exception:
            log.exception('Failed to replace main CSV file %s', old_csv_path)
            return False, []
        
        # Обновляем записи в БД - сбрасываем согласованность ТОЛЬКО для конкретных записей (по personal_path)
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30)
            c = conn.cursor()
            
            reset_count = 0
            for vk_id, personal_path in updated_personal_paths:
                # Сбрасываем статус только для записи с этим personal_path
                c.execute('''UPDATE vedomosti_users 
                             SET status = NULL, disagree_reason = NULL, confirmed_at = NULL 
                             WHERE personal_path = ?''',
                         (personal_path,))
                affected = c.rowcount
                log.info('Reset agreement status for personal_path=%s (affected rows: %d)', personal_path, affected)
                
                # Если не нашли по personal_path, пробуем по vk_id + original_filename + LIKE personal_path
                if affected == 0:
                    c.execute('''UPDATE vedomosti_users 
                                 SET status = NULL, disagree_reason = NULL, confirmed_at = NULL 
                                 WHERE vk_id = ? AND original_filename = ? AND personal_path LIKE ?''',
                             (vk_id, target_filename, f'%{os.path.basename(personal_path)}%'))
                    affected = c.rowcount
                    log.info('Second attempt: Reset for vk_id=%s path_like=%s (affected rows: %d)', vk_id, os.path.basename(personal_path), affected)
                
                if affected > 0:
                    reset_count += affected
            
            conn.commit()
            conn.close()
            log.info('Reset agreement status in database for %d entries', reset_count)
        except Exception:
            log.exception('Failed to reset agreement status in database')
        
        return True, updated_users
        
    except Exception:
        log.exception('Failed to update statement data')
        return False, []

def send_update_notifications(updated_users: list, statement_name: str):
    """Отправляет уведомления пользователям об обновлении данных в ведомости."""
    if not updated_users:
        return
    
    message = ("Данные в Вашей выплате были обновлены. Для просмотра нажмите на кнопку 'К списку выплат' -> выплата")
    
    # Формируем JSON клавиатуры
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
    
    # Отправляем уведомления
    sent = 0
    failed = 0
    
    for vk_id in updated_users:
        try:
            success = send_vk_message(str(vk_id), message, keyboard_json)
            if success:
                sent += 1
                log.info('Sent update notification to vk_id=%s', vk_id)
            else:
                failed += 1
                log.warning('Failed to send update notification to vk_id=%s', vk_id)
        except Exception:
            log.exception('Exception sending update notification to vk_id=%s', vk_id)
            failed += 1
    
    log.info('Update notifications sent: success=%d, failed=%d', sent, failed)


def find_archived_statement(statement_name: str) -> tuple:
    """Находит архивную ведомость по названию. Возвращает (statement_folder, target_filename) или (None, None)"""
    archive_path = os.path.join(HOSTING_ROOT, ARCHIVE_DIRNAME)
    
    if not os.path.exists(archive_path):
        return None, None
    
    # Очищаем название от возможных символов маркера списка
    
            # Ищем точное совпадение
        # Очищаем название от возможных символов маркера списка
    statement_name = statement_name.strip().lstrip('•').strip()
    # Убираем расширение .csv если оно есть
    if statement_name.endswith('.csv'):
        statement_name = statement_name[:-4]
    
    # Ищем только точное совпадение (без учета расширения файла и регистра)
    for root, dirs, files in os.walk(archive_path):
        if 'users' in root:
            continue
        for file in files:
            if file.endswith('.csv'):
                file_base = os.path.splitext(file)[0]
                # Точное совпадение без учета регистра
                if file_base.lower() == statement_name.lower():
                    return root, file

    return None, None


async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для удаления архивных ведомостей: /delete <название1> <название2> ... или многострочный список"""
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /delete from non-admin %s', from_id)
        await msg.reply_text('Только админы могут удалять ведомости.')
        return
        
    # Получаем список ведомостей для удаления
    if not context.args and not msg.text:
        await msg.reply_text(
            'Укажите название ведомости(ей) для удаления.\n'
            'Использование: /delete <название1> <название2> ...\n'
            'Пример: /delete Хим_Катя_ГК_1блок Физика_ОГЭ_ПГК_3\n\n'
            'Можно указать несколько ведомостей через пробел.\n'
            'Также можно вставить список с символами • (маркеры списка будут автоматически удалены).\n\n'
            'Для просмотра доступных ведомостей используйте /liststatements'
        )
        return
    
    # Получаем текст команды (все после /delete)
    command_text = msg.text or ''
    if command_text.startswith('/delete'):
        command_text = command_text[7:].strip()  # Убираем "/delete"
    
    # Если есть аргументы из context.args, используем их
    if context.args:
        statement_names = context.args
    else:
        # Парсим текст - разбиваем по строкам и убираем пустые
        lines = [line.strip() for line in command_text.split('\n') if line.strip()]
        statement_names = []
        for line in lines:
            # Убираем маркеры списка (•, -, *, и пробелы в начале) и очищаем
            cleaned_line = line.lstrip('•').lstrip('-').lstrip('*').lstrip().strip()
            # Пропускаем пустые строки или строки только с маркерами
            if not cleaned_line or cleaned_line == '•' or cleaned_line == '-' or cleaned_line == '*':
                continue
            # Если строка содержит .csv, оставляем как есть, иначе разбиваем по пробелам
            if '.csv' in cleaned_line:
                # Убираем .csv если есть (будем искать с ним и без)
                name_without_ext = cleaned_line.replace('.csv', '').strip()
                if name_without_ext:
                    statement_names.append(name_without_ext)
            else:
                # Разбиваем на отдельные названия (если в строке несколько)
                parts = cleaned_line.split()
                statement_names.extend([p for p in parts if p and p not in ('•', '-', '*')])
    
    if not statement_names:
        await msg.reply_text('Не указаны ведомости для удаления.')
        return
    statement_names = [name for name in dict.fromkeys(statement_names) if name and name.strip()]

    total_to_delete = len(statement_names)
    await msg.reply_text(f'Начинаю удаление {total_to_delete} архивных ведомостей...')

    archive_path = os.path.join(HOSTING_ROOT, ARCHIVE_DIRNAME)
    results = {
        'success': [],
        'not_found': [],
        'errors': []
    }

    try:
        for statement_name in statement_names:
            statement_folder, target_filename = find_archived_statement(statement_name)
            
            if not statement_folder or not target_filename:
                results['not_found'].append(statement_name)
                log.warning('Archived statement not found: %s', statement_name)
                continue
            
            try:
                # Подсчитываем количество пользователей в БД
                users_count = count_users_in_statement(target_filename)  # подсчитываем количество пользователей в БД
                
                # Удаляем папку с ведомостью
                shutil.rmtree(statement_folder)
                log.info('Deleted archived statement folder: %s', statement_folder)
            
            # Удаляем записи из БД
                removed_count = remove_users_from_statement(target_filename)
                
                results['success'].append({
                            'name': statement_name,
                            'filename': target_filename,
                            'folder': statement_folder,
                            'users_count': users_count,
                            'removed_count': removed_count
                        })
                    
            except Exception as e:
                log.exception('Failed to delete archived statement %s: %s', statement_name, e)
                results['errors'].append({
                    'name': statement_name,
                    'error': str(e)
            })
    # Удаляем дубликаты, сохраняя порядок, и фильтруем пустые значения

        
        # Формируем итоговый отчет
        report_lines = [f'Удаление завершено. Обработано: {total_to_delete} ведомостей\n']
        
        if results['success']:
            report_lines.append(f'\nУспешно удалено: {len(results["success"])}')
            for item in results['success']:
                removed_info = f'удалено записей из БД: {item["removed_count"]}'
                if item["removed_count"] == 0:
                    removed_info += ' (записей не было в БД или уже удалены)'
                report_lines.append(f'  • {item["name"]} ({removed_info})')
        
        if results['not_found']:
            report_lines.append(f'\nНе найдено: {len(results["not_found"])}')
            for name in results['not_found']:
                report_lines.append(f'  • {name}')
        
        if results['errors']:
            report_lines.append(f'\nОшибки при удалении: {len(results["errors"])}')
            for item in results['errors']:
                report_lines.append(f'  • {item["name"]}: {item["error"]}')
        
        report = '\n'.join(report_lines)
        
        # Telegram ограничивает длину сообщений до 4096 символов
        if len(report) > 4000:
            report = report[:4000] + '\n...(отчет обрезан)'
        
        await msg.reply_text(report)
        
    except Exception as e:
        log.exception('Error in delete command')
        await msg.reply_text(f'Критическая ошибка при удалении: {str(e)}')

async def update_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для обновления ведомости: /update <название ведомости>"""
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /update from non-admin %s', from_id)
        await msg.reply_text('Только админы могут обновлять ведомости.')
        return

    # Получаем название ведомости
    if not context.args:
        await msg.reply_text(
            'Укажите название ведомости для обновления.\n'
            'Использование: /update <название ведомости>\n'
            'Пример: /update Русский ОГЭ ПГК\n\n'
            'Для просмотра доступных ведомостей используйте /liststatements'
        )
        return
    
    statement_name = ' '.join(context.args).strip()
    
    try:
        # Ищем ведомость в открытых папках
        statement_folder = find_statement_folder(statement_name + '.csv')
        if not statement_folder:
            # Пробуем гибкий поиск
            statement_folder = find_statement_folder_flexible(statement_name)
            if statement_folder:
                # Находим точное имя файла
                for file in os.listdir(statement_folder):
                    if file.endswith('.csv'):
                        target_filename = file
                        break
            else:
                await msg.reply_text(f'Ведомость "{statement_name}" не найдена в открытых папках.\nИспользуйте /liststatements для просмотра доступных ведомостей.')
                return
        else:
            target_filename = statement_name + '.csv'
        
        # Проверяем есть ли ожидающий файл для обновления
        cur = load_current_for_user(from_id)
        file_path = cur.get('file_path', '')
        if not file_path:
            await msg.reply_text('Нет ожидающего файла для обновления. Сначала пришлите новый файл (CSV/XLSX).')
            return
        
        # Конвертируем файл в CSV
        csv_path = ensure_csv(file_path)
        if not csv_path:
            await msg.reply_text(f'Не удалось обработать файл {file_path} (чтение/конвертация).')
            return
        
        # Выполняем обновление
        success, updated_users = update_statement_data(statement_folder, target_filename, csv_path)
        
        if success:
            # Очищаем ожидающий файл
            save_current_for_user(from_id, file_path='', awaiting_meta=False)
            
            # Отправляем уведомления пользователям об обновлении
            if updated_users:
                send_update_notifications(updated_users, statement_name)
            
            await msg.reply_text(
                f'Ведомость "{statement_name}" успешно обновлена.\n'
                f'Обновлено пользователей: {len(updated_users)}\n'
                f'Уведомления отправлены пользователям с изменениями.'
            )
        else:
            await msg.reply_text(f'Ошибка при обновлении ведомости "{statement_name}". Проверьте логи.')
            
    except Exception as e:
        log.exception('Error in update command')
        await msg.reply_text(f'Ошибка при обновлении: {str(e)}')

async def find_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для поиска ведомостей по VK ID: /find https://vk.com/id160898445"""
    msg = update.message
    from_id = msg.from_user.id
    if not is_admin(from_id):
        log.info('Ignoring /find from non-admin %s', from_id)
        await msg.reply_text('Только админы могут использовать эту команду.')
        return

    # Получаем аргумент (URL или VK ID)
    if not context.args:
        await msg.reply_text(
            'Укажите VK ID или ссылку на профиль.\n'
            'Использование: /find https://vk.com/id160898445\n'
            'Или: /find 160898445\n\n'
            'Команда выведет все ведомости пользователя с их статусами согласования.'
        )
        return
    
    # Парсим VK ID из аргумента
    vk_id_arg = ' '.join(context.args).strip()
    vk_id = None
    
    # Пробуем извлечь ID из URL
    url_patterns = [
        r'https?://vk\.com/id(\d+)',
        r'https?://vk\.com/(\d+)',
        r'vk\.com/id(\d+)',
        r'vk\.com/(\d+)',
    ]
    
    for pattern in url_patterns:
        match = re.search(pattern, vk_id_arg)
        if match:
            vk_id = match.group(1)
            break
    
    # Если не нашли в URL, пробуем как прямой ID
    if not vk_id:
        if vk_id_arg.isdigit():
            vk_id = vk_id_arg
        else:
            await msg.reply_text(
                f'Не удалось извлечь VK ID из "{vk_id_arg}".\n'
                'Использование: /find https://vk.com/id160898445\n'
                'Или: /find 160898445'
            )
            return
    
    try:
        # Получаем все ведомости пользователя из БД
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        c.execute("""
            SELECT original_filename, status, created_at
            FROM vedomosti_users 
            WHERE vk_id = ? AND state LIKE 'imported:%'
            ORDER BY created_at DESC, id DESC
        """, (str(vk_id),))
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            await msg.reply_text(f'Ведомости для пользователя VK ID {vk_id} не найдены.')
            return
        
        # Форматируем результат
        result_lines = [f'Ведомости для пользователя VK ID: {vk_id}\n']
        result_lines.append(f'Всего ведомостей: {len(rows)}\n')
        result_lines.append('─' * 40)
        
        for idx, (original_filename, status, created_at) in enumerate(rows, 1):
            # Убираем расширение .csv из названия
            filename_display = original_filename.replace('.csv', '') if original_filename else 'Без названия'
            
            # Определяем статус согласования
            if status == 'agreed':
                status_text = ' Согласовано'
            elif status == 'disagreed':
                status_text = ' Не согласовано'
            elif status:
                status_text = f' {status}'
            else:
                status_text = ' Ожидает согласования'
            
            result_lines.append(f'\n{idx}. {filename_display}')
            result_lines.append(f'   Статус: {status_text}')
        
        result_text = '\n'.join(result_lines)
        
        # Разбиваем на части, если сообщение слишком длинное
        max_length = 4000  # Лимит Telegram
        if len(result_text) > max_length:
            # Отправляем первую часть
            first_part = result_text[:max_length]
            last_newline = first_part.rfind('\n')
            if last_newline > 0:
                first_part = first_part[:last_newline]
            await msg.reply_text(first_part)
            
            # Отправляем оставшуюся часть
            remaining = result_text[last_newline+1:]
            while len(remaining) > max_length:
                chunk = remaining[:max_length]
                last_newline = chunk.rfind('\n')
                if last_newline > 0:
                    chunk = chunk[:last_newline]
                await msg.reply_text(chunk)
                remaining = remaining[len(chunk)+1:]
            if remaining:
                await msg.reply_text(remaining)
        else:
            await msg.reply_text(result_text)
            
        log.info('Find command executed by %s for VK ID %s, found %d statements', from_id, vk_id, len(rows))
        
    except Exception as e:
        log.exception('Error in find command')
        await msg.reply_text(f'Ошибка при поиске: {str(e)}')

async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Неизвестная команда. Используйте /start, пришлите файл или /send <предмет> <тип курса> <блок>.')

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
    
    # Create application with bot token
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    try:
        commands = [
            BotCommand('start', 'Знакомство'),
            BotCommand('description', 'Показать описание процесса загрузки'),
            BotCommand('send', 'Отправить файл на хостинг: /send <предмет> <тип курса> <блок>'),
            BotCommand('send_repet', 'Отправить файл репетиторов: /send_repet <предмет> <тип курса> <блок>'),
            BotCommand('update', 'Обновить ведомость: /update <название ведомости>'),
            BotCommand('notify', 'Разослать уведомление vk_id из БД'),
            BotCommand('notify_repet', 'Разослать уведомление репетиторам (VK из столбца ВК)'),
            BotCommand('liststatements', 'Показать список открытых и архивных ведомостей'),
            BotCommand('find', 'Поиск ведомостей по VK ID: /find https://vk.com/id160898445'),
            BotCommand('addadmin', 'Добавить админа: /addadmin <username_or_id>'),
            BotCommand('deladmin', 'Удалить админа: /deladmin <username_or_id>'),
            BotCommand('listadmins', 'Показать список текущих админов'),
            BotCommand('archive', 'Переместить ведомость в архив: /archive <название>'),
            BotCommand('delete', 'Удалить архивные ведомости: /delete <название1> <название2> ...')
        ]
        application.bot.set_my_commands(commands)
        log.info('Bot commands (menu) set: %s', [c.command for c in commands])
    except Exception as e:
        log.exception('Failed to set bot commands: %s', e)

    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('description', description))
    application.add_handler(CommandHandler('send', send_command))
    application.add_handler(CommandHandler('send_repet', send_repet_command))
    application.add_handler(CommandHandler('update', update_command))
    application.add_handler(CommandHandler('notify', notify_command))
    application.add_handler(CommandHandler('notify_repet', notify_repet_command))
    application.add_handler(CommandHandler('send_keyboard', send_keyboard_command))
    application.add_handler(CommandHandler('liststatements', liststatements_command))
    application.add_handler(CommandHandler('find', find_command))
    application.add_handler(CommandHandler('addadmin', addadmin_command))
    application.add_handler(CommandHandler('deladmin', deladmin_command))
    application.add_handler(CommandHandler('listadmins', listadmins_command))
    application.add_handler(CommandHandler('archive', archive_command))
    application.add_handler(CommandHandler('delete', delete_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(MessageHandler(filters.COMMAND, unknown))

    log.info('Telegram payroll hosting bot started (DRY_RUN=%s)', DRY_RUN)
    application.run_polling()

if __name__ == '__main__':
    run_bot()
