#!/usr/bin/env python3
"""
Скрипт для исправления дублирующихся состояний в таблице vedomosti_users.
Обновляет все записи с неуникальными state на уникальные UUID.
"""

import sqlite3
import uuid
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

DB_PATH = 'hosting.db'

def fix_duplicate_states():
    """Исправляет дублирующиеся state записи, делая их уникальными."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        
        # Находим все записи, которые имеют дублирующиеся state
        c.execute("""
            SELECT id, vk_id, state 
            FROM vedomosti_users 
            WHERE state LIKE 'imported:%' 
            AND state NOT LIKE 'imported:%-%-%-%-%'  -- Исключаем уже уникальные UUID
            ORDER BY id
        """)
        
        duplicate_records = c.fetchall()
        
        if not duplicate_records:
            log.info("No duplicate state records found")
            return
        
        log.info(f"Found {len(duplicate_records)} records with non-unique states")
        
        # Обновляем каждую запись с уникальным UUID
        updated_count = 0
        for db_id, vk_id, old_state in duplicate_records:
            # Создаем новое уникальное состояние
            new_state = f"imported:{uuid.uuid4()}"
            
            # Обновляем запись
            c.execute(
                "UPDATE vedomosti_users SET state = ? WHERE id = ?",
                (new_state, db_id)
            )
            
            log.info(f"Updated record id={db_id} vk_id={vk_id}: {old_state} -> {new_state}")
            updated_count += 1
        
        conn.commit()
        conn.close()
        
        log.info(f"Successfully updated {updated_count} records with unique states")
        
    except Exception:
        log.exception("Failed to fix duplicate states")

if __name__ == "__main__":
    fix_duplicate_states()