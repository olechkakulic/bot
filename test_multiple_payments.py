#!/usr/bin/env python3
"""
Тест обработки множественных ведомостей для одного пользователя
"""
import sqlite3
import os
import json

DB_PATH = "hosting.db"

def test_multiple_payments_per_user():
    """Проверяем как бот обрабатывает несколько ведомостей у одного пользователя"""
    print("Тестирование множественных ведомостей для одного пользователя\n")
    
    if not os.path.exists(DB_PATH):
        print("❌ База данных не найдена")
        return
    
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        c = conn.cursor()
        
        # 1. Находим пользователей с множественными ведомостями
        c.execute("""
            SELECT vk_id, COUNT(*) as count, GROUP_CONCAT(original_filename) as files
            FROM vedomosti_users 
            WHERE state LIKE 'imported:%'
            GROUP BY vk_id
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        """)
        
        multi_users = c.fetchall()
        
        print("1. ПОЛЬЗОВАТЕЛИ С МНОЖЕСТВЕННЫМИ ВЕДОМОСТЯМИ:")
        print("="*60)
        
        if not multi_users:
            print("❌ Нет пользователей с множественными ведомостями для тестирования")
            
            # Показываем всех пользователей
            c.execute("""
                SELECT vk_id, COUNT(*) as count, GROUP_CONCAT(original_filename) as files
                FROM vedomosti_users 
                WHERE state LIKE 'imported:%'
                GROUP BY vk_id
                ORDER BY count DESC
            """)
            all_users = c.fetchall()
            
            print("\nВСЕ ПОЛЬЗОВАТЕЛИ В БД:")
            for vk_id, count, files in all_users:
                print(f"  VK ID {vk_id}: {count} ведомостей ({files})")
            
            conn.close()
            return
        
        # Анализируем каждого пользователя с множественными ведомостями
        for vk_id, count, files in multi_users[:3]:  # Берем первых 3
            print(f"VK ID: {vk_id}")
            print(f"Количество ведомостей: {count}")
            print(f"Файлы: {files}")
            
            # Детальная информация по каждой ведомости
            c.execute("""
                SELECT id, original_filename, state, status, created_at
                FROM vedomosti_users 
                WHERE vk_id = ? AND state LIKE 'imported:%'
                ORDER BY created_at DESC
            """, (vk_id,))
            
            user_payments = c.fetchall()
            
            print("  Детали ведомостей:")
            for i, (db_id, filename, state, status, created_at) in enumerate(user_payments, 1):
                # Извлекаем payment_id из state
                payment_id = state.split(':', 1)[1] if ':' in state else 'unknown'
                print(f"    {i}. {filename}")
                print(f"       Payment ID: {payment_id[:8]}...")
                print(f"       Status: {status or 'new'}")
                print(f"       DB ID: {db_id}")
                print(f"       Created: {created_at}")
            print()
        
        # 2. Проверяем уникальность payment_id
        print("2. ПРОВЕРКА УНИКАЛЬНОСТИ PAYMENT_ID:")
        print("="*60)
        
        c.execute("""
            SELECT state, COUNT(*) as count
            FROM vedomosti_users 
            WHERE state LIKE 'imported:%'
            GROUP BY state
            HAVING COUNT(*) > 1
        """)
        
        duplicate_states = c.fetchall()
        
        if duplicate_states:
            print("❌ НАЙДЕНЫ ДУБЛИРУЮЩИЕСЯ PAYMENT_ID:")
            for state, count in duplicate_states:
                payment_id = state.split(':', 1)[1] if ':' in state else state
                print(f"  Payment ID {payment_id[:8]}... используется {count} раз")
        else:
            print("✅ Все payment_id уникальны")
        
        # 3. Проверяем логику фильтрации
        print("\n3. ТЕСТ ЛОГИКИ GET_ALL_PAYMENTS_FOR_USER_FROM_DB:")
        print("="*60)
        
        if multi_users:
            test_user_id = multi_users[0][0]  # Берем первого пользователя
            
            c.execute("""
                SELECT id, vk_id, personal_path, original_filename, state, status, disagree_reason, confirmed_at, created_at 
                FROM vedomosti_users 
                WHERE vk_id = ? AND state LIKE 'imported:%'
                ORDER BY created_at DESC, id DESC
                LIMIT 100
            """, (str(test_user_id),))
            
            rows = c.fetchall()
            
            print(f"Пользователь {test_user_id}:")
            print(f"Найдено записей в БД: {len(rows)}")
            
            # Симулируем НОВУЮ логику функции
            payments = []
            
            for db_row in rows:
                db_id, vk_id_raw, personal_path, original_filename, state, status_db, disagree_reason_db, confirmed_at_db, created_at_db = db_row
                
                if not state or not state.startswith('imported:'):
                    continue
                
                parts = state.split(':', 1)
                if len(parts) != 2 or not parts[1]:
                    continue
                
                payment_id = parts[1]
                
                # НОВАЯ ЛОГИКА: Создаем уникальный payment_id на основе db_id
                unique_payment_id = f"{payment_id}_{db_id}" if payment_id else f"payment_{db_id}"
                
                payments.append({
                    "id": unique_payment_id,
                    "filename": original_filename,
                    "status": status_db or "new", 
                    "created_at": created_at_db,
                    "db_id": db_id,
                    "original_payment_id": payment_id
                })
            
            print(f"Уникальных ведомостей после обработки: {len(payments)}")
            print("Список ведомостей:")
            for i, p in enumerate(payments, 1):
                print(f"  {i}. {p['filename']} - {p['status']} ({p['id'][:8]}...)")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_multiple_payments_per_user()
    print(f"\n📋 ВОЗМОЖНЫЕ ПРОБЛЕМЫ:")
    print(f"1. Дублирующиеся payment_id могут вызывать конфликты")
    print(f"2. Неправильная сортировка может показывать старые ведомости")
    print(f"3. Фильтр по статусу может скрывать новые ведомости")
    print(f"4. Проблемы с кэшированием в памяти vs данные из БД")
    print(f"\n🔍 ПРОВЕРЬТЕ логи выше для выявления конкретной проблемы")