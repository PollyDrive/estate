# Извлечение и приоритизация локаций

## Дата: 2025-11-12

## Обзор

Добавлена возможность извлекать локацию из описания объявления и сохранять в отдельную колонку `location_extracted`. Это позволит:
1. Точно знать где находится объявление
2. Сравнивать с разрешенными локациями (`allowed_locations`)
3. Выставлять приоритеты для сортировки
4. Анализировать распределение по локациям

---

## База данных

### Новые колонки

**listings table:**
```sql
location_extracted VARCHAR(100)  -- Извлеченная локация (Ubud, Canggu, etc)
priority INTEGER                 -- Приоритет (выше = лучше)
```

**Индексы:**
```sql
idx_listings_location_extracted  -- Для фильтрации по локации
idx_listings_priority            -- Для сортировки по приоритету
```

---

## Извлечение локации

### PropertyParser.extract_location()

**Метод:** `parser.extract_location(text: str) -> Optional[str]`

**Известные локации (40+ мест):**
- **Приоритетные:** Ubud, Abiansemal, Singakerta, Mengwi, Gianyar
- **Популярные:** Canggu, Seminyak, Kuta, Sanur, Denpasar
- **Другие:** Pererenan, Berawa, Umalas, Tegallalang, и т.д.

### Паттерны поиска

**Приоритет 1: Явные указания**
```
"in Ubud"           → Ubud
"at Canggu"         → Canggu
"di Seminyak"       → Seminyak (Indonesian)
"Ubud area"         → Ubud
"Canggu location"   → Canggu
```

**Приоритет 2: Упоминание в начале**
```
"Ubud - Beautiful villa..."  → Ubud
"
Canggu\nVilla 2BR..."        → Canggu
```

### Примеры

```python
# Пример 1: Явное указание
text = "Beautiful 2BR villa in Ubud with pool"
location = parser.extract_location(text)
# → "Ubud"

# Пример 2: Indonesian
text = "Villa cantik di Canggu, 2 kamar"
location = parser.extract_location(text)
# → "Canggu"

# Пример 3: Начало строки
text = """
Ubud
Villa 2BR monthly rent
Pool, kitchen, AC
"""
location = parser.extract_location(text)
# → "Ubud"

# Пример 4: Не найдено
text = "Beautiful villa near Ubud"  # "near" не подходит
location = parser.extract_location(text)
# → None
```

---

## Обработка на Stage 2

На Stage 2 для каждого объявления:

1. **Извлекается локация:**
   ```python
   location_extracted = parser.extract_location(description)
   ```

2. **Сохраняется в базу:**
   ```python
   update_details = {
       ...
       'location_extracted': location_extracted,
       ...
   }
   ```

3. **Логируется:**
   ```
   [STAGE 2] Processing 123456: Location extracted: Ubud
   ```

---

## Приоритизация (будущее)

### Колонка priority

**Значения:**
- `NULL` - не рассчитан
- `1-5` - низкий приоритет (нежелательные локации)
- `6-8` - средний приоритет (нейтральные)
- `9-10` - высокий приоритет (allowed_locations)

### Алгоритм (планируется)

```python
def calculate_priority(location_extracted, config):
    allowed_locations = config['criterias']['allowed_locations']
    
    if not location_extracted:
        return 5  # Неизвестная локация - средний приоритет
    
    if location_extracted in allowed_locations:
        return 10  # Топ приоритет!
    
    # Можно добавить расстояние, популярность и т.д.
    return 7  # Нейтральная локация
```

### Использование

**Сортировка в Telegram:**
```python
# Отправлять сначала с высоким приоритетом
listings = db.get_listings_for_telegram()
listings_sorted = sorted(listings, key=lambda x: x['priority'], reverse=True)
```

**Фильтрация:**
```python
# Показать только топ локации
SELECT * FROM listings 
WHERE priority >= 9 
  AND status = 'stage4'
ORDER BY priority DESC, created_at DESC;
```

---

## Статистика по локациям

### SQL запросы

**Распределение по локациям:**
```sql
SELECT 
    location_extracted,
    COUNT(*) as count,
    AVG(price_extracted) as avg_price,
    AVG(bedrooms) as avg_bedrooms
FROM listings
WHERE location_extracted IS NOT NULL
GROUP BY location_extracted
ORDER BY count DESC;
```

**Топ локации:**
```sql
SELECT location_extracted, COUNT(*) as count
FROM listings
WHERE status = 'stage5_sent'
GROUP BY location_extracted
ORDER BY count DESC
LIMIT 10;
```

**Пропущенные локации:**
```sql
SELECT COUNT(*) as no_location_count
FROM listings
WHERE location_extracted IS NULL
  AND status IN ('stage2', 'stage3', 'stage4');
```

---

## Примеры улучшений

### 1. Добавить новую локацию

Если нужна новая локация (например, "Pecatu"):

```python
# src/property_parser.py
self.known_locations = [
    ...
    'Pecatu',  # Добавить сюда
    ...
]
```

### 2. Улучшить паттерны

Добавить новые паттерны поиска:

```python
patterns = [
    ...
    rf'{re.escape(location_lower)}\s+villa\b',  # "Ubud villa"
    rf'near\s+{re.escape(location_lower)}\b',   # "near Ubud"
]
```

### 3. Расстояние до локации

Извлекать расстояние:
```
"15 minutes from Ubud" → location="Ubud", distance=15
```

---

## Следующие шаги

1. ✅ Добавлена колонка `location_extracted`
2. ✅ Добавлена колонка `priority`
3. ✅ Реализовано извлечение локации
4. ⏳ Реализовать расчет priority
5. ⏳ Использовать priority для сортировки в Telegram
6. ⏳ Добавить статистику по локациям в админку

---

## Файлы изменены

- `db/migration_add_location_extracted.sql` - миграция БД
- `src/property_parser.py` - метод `extract_location()`
- `scripts/run_stage2_manual.py` - извлечение и сохранение
- `src/main.py` - извлечение и сохранение
