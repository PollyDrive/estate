# Улучшения фильтров Stage 1 и Stage 2

## Дата: 2025-11-12

## Изменения

### 1. **Stop Locations - более точная фильтрация**

**Было:**
```json
"stop_locations": ["Kuta", "Uluwatu", "Seminyak", ...]
```
Отклоняло объявления где упоминались эти локации в **любом контексте**, включая:
- "30 minutes from Seminyak"
- "Close to Kuta beach"

**Стало:**
```json
"stop_locations": ["in Kuta", "in Uluwatu", "in Seminyak", ...]
```
Отклоняет только если объявление **находится** в этих локациях:
- ✅ Пропускает: "30 minutes from Seminyak"
- ❌ Отклоняет: "Villa in Seminyak"

---

### 2. **Price Max - вынесен в конфиг и поднят до 16M**

**Было:**
- Захардкожен в коде: `max_price = 14000000`

**Стало:**
```json
"criterias": {
  "price_max": 16000000,
  "bedrooms_min": 2
}
```
- Максимальная цена: **16M IDR** (было 14M)
- Вынесено в конфиг для простого изменения
- Используется через `criteria.get('price_max', 16000000)`

---

### 3. **Bedrooms - гибкая логика по этапам**

**Stage 1 (Groups):**
```python
# Отклоняет только 1BR (слишком мало)
if bedrooms == 1:
    return False, "Bedrooms: 1 (need 2+)"
```
- ✅ Пропускает: не указано, 2BR, 3BR, 4BR
- ❌ Отклоняет: 1BR (студии проходят, если не указано)

**Stage 2 (Detailed):**
```python
# Требует >= 2 BR
bedrooms_min = criteria.get('bedrooms_min', 2)
if bedrooms < bedrooms_min:
    return False, f"Bedrooms: {bedrooms} (need {bedrooms_min}+)"
```
- ✅ Пропускает: 2BR, 3BR, 4BR+
- ❌ Отклоняет: 0BR (studio), 1BR

---

### 4. **Kitchen - обязательная проверка на Stage 2**

**Было:**
```python
# Закомментировано
# if not params.get('has_kitchen'):
#     return False, "No kitchen mentioned"
```

**Стало:**
```python
# Stage 2: Kitchen required
if stage == 2:
    if not params.get('has_kitchen'):
        return False, "No kitchen mentioned"
```

Ищет упоминания:
- kitchen, kitchenette, dapur (из config.json)

---

### 5. **Stop Words - убраны ложные срабатывания**

**Удалены:**
- `"only room"` - могло быть частью нормального предложения
- `"1 bathroom"` - отклоняло 2BR с одним санузлом
- `"1 bath"` - аналогично
- `"1 KM"` - могло означать расстояние

**Оставлены точные:**
- `"only room"` → осталось, так как это действительно комната
- Но теперь используются более контекстные проверки

---

## Новая логика фильтрации

### Stage 1: Мягкие фильтры
**Цель:** Отсечь явно нерелевантное, пропустить потенциально подходящее

✅ **Пропускает:**
- Объявления без указания спален (будет проверено на Stage 2)
- Объявления без упоминания кухни (будет проверено на Stage 2)
- Объявления с ценой до 16M IDR
- Упоминания нежелательных локаций в контексте расстояния

❌ **Отклоняет:**
- 1BR (точно не подходит)
- daily/weekly rental
- Цена > 16M IDR
- Находится в нежелательной локации ("in Seminyak")
- Stop words (tanah, shop, commercial и т.д.)

### Stage 2: Жесткие фильтры
**Цель:** Проверить детали в полном description

✅ **Пропускает:**
- 2+ BR
- Есть упоминание кухни
- Цена до 16M IDR
- Нет detailed stop words (minimum lease X years)

❌ **Отклоняет:**
- < 2 BR (0BR studio, 1BR)
- Нет упоминания кухни
- Detailed stop words
- Цена > 16M IDR

---

## Пример работы фильтров

### Объявление 1: "Villa 30 min from Seminyak, 2BR, 12jt/month"
- **Stage 1:** ✅ PASS (упоминание Seminyak в контексте расстояния)
- **Stage 2:** Нужно проверить описание на кухню

### Объявление 2: "Beautiful villa in Seminyak, 2BR, 15jt"
- **Stage 1:** ❌ REJECT (находится "in Seminyak")

### Объявление 3: "Cozy room for rent, 1BR, 5jt"
- **Stage 1:** ❌ REJECT (1BR)

### Объявление 4: "Villa 3BR Ubud, pool, 14jt monthly"
- **Stage 1:** ✅ PASS (3BR, цена подходит)
- **Stage 2:** Если есть кухня → ✅ PASS

### Объявление 5: "Studio in Ubud, 8jt, no kitchen"
- **Stage 1:** ✅ PASS (спальни не указаны)
- **Stage 2:** ❌ REJECT (no kitchen в description)

---

## Конфигурация

Все параметры в `config/config.json`:

```json
{
  "criterias": {
    "bedrooms_min": 2,
    "price_max": 16000000
  },
  "filters": {
    "stop_locations": ["in Kuta", "in Uluwatu", ...],
    "required_words": ["kitchen", "kitchenette", "dapur"]
  }
}
```

Изменение параметров не требует изменения кода!
