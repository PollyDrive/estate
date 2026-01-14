# Оптимизация схемы базы данных и обработки групп

## Дата: 2025-11-12

## Изменения в обработке Facebook Groups

### Stage 1: Сохранение только description

**Раньше:**
- `title` = полный текст поста
- `description` = полный текст поста (дубликат)

**Теперь:**
- `title` = пустая строка
- `description` = полный текст поста

**Обоснование:**
- Избегаем дублирования данных
- Title в группах часто неинформативен (это просто первая строка)
- Полный текст в description позволяет лучше извлекать информацию

### Stage 2: Извлечение title из description

На втором этапе для групп:
1. Извлекаем осмысленный title из description (первое предложение, макс 150 символов)
2. Парсим параметры: bedrooms, price_extracted, has_*, kitchen_type и т.д.
3. Обновляем запись с извлеченным title

**Метод:** `PropertyParser.extract_title_from_description(text, max_length=150)`

## Изменения в схеме БД

### Переименование колонок (groq → llm)

Для универсальности и возможности использования разных LLM:

- `groq_passed` → `llm_passed`
- `groq_reason` → `llm_reason`
- `groq_analyzed_at` → `llm_analyzed_at`

### Удаленные колонки

- `all_images` - хранился JSON массив картинок, никогда не использовался
- `timestamp` - дублировал функционал `created_at`

### Сохраненные колонки

Все эти колонки **заполняются в Stage 2** при парсинге description:

- `bedrooms` - количество спален
- `price_extracted` - извлеченная цена (numeric)
- `kitchen_type` - тип кухни (enclosed/outdoor/shared)
- `has_ac`, `has_wifi`, `has_pool`, `has_parking` - удобства
- `utilities` - коммунальные услуги (included/excluded)
- `furniture` - мебель (fully_furnished/partially/unfurnished)
- `rental_term` - срок аренды (monthly/yearly/daily/weekly) - используется для фильтрации

## Валидация данных групп

### Новая валидация в group_scraper.py

Посты отклоняются если:
- Отсутствует URL
- Отсутствует текст (пустое поле `text`)

Это предотвращает попадание пустых/неполных объявлений в базу.

## Миграция

Файл: `db/migration_optimize_schema.sql`

Применение:
```bash
docker-compose exec -T postgres psql -U realty_user -d realty_bot < db/migration_optimize_schema.sql
```

## Обратная совместимость

Весь код обновлен для работы с новыми названиями колонок:
- `src/database.py` - методы обновления Stage 3
- `src/main.py` - получение данных для Telegram
- `scripts/run_stage3_groq.py` - LLM анализ
- `scripts/run_stage2_manual.py` - извлечение title для групп

## Итоговая структура данных

### Stage 1 (Groups)
```python
{
    'fb_id': '123456',
    'title': '',  # Пусто!
    'description': 'Полный текст поста...',
    'source': 'facebook_group',
    'group_id': '789012',
    'status': 'stage1'
}
```

### Stage 2 (Groups после обработки)
```python
{
    'fb_id': '123456',
    'title': 'Villa 2BR Ubud monthly rent...',  # Извлечено!
    'description': 'Полный текст поста...',
    'bedrooms': 2,
    'price_extracted': 10000000,
    'has_ac': True,
    'has_wifi': True,
    'kitchen_type': 'enclosed',
    'status': 'stage2'
}
```
