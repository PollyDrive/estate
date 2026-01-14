# 🏠 RealtyBot-Bali

Автоматический парсер Facebook Marketplace + Groups для поиска жилья на Бали с умной фильтрацией.

### 1. Подписка на Apify Actor ($15/мес)

```bash
# Открыть в браузере
https://apify.com/memo23/facebook-marketplace-cheerio

# Нажать "Subscribe" → $15/месяц или за 4 часа free обработать что нужно

# ВАЖНО: Установить лимит расходов
https://console.apify.com/billing/limits

```

### 2. Настройка .env

```bash
cp .env.example .env
nano .env
```

Заполните:
```env
APIFY_API_KEY=apify_api_...
DATABASE_URL=postgresql://...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

### 3. Первый запуск

```bash
# Тест актора (безопасно, ~$0.01)
python3 test_cheerio.py

# Проверить стоимость
open https://console.apify.com/billing

# Если ОК → полный запуск
python3 src/main.py
```

## 📋 Что делает бот

1. **Скрапит Facebook Marketplace** через Apify Cheerio actor
2. **Парсит параметры**: спальни, цена, кухня, AC, WiFi, телефон
3. **Фильтрует** по критериям (без LLM!)
4. **Сохраняет** в PostgreSQL
5. **Отправляет** в Telegram подходящие варианты

## 🎯 Критерии поиска

Настраиваются в `config.json`:

```json
{
  "marketplace_cheerio": {
    "max_items": 20,  // ← ЛИМИТ РАСХОДОВ!
    "marketplace_urls": [
      "https://www.facebook.com/marketplace/107286902636860/search?query=villa%20rent&maxPrice=16000000"
    ]
  },
  "criterias": {
    "price_rules": [
      {"bedrooms": 0, "max_price": 5000000},   // Студия ≤ 5M IDR
      {"bedrooms": 1, "max_price": 6000000},   // 1BR ≤ 6M IDR
      {"bedrooms": 2, "max_price": 14000000}   // 2BR ≤ 14M IDR
    ]
  }
}
```

### Что фильтруется автоматически:

- ❌ Daily/weekly rentals (посуточно/понедельно)
- ❌ Outdoor kitchen (уличная кухня)
- ❌ Shared kitchen (общая кухня)
- ❌ Нет AC или WiFi
- ❌ Цена выше лимита

### Что пропускается:

- ✅ Monthly/yearly rental
- ✅ Enclosed/indoor kitchen
- ✅ AC + WiFi упоминаются
- ✅ Цена в пределах лимита

## 💰 Стоимость

| Компонент | Цена |
|-----------|------|
| Подписка на актор | $15/месяц |
| Apify credits | ~$0.01-0.02 за запуск |
| **При 3 запусках/день** | **~$16-18/месяц** |

**Контроль расходов:**
- `max_items: 20` в config.json (безопасно)
- Установить Monthly limit в Apify Console
- Первые дни запускать вручную

## 🤖 Автоматизация

```bash
crontab -e

# Каждые 3 часа
0 */3 * * * cd /path/to/estate && python3 src/main.py >> logs/cron.log 2>&1
```

## 📊 Мониторинг

```bash
# Логи
tail -f logs/realty_bot.log

# База данных
psql $DATABASE_URL -c "SELECT COUNT(*), source FROM listings GROUP BY source;"

# Apify credits
open https://console.apify.com/billing
```

## 📁 Структура

```
estate/
├── src/
│   ├── main.py                                 # Главный скрипт
│   ├── facebook_marketplace_cheerio_scraper.py # Cheerio актор
│   ├── property_parser.py                      # Парсинг параметров (БЕЗ LLM!)
│   ├── database.py                             # PostgreSQL
│   └── telegram_notifier.py                    # Telegram бот
├── config.json                                 # Настройки
├── test_cheerio.py                             # Тест актора
└── README.md                                   # Этот файл
```

## 🔧 Настройка search URLs

### Создать свой URL:

1. Зайти на Facebook Marketplace: https://www.facebook.com/marketplace/107286902636860
2. Ввести поисковый запрос: "villa rent"
3. Применить фильтры (цена, локация)
4. Скопировать URL из адресной строки

### Примеры для Бали:

```json
"marketplace_urls": [
  "https://www.facebook.com/marketplace/107286902636860/search?query=villa%20rent&maxPrice=16000000",
  "https://www.facebook.com/marketplace/107286902636860/search?query=house%20rent&maxPrice=14000000",
  "https://www.facebook.com/marketplace/ubud/search?query=monthly%20rent"
]
```

## 🐛 Troubleshooting

| Проблема | Решение |
|----------|---------|
| "Actor not found" | Не подписались на актор |
| "APIFY_API_KEY not found" | Проверить `.env` |
| "0 listings" | Проверить URL в браузере |
| Слишком дорого | Снизить `max_items` до 10-20 |

## 📞 Telegram уведомления

Формат сообщения:

```
🏡 Новый вариант!

2 спален | кухня: enclosed | мебель: fully_furnished | счета: excluded

💰 Цена: Rp 12,000,000
📞 Телефон: +62 812-3456-7890
🔗 Ссылка: https://facebook.com/marketplace/item/...
```

## 🗃️ База данных

Используется PostgreSQL с расширенной схемой:

```sql
CREATE TABLE listings (
    id SERIAL PRIMARY KEY,
    fb_id VARCHAR(255) UNIQUE,
    source VARCHAR(50),           -- 'marketplace_cheerio'
    title TEXT,
    description TEXT,
    price VARCHAR(100),
    location VARCHAR(255),
    listing_url TEXT,
    phone_number VARCHAR(50),
    sent_to_telegram BOOLEAN,
    
    -- Extracted parameters
    bedrooms INTEGER,
    price_extracted NUMERIC,
    kitchen_type VARCHAR(50),
    has_ac BOOLEAN,
    has_wifi BOOLEAN,
    has_pool BOOLEAN,
    has_parking BOOLEAN,
    utilities VARCHAR(50),
    furniture VARCHAR(50),
    rental_term VARCHAR(50),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🎓 Как работает PropertyParser

**БЕЗ LLM API!** Все через regex и правила:

```python
# Извлекает из текста:
- Bedrooms: "2BR", "studio", "2 bedrooms" → 2, 0, 2
- Price: "10 juta", "3.5 million", "Rp 5,000,000" → 10M, 3.5M, 5M IDR
- Kitchen: "enclosed kitchen" → enclosed
- AC: "AC in all rooms" → True
- WiFi: "WiFi included" → True
- Phone: "+62 812-3456-7890", "0812-3456-7890" → нормализует
```

## 📝 Следующие шаги

1. ✅ Подписаться на актор
2. ✅ Настроить `.env`
3. ✅ Запустить тест: `python3 test_cheerio.py`
4. ✅ Проверить стоимость в Apify Console
5. ✅ Настроить URLs под свои критерии
6. ✅ Запустить: `python3 src/main.py`
7. ✅ Проверить Telegram уведомления
8. ✅ Настроить cron для автозапуска

## 🔗 Полезные ссылки

- **Apify Billing**: https://console.apify.com/billing
- **Actor Runs**: https://console.apify.com/actors/3pS4Ux0mBVXsgDUUE/runs
- **Set Limits**: https://console.apify.com/billing/limits
- **Telegram BotFather**: https://t.me/botfather

---

**Статус**: ✅ Production Ready  
**Стоимость**: ~$16-18/месяц  
**Последнее обновление**: 2025-01-08

flowchart TD

subgraph Marketplace
    A1[Stage 1: apify/facebook-marketplace-scraper<br>→ извлечение заголовка и локации<br>→ фильтрация стоп-слов и стоп-локаций<br>→ сохранение с флагом stage1]
    A2[Stage 2: memo23/facebook-marketplace-cheerio<br>→ извлечение description по URL айтемов<br>→ проверка детальных стоп-слов<br>→ сохранение с флагом stage2]
end

subgraph Groups
    B1[Stage 1: apify/facebook-groups-scraper<br>→ извлечение заголовка, description и локации<br>→ фильтрация стоп-слов и стоп-локаций<br>→ сохранение с флагом stage1]
    B2[Stage 2: автофильтр<br>→ если description есть → проверка детальных стоп-слов<br> → флаг stage2<br>если description нет → флаг no_description]
end

subgraph Общие этапы
    C3[Stage 3: анализ title, description, location через grok<br>→ сохранение с флагом stage3]
    C4[Stage 4: дедупликация grok по совпадению ключевых параметров<br>→ генерация короткого RU-текста для телеги<br>→ сохранение с флагом stage4]
    C5[Stage 5: отправка в Telegram<br>→ флаг stage5]
end

A2 --> C3
B2 --> C3
C3 --> C4
C4 --> C5



`docker-compose run --rm bot python3 scripts/run_stage1_manual.py`

• Скрейпит заголовки с Facebook Marketplace через Apify
• Применяет базовые фильтры
• Сохраняет новые объявления в БД со статусом stage1_new

----------------------------------
`docker-compose run --rm bot python3 scripts/run_group_scrape_manual.py`

• Выбирает 6 групп из списка (те, что давно не скрейпились)
• Скрейпит до 15 постов из каждой группы
• Фильтрует по stop_words в заголовке
• Фильтрует по stop_locations в локации
• Сохраняет кандидатов со статусом stage1 и source='facebook_group'

Примечание: Будут обработаны только публичные группы из твоего списка, так как куки пока не используются. Закрытые
группы не дадут результатов.

---------------------------
`docker-compose run --rm bot python3 scripts/run_stage2_manual.py`

• Возьмёт объявления со статусом stage1
• Отфильтрует marketplace listings (151 + 83 = 234 объявлений)
• Запустит cheerio scraper для первых 50 объявлений (лимит max_stage2_items)
• Проверит детальные стоп-слова в description
• Обработает Groups:
 • С description (159) → проверит детальные стоп-слова
 • Без description → пометит как no_description
• Обновит статусы: stage2, stage2_failed, или no_description


---------------------------

`docker-compose run --rm bot python3 scripts/run_stage3_groq.py`

• Обработает объявления marketplace + groups
• Проверит через Groq LLM на соответствие критериям
• Обновит статусы: stage3 (прошли) или stage3_failed (не прошли)

------------------

Stage 4 (дедупликация + русские саммари)

`docker-compose run --rm bot python3 scripts/run_stage4.py`

• Удалит дубликаты
• Сгенерирует русские описания
• Переведет в stage4_final_candidates

------------------

Stage 5 (Telegram)

`docker-compose run --rm bot python3 scripts/run_stage5.py`

• Отправит 10 listings в группу
• С задержкой 2 секунды между сообщениями
• Не будет отправлять с 01:00 до 07:00