# RealtyBot Bali

Система многоэтапной обработки объявлений из Facebook Marketplace и Facebook Groups:
- сбор через Apify (QFR для Marketplace, Groups scraper),
- автофильтры и LLM-проверка через OpenRouter,
- профильная фильтрация, дедупликация и отправка в Telegram.

## Источники объявлений

- **QFR** (`run_stage2_manual_qfr.py`) — Facebook Marketplace по поисковым URL из профиля (`qfr_start_urls`)
- **Groups** (`run_group_scrape_manual.py` + `run_stage2_manual.py`) — Facebook Groups из `config.json`

Листинги попадают в общую таблицу `listings`, далее проходят единый pipeline.

## Pipeline

| Этап | Скрипт | Описание |
|------|--------|----------|
| Stage 1 | Groups scrape / QFR | Сбор и первичная запись |
| Stage 2 | `run_stage2_manual.py` (groups) / QFR upsert | Автофильтры, stop_words, stop_locations |
| Stage 3 | `run_stage3_llm.py` | Глобальная LLM-проверка (тип, локация, room-only, term) |
| Stage 4 | `run_stage4.py --chat X` | Профильная проверка (bedrooms, price, allowed_locations), генерация summary_ru |
| Stage 5 | `run_stage5.py --chat X` | Отправка в Telegram, stage5_guard, дедупликация per-chat |

## Добавление нового чата

### 1. Узнать chat_id

Написать `/start` в чате — бот ответит, в логах `telegram-bot` будет `chat.id`.

### 2. Добавить профиль в `config/profiles.json`

```json
{
  "chat_id": -1009999999999,
  "name": "2BR Budget Canggu",
  "enabled": true,
  "bedrooms_min": 1,
  "bedrooms_max": 2,
  "price_max": 15000000,
  "allowed_locations": ["Canggu", "Pererenan", "Berawa", "Seminyak"],
  "stop_locations": [],
  "stop_words": ["3br", "4br", ...],
  "qfr_start_urls": [
    "https://www.facebook.com/marketplace/107286902636860/propertyrentals?minBedrooms=1&maxBedrooms=2&maxPrice=15000000&daysSinceListed=4"
  ]
}
```

### 3. Запустить pipeline

```bash
# QFR для нового чата (или без --chat для всех enabled)
docker compose run --rm bot python3 scripts/run_stage2_manual_qfr.py --chat -1009999999999

# Stage 3 — глобальный, обрабатывает все stage2
docker compose run --rm bot python3 scripts/run_stage3_llm.py

# Stage 4 и 5 — per-chat
docker compose run --rm bot python3 scripts/run_stage4.py --chat -1009999999999
docker compose run --rm bot python3 scripts/run_stage5.py --chat -1009999999999
```

## Cron (Docker)

Расписание задаётся в `crontab`, применяется в контейнере `bot`:

- **QFR**: каждые 10 мин (per-chat)
- **Stage 3**: каждый час (30 мин)
- **Stage 4**: каждый час (43 мин), после Stage 3
- **Groups scrape**: каждые 3 часа (20 мин)
- **Stage 2 manual** (groups): каждые 3 часа (23 мин)
- **Stage 5**: каждый час (53 мин), per-chat

## Команды бота

- `/stats` — фидбек по реакциям (❤️💩🤡) + статистика pipeline по этапам
- `/favorites` — объявления с ❤
- Реакции per-chat: ❤ в чате A не влияет на статистику чата B

## Конфигурация

- `config/config.json` — глобальные настройки (Apify, LLM, telegram, filters)
- `config/profiles.json` — массив профилей чатов (chat_id, bedrooms, price_max, allowed_locations, qfr_start_urls)
