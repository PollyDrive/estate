# RealtyBot Bali

Система многоэтапной обработки объявлений из Facebook Marketplace и Facebook Groups:
- сбор через Apify,
- фильтрация по правилам,
- LLM-проверка через OpenRouter,
- дедупликация и отправка в Telegram.

## Что актуально сейчас

- Используется PostgreSQL схема `"_4BR"` для текущего запуска (`POSTGRES_SCHEMA=_4BR`).
- Целевой профиль: аренда вилл/домов с фокусом на `4+ bedrooms`.
- LLM в Stage 3 и Stage 4: только OpenRouter (Gemini напрямую не используется).
- Для Stage 2 добавлен новый manual-скрипт на actor `qFR6mjgdwPouKLDvE`, старый Cheerio-скрипт сохранен.

## Быстрый старт

### 1) Подготовка окружения

```bash
cp .env.example .env
```

Заполните минимум:

```env
DATABASE_URL=postgresql://realty_user:realty_pass@postgres:5432/realty_bot
POSTGRES_SCHEMA=_4BR
APIFY_API_KEY=apify_api_...
OPENROUTER_API_KEY=sk-or-v1-...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

### 2) Поднять сервисы

```bash
docker compose up -d
```

### 3) Проверка БД статусов

```bash
docker compose exec bot python - <<'PY'
from src.database import Database
with Database() as db:
    db.cursor.execute("SELECT status, COUNT(*) FROM listings GROUP BY status ORDER BY status")
    print(db.cursor.fetchall())
PY
```

## Пайплайн (этапы)

### Stage 1: первичный сбор

Marketplace:

```bash
docker compose exec bot python scripts/run_stage1_manual.py
```

Groups:

```bash
docker compose exec bot python scripts/run_group_scrape_manual.py
```

Результат: новые записи со статусами `stage1` / `stage1_new`.

---

### Stage 2: расширенный скрейп + структурный парсинг

#### Новый вариант (рекомендуется): QFR actor

```bash
docker compose exec bot python scripts/run_stage2_manual_qfr.py
```

Использует новый скрапер:
- `src/facebook_marketplace_qfr_scraper.py`
- actor `qFR6mjgdwPouKLDvE`

#### Legacy-вариант: Cheerio actor

```bash
docker compose exec bot python scripts/run_stage2_manual.py
```

Использует:
- `src/facebook_marketplace_cheerio_scraper.py`

Результат Stage 2:
- `stage2` — прошли структурные фильтры,
- `stage2_failed` — не прошли,
- `no_description` — для групп без описания.

---

### Stage 3: LLM-фильтрация (OpenRouter only)

```bash
docker compose exec bot python scripts/run_stage3_llm.py
```

Важно:
- при любой LLM-ошибке скрипт останавливается с `exit code 1`,
- текущий листинг не помечается как failed из-за ошибки LLM (остается в `stage2`),
- бизнес-отклонения получают `stage3_failed` с причиной `REJECT_*`,
- прошедшие получают `stage3`.

---

### Stage 4: дедупликация + RU summary

```bash
docker compose exec bot python scripts/run_stage4.py
```

Что делает:
- обрабатывает `stage3`,
- формирует бакеты точных дублей по `description + location + price_extracted`,
- выбирает каноническую запись (самую раннюю по `created_at`) -> `stage4`,
- остальные в бакете -> `stage4_duplicate`,
- генерирует `summary_ru` через OpenRouter.

---

### Stage 5: отправка в Telegram

```bash
docker compose exec bot python scripts/run_stage5.py
```

## Ключевые статусы

- `stage1`, `stage1_new` — первичный кандидат.
- `stage2` — прошел структурный Stage 2.
- `stage2_failed` — отфильтрован на Stage 2.
- `stage3` — прошел LLM-фильтр.
- `stage3_failed` — отклонен LLM/бизнес-правилами (`REJECT_*`).
- `stage4_duplicate` — точный дубль канонической записи.
- `stage5_sent` — отправлен в Telegram.

## Конфигурация

Файл: `config/config.json`.

Критичное:
- `criterias.bedrooms_min: 4`
- `filters.stop_words`, `filters.stop_words_detailed`, `filters.stop_locations`
- `llm.openrouter`:
  - `model: "openrouter/auto"`
  - `fallback_models` (цепочка)
  - retry/backoff/timeout параметры
- `marketplace_qfr` — настройки нового Stage 2 скрапера.

## Частые операции

### Вернуть проблемные записи из `stage3_failed` обратно в `stage2`

```sql
UPDATE listings
SET status = 'stage2'
WHERE status = 'stage3_failed';
```

### Очистить ошибочно помеченные Stage 3 записи (LLM error)

```sql
UPDATE listings
SET status = 'stage2',
    llm_passed = NULL,
    llm_reason = NULL,
    llm_analyzed_at = NULL
WHERE status = 'stage3'
  AND llm_reason ILIKE '%error%';
```

### Сводка по статусам

```sql
SELECT status, COUNT(*)
FROM listings
GROUP BY status
ORDER BY status;
```

## Импорт из существующих Apify runs

Для добора данных из исторических запусков:

```bash
docker compose exec bot python scripts/import_cheerio_runs.py --runs-limit 50
```

Скрипт:
- пытается подтянуть datasets run'ов,
- добавляет недостающие записи,
- обновляет `stage1/stage1_new -> stage2/stage2_failed` для обработанных объявлений.

## Troubleshooting

- `Missing APIFY_API_KEY` -> проверьте `.env` и `docker compose exec bot env | grep APIFY`.
- `run_stage3_llm.py exits 1` -> это ожидаемо при LLM-сбое; исправить ключ/лимиты и перезапустить Stage 3.
- В Stage 2 обрабатывается мало marketplace записей -> смотреть `logs/stage2_manual_qfr.log` (или `stage2_manual.log`) и статус конкретного actor run в Apify Console.
- OpenRouter SSL/EOF ошибки -> уже включен увеличенный timeout + retry/backoff в `src/llm_filters.py`.

## Основные файлы

- `scripts/run_stage1_manual.py`
- `scripts/run_group_scrape_manual.py`
- `scripts/run_stage2_manual_qfr.py` (новый Stage 2)
- `scripts/run_stage2_manual.py` (legacy Cheerio)
- `scripts/run_stage3_llm.py`
- `scripts/run_stage4.py`
- `scripts/run_stage5.py`
- `src/facebook_marketplace_qfr_scraper.py`
- `src/facebook_marketplace_cheerio_scraper.py`
- `src/llm_filters.py`
- `config/config.json`

---

Последнее обновление: 2026-02-13

