# Готовые решения для парсинга Facebook Marketplace + Groups

## 📋 Сводка найденных решений

### 🏆 ТОП-3 Лучших готовых решения

---

## 1. **facebook-scraper** (kevinzg) - Лучший для Groups
**GitHub:** https://github.com/kevinzg/facebook-scraper  
**PyPI:** https://pypi.org/project/facebook-scraper/

### ✅ Преимущества:
- **Очень популярный** (2.8K звёзд на GitHub)
- Работает **БЕЗ API ключей**
- Поддерживает **Groups И Pages**
- **Легко извлекает описания** из постов
- Работает с cookies для авторизации
- Встроенная поддержка комментариев и реакций

### ⚠️ ПРОБЛЕМА (2024):
**НЕ РАБОТАЕТ** - Facebook изменил HTML структуру
- Ошибка: "No raw posts (<article> elements) were found"
- 438+ открытых Issues на GitHub
- Библиотека не обновляется
- Последний релиз: v0.2.59 (устарел)

### 📦 Установка:
```bash
pip install facebook-scraper
```

### 💻 Базовый пример использования:

#### Парсинг постов из группы:
```python
from facebook_scraper import get_posts

# Парсинг постов группы (нужен group_id)
for post in get_posts(group='YOUR_GROUP_ID', pages=5, cookies='cookies.txt'):
    print(f"Текст: {post['text']}")
    print(f"Время: {post['time']}")
    print(f"Ссылка: {post['post_url']}")
    print(f"Лайки: {post['likes']}")
    print(f"Описание: {post['text']}")  # Полное описание поста!
    print("-" * 50)
```

#### Парсинг конкретных URL постов:
```python
from facebook_scraper import get_posts

# Если у вас есть ссылки на конкретные посты
post_urls = [
    "https://www.facebook.com/groups/GROUP_ID/posts/POST_ID1",
    "https://www.facebook.com/groups/GROUP_ID/posts/POST_ID2"
]

for post in get_posts(post_urls=post_urls, cookies='cookies.txt'):
    print(f"URL: {post['post_url']}")
    print(f"Полное описание: {post['text']}")
    print(f"Изображения: {post['images']}")
```

#### Извлечение комментариев (с описаниями):
```python
from facebook_scraper import get_posts

POST_ID = "pfbid02NsuAiBU9o1ouwBrw1vYAQ7khcVXvz8F8zMvkVat9UJ"

gen = get_posts(
    post_urls=[POST_ID],
    options={"comments": 100, "progress": True},
    cookies='cookies.txt'
)

post = next(gen)
comments = post['comments_full']

for comment in comments:
    print(f"Комментарий: {comment['comment_text']}")
    for reply in comment['replies']:
        print(f"  Ответ: {reply['comment_text']}")
```

### 🍪 Как получить cookies:
1. Установите расширение **Get cookies.txt LOCALLY** (Chrome) или **Cookie Quick Manager** (Firefox)
2. Зайдите на Facebook
3. Экспортируйте cookies в файл `cookies.txt`
4. Используйте файл в скрипте

### ⚙️ Дополнительные возможности:
- `get_profile()` - информация о профиле
- `get_group_info()` - информация о группе
- `write_posts_to_csv()` - прямая запись в CSV

---

## 2. **fb-marketplace-scraper** (SPolton) - Лучший для Marketplace
**GitHub:** https://github.com/SPolton/fb-marketplace-scraper

### ✅ Преимущества:
- **Playwright** + BeautifulSoup (современный стек)
- **GUI на Streamlit** (удобный интерфейс)
- **FastAPI** для API
- **SQLite** база данных для отслеживания новых объявлений
- **Push уведомления** через ntfy
- **Автоматический планировщик** для регулярного парсинга
- Поддержка множества городов и категорий

### 📦 Установка:
```bash
git clone https://github.com/SPolton/fb-marketplace-scraper
cd fb-marketplace-scraper
python -m venv venv
source venv/bin/activate  # Linux/Mac
# или venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### ⚙️ Настройка .env файла:
```env
# Обязательные
FB_USER = your_email@example.com
FB_PASSWORD = your_password

# Опциональные
HOST = 127.0.0.1
PORT = 8000
DATABASE = static/search_results.db
NTFY_SERVER = https://ntfy.sh
```

### 💻 Запуск:
```bash
# API
python app.py

# GUI (в другом терминале)
streamlit run gui.py
```

### 📊 Возможности GUI:
- Выбор города из списка
- Выбор категории товаров
- Поиск по ключевым словам
- Диапазон цен
- Состояние товара (новый/б/у)
- Отслеживание новых объявлений
- Автоматический парсинг по расписанию

### 🔧 Структура данных в БД:
**Таблица results:**
- url (ссылка на объявление)
- title (название)
- price (цена)
- location (местоположение)
- image (фото)
- is_new (новое объявление или нет)
- timestamp (время добавления)

---

## 3. **facebook-marketplace-scraper** (passivebot)
**GitHub:** https://github.com/passivebot/facebook-marketplace-scraper

### ✅ Преимущества:
- Также Playwright + BeautifulSoup
- Streamlit GUI
- Простая архитектура
- JSON вывод данных

### 📦 Установка:
```bash
git clone https://github.com/passivebot/facebook-marketplace-scraper
cd facebook-marketplace-scraper
pip install -r requirements.txt
```

---

## 🎯 Комбинированное решение: Marketplace + Groups

### Стратегия объединения:

```python
# 1. Используем facebook-scraper для Groups
from facebook_scraper import get_posts

# 2. Используем Playwright для Marketplace (из SPolton)
from playwright.sync_api import sync_playwright

class CombinedFacebookScraper:
    def __init__(self, email, password, cookies_file='cookies.txt'):
        self.email = email
        self.password = password
        self.cookies_file = cookies_file
        
    def scrape_groups(self, group_id, max_posts=50):
        """Парсинг группы с полными описаниями"""
        posts_data = []
        
        for post in get_posts(
            group=group_id, 
            pages=10,
            cookies=self.cookies_file,
            options={"comments": True, "allow_extra_requests": True}
        ):
            posts_data.append({
                'source': 'group',
                'url': post.get('post_url'),
                'title': post.get('text', '')[:100],  # Первые 100 символов как заголовок
                'description': post.get('text', ''),  # ПОЛНОЕ ОПИСАНИЕ
                'images': post.get('images', []),
                'likes': post.get('likes'),
                'comments_count': post.get('comments'),
                'shares': post.get('shares'),
                'timestamp': post.get('time')
            })
            
            if len(posts_data) >= max_posts:
                break
                
        return posts_data
    
    def scrape_marketplace_listing(self, url):
        """Переход в конкретное объявление и извлечение описания"""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()
            
            # Логин и загрузка cookies если нужно
            page.goto('https://www.facebook.com')
            # ... логин ...
            
            # Переход на страницу объявления
            page.goto(url)
            page.wait_for_timeout(3000)
            
            # Извлечение данных
            try:
                # Селекторы для описания
                description_selectors = [
                    'div[class*="x1iorvi4"] span',
                    'div[data-ad-preview="message"] span',
                    'span[dir="auto"]'
                ]
                
                description = ""
                for selector in description_selectors:
                    elements = page.query_selector_all(selector)
                    texts = [el.inner_text() for el in elements if el.inner_text()]
                    if texts:
                        description = "\n".join(texts)
                        break
                
                # Цена
                price = page.query_selector('span:has-text("₽")') or \
                        page.query_selector('span:has-text("руб")')
                
                # Местоположение
                location = page.query_selector('span:has-text("км")') or \
                          page.query_selector('span[class*="location"]')
                
                data = {
                    'source': 'marketplace',
                    'url': url,
                    'description': description,
                    'price': price.inner_text() if price else None,
                    'location': location.inner_text() if location else None
                }
                
                browser.close()
                return data
                
            except Exception as e:
                print(f"Ошибка: {e}")
                browser.close()
                return None
    
    def scrape_marketplace_search(self, query, city="moscow", max_listings=20):
        """Парсинг результатов поиска в Marketplace"""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()
            
            # Поиск
            url = f"https://www.facebook.com/marketplace/{city}/search?query={query}"
            page.goto(url)
            page.wait_for_timeout(5000)
            
            # Прокрутка для загрузки
            for _ in range(3):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(2000)
            
            # Сбор ссылок на объявления
            links = page.query_selector_all('a[href*="/marketplace/item/"]')
            listing_urls = []
            
            for link in links[:max_listings]:
                href = link.get_attribute('href')
                if href and '/marketplace/item/' in href:
                    clean_url = href.split('?')[0]
                    listing_urls.append(clean_url)
            
            browser.close()
            
            # Удаляем дубликаты
            listing_urls = list(set(listing_urls))
            
            return listing_urls
    
    def combined_scrape(self, marketplace_query, group_id, city="moscow"):
        """Комбинированный парсинг Marketplace + Groups"""
        all_data = []
        
        # 1. Парсим Marketplace
        print("🛒 Парсинг Marketplace...")
        marketplace_urls = self.scrape_marketplace_search(
            query=marketplace_query, 
            city=city, 
            max_listings=10
        )
        
        # Извлекаем описания из каждого объявления
        for url in marketplace_urls:
            print(f"📄 Извлечение описания: {url}")
            listing_data = self.scrape_marketplace_listing(url)
            if listing_data:
                all_data.append(listing_data)
        
        # 2. Парсим Groups
        print("👥 Парсинг Groups...")
        group_posts = self.scrape_groups(group_id=group_id, max_posts=10)
        all_data.extend(group_posts)
        
        return all_data

# ИСПОЛЬЗОВАНИЕ:
scraper = CombinedFacebookScraper(
    email='your_email@example.com',
    password='your_password',
    cookies_file='cookies.txt'
)

# Комбинированный парсинг
results = scraper.combined_scrape(
    marketplace_query='iphone',
    group_id='YOUR_GROUP_ID',
    city='moscow'
)

# Вывод результатов
for item in results:
    print(f"\n{'='*60}")
    print(f"Источник: {item['source']}")
    print(f"URL: {item['url']}")
    print(f"Описание: {item.get('description', 'N/A')[:200]}...")
    print(f"{'='*60}")
```

---

## 🔑 Ключевые моменты для извлечения описаний:

### 1. **Для Groups** (facebook-scraper):
```python
# Описание уже извлекается автоматически через поле 'text'
post['text']  # Это и есть полное описание поста!
```

### 2. **Для Marketplace** (Playwright/Selenium):
```python
# Нужно перейти по ссылке и найти элементы с описанием
description_selectors = [
    'div[class*="x1iorvi4"] span',
    'span[class*="x193iq5w"]',
    'div[data-ad-preview="message"]',
]
```

### 3. **Важные селекторы для Marketplace:**

```python
# Описание
"//div[contains(@class, 'x1iorvi4')]//span"
"//span[contains(@class, 'x193iq5w') and string-length(text()) > 50]"

# Цена
"//span[contains(text(), '₽') or contains(text(), 'руб')]"

# Местоположение
"//span[contains(text(), 'км')]"
"//span[contains(@class, 'location')]"

# Продавец
"//a[contains(@href, '/marketplace/profile')]//span"
```

---

## 📚 Дополнительные ресурсы:

### Коммерческие решения (если нужна надёжность):
1. **Apify Facebook Marketplace Scraper**
   - https://apify.com/apify/facebook-marketplace-scraper
   - Готовый cloud-сервис
   - $49/мес за ~9,800 объявлений

2. **ScrapFly**
   - https://scrapfly.io/blog/posts/how-to-scrape-facebook
   - Профессиональный парсинг с обходом блокировок

### Статьи и туториалы:
- https://www.promptcloud.com/blog/python-facebook-scraper/
- https://iproyal.com/blog/how-to-build-a-facebook-scraper-and-an-amazon-scraper/
- https://scrapfly.io/blog/posts/how-to-scrape-facebook

---

## ⚠️ Важные предупреждения:

1. **Легальность**: Facebook запрещает автоматический парсинг в ToS
2. **Блокировки**: Используйте задержки, прокси, ротацию IP
3. **Cookies**: Обязательно сохраняйте сессию для длительной работы
4. **Rate Limiting**: Не более 1 запроса в 3-5 секунд
5. **User-Agent**: Используйте реальные браузерные user-agents

---

## 🎯 Рекомендация:

**Для вашей задачи (Marketplace + Groups с описаниями):**

1. **Используйте Playwright для ОБОИХ источников** - facebook-scraper сломан
2. **Адаптируйте `fb-marketplace-scraper` (SPolton)** для Marketplace
3. **Создайте Playwright Groups scraper** аналогично Marketplace

Это даст вам:
✅ Работающий парсинг обоих источников  
✅ Полные описания из постов и объявлений  
✅ Независимость от изменений HTML (браузерный рендеринг)  
✅ Возможность делать скриншоты для дебага
