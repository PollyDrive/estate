#!/usr/bin/env python3
"""
Тест нового строгого формата summary
"""

import os
import json
from dotenv import load_dotenv
from zhipuai import ZhipuAI

load_dotenv()

# Load config
with open('config/config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

client = ZhipuAI(api_key=os.getenv('ZHIPU_API_KEY'))

# Test listing
test_listing = {
    "title": "Beautiful 2BR Villa with Pool in Ubud",
    "price": "12,000,000 IDR/month",
    "location": "Ubud, Bali",
    "description": """FOR RENT: Stunning 2 bedroom villa in the heart of Ubud
    
Features:
- 2 bedrooms with AC
- 2 bathrooms with hot water
- Fully equipped kitchen
- Private swimming pool
- High-speed WiFi
- Parking for 2 cars

Located in quiet area, 5 minutes from Ubud center
Monthly rent: 12 million IDR

Contact: 0812345678
"""
}

full_text = f"""Заголовок: {test_listing['title']}
Цена: {test_listing['price']}
Локация: {test_listing['location']}
Описание: {test_listing['description']}"""

prompt = f"""Извлеки из объявления ключевую информацию и верни СТРОГО в формате списка с маркерами.

ФОРМАТ (используй ТОЛЬКО маркеры •):
• [количество] спальни/спален
• [район, город]
• [удобства через запятую: бассейн, кухня, AC, WiFi и т.д.]
• [цена]/мес

ПРАВИЛА:
- Каждый пункт начинается с •
- Каждый пункт на новой строке
- БЕЗ лишних слов и предложений
- Если информация отсутствует - пропускай пункт
- НЕ добавляй эмодзи
- НЕ добавляй комментарии

ПРИМЕР:
• 2 спальни
• Убуд, Бали
• Бассейн, кухня, AC, WiFi
• 12 млн IDR/мес

Текст объявления:
{full_text}

СПИСОК:"""

print("🤖 Generating strict format summary...\n")

response = client.chat.completions.create(
    model=config['llm']['zhipu']['model'],
    messages=[{"role": "user", "content": prompt}],
    temperature=0.1,
    max_tokens=150
)

summary = response.choices[0].message.content.strip()

print("=" * 60)
print("СТРОГИЙ ФОРМАТ SUMMARY:")
print("=" * 60)
print(summary)
print("=" * 60)
