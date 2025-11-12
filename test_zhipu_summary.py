#!/usr/bin/env python3
"""
Тест генерации русского summary через Zhipu
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
from zhipuai import ZhipuAI

# Load environment
load_dotenv()

# Load config
with open('config/config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

# Get API key
zhipu_api_key = os.getenv('ZHIPU_API_KEY')

if not zhipu_api_key:
    print("❌ ZHIPU_API_KEY not found!")
    sys.exit(1)

# Initialize client
print("🤖 Initializing Zhipu client...\n")
client = ZhipuAI(api_key=zhipu_api_key)

# Test listing (example in English and Indonesian)
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
- Tropical garden
- High-speed WiFi
- Parking for 2 cars

Located in quiet area, 5 minutes from Ubud center
Monthly rent: 12 million IDR
Available now

Contact: 0812345678 (WhatsApp)
"""
}

# Build full text
full_text = f"""Заголовок: {test_listing['title']}
Цена: {test_listing['price']}
Локация: {test_listing['location']}
Описание: {test_listing['description']}"""

# Prompt for summary
prompt = f"""Ты помощник по недвижимости. Создай КРАТКОЕ описание объявления на русском языке (2-4 предложения).

ПРАВИЛА:
- Будь краток и информативен
- Укажи количество комнат, основные удобства (бассейн, кухня, AC и т.д.)
- Укажи район/локацию если есть
- НЕ указывай телефоны и контакты
- Пиши простым языком

Текст объявления:
{full_text}

КРАТКОЕ ОПИСАНИЕ НА РУССКОМ:"""

print("=" * 80)
print("Testing Zhipu Summary Generation")
print("=" * 80)
print(f"\nOriginal listing:")
print(f"Title: {test_listing['title']}")
print(f"Price: {test_listing['price']}")
print(f"Location: {test_listing['location']}")
print(f"\nGenerating Russian summary...\n")

response = client.chat.completions.create(
    model=config['llm']['zhipu']['model'],
    messages=[{"role": "user", "content": prompt}],
    temperature=0.3,
    max_tokens=200
)

summary = response.choices[0].message.content.strip()

print("=" * 80)
print("Russian Summary:")
print("=" * 80)
print(summary)
print("\n" + "=" * 80)
