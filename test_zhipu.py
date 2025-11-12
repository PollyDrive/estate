#!/usr/bin/env python3
"""
Простой тест для Zhipu AI (GLM-4)
"""

import os
from dotenv import load_dotenv
from zhipuai import ZhipuAI

# Загружаем переменные окружения
load_dotenv()

# Получаем API ключ
api_key = os.getenv('ZHIPU_API_KEY')

if not api_key:
    print("❌ Ошибка: ZHIPU_API_KEY не найден в переменных окружения!")
    exit(1)

# Создаем клиент
client = ZhipuAI(api_key=api_key)

print("🤖 Отправляем запрос к GLM-4...")

# Делаем запрос к модели
response = client.chat.completions.create(
    model="glm-4",
    messages=[
        {"role": "user", "content": "Привет, как тебя зовут?"}
    ]
)

# Получаем ответ
answer = response.choices[0].message.content

# Выводим на экран
print(f"\n✅ Ответ модели:\n{answer}\n")
