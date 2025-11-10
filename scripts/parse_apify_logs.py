import re
import json
import os
from pathlib import Path

def clean_description_line(line):
    """
    Убирает timestamp из строки description
    Пример: "2025-11-09T11:59:45.069Z       'text..." -> "text..."
    """
    # Удаляем timestamp в начале строки
    cleaned = re.sub(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\s+', '', line)
    
    # Убираем артефакты конкатенации строк из логов
    # Примеры: "\\n' +", "' +", "+ '", просто '
    cleaned = cleaned.strip()
    cleaned = re.sub(r"\\n'\s*\+\s*$", '', cleaned)  # Убираем \n' + в конце
    cleaned = re.sub(r"^'\s*\+\s*$", '', cleaned)     # Убираем ' + (пустая строка)
    cleaned = re.sub(r"^\+\s*'", '', cleaned)         # Убираем + ' в начале
    cleaned = cleaned.strip("'").strip()              # Убираем кавычки по краям
    
    # Заменяем \\n на настоящий перевод строки
    cleaned = cleaned.replace('\\n', '\n')
    
    return cleaned

def parse_apify_log(log_file_path):
    """
    Парсит лог-файл Apify и извлекает сопоставления fb_id -> {title, description}
    
    Логика:
    1. Найти [GET_ITEM_DETAILS] entering... URL 200
    2. Проверить, нет ли ERROR/RETRY до следующего moreDetails
    3. Извлечь description из moreDetails
    4. Сохранить сопоставление
    """
    results = []
    
    current_fb_id = None
    expecting_details = False
    has_error = False
    collecting_description = False
    description_lines = []
    
    # Паттерны
    pattern_entering = re.compile(r'\[GET_ITEM_DETAILS\] entering\.\.\.\s*https://www\.facebook\.com/marketplace/item/(\d+)\s+200')
    pattern_error = re.compile(r'(ERROR|RETRY)', re.IGNORECASE)
    pattern_more_details = re.compile(r'moreDetails:\s*\{')
    pattern_description = re.compile(r"description:\s*'(.*)$")
    pattern_description_end = re.compile(r"^(.*)',?\s*$")
    
    with open(log_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            
            # Новый запрос GET_ITEM_DETAILS
            match_entering = pattern_entering.search(line)
            if match_entering:
                # Сохраняем предыдущий результат, если был
                if current_fb_id and expecting_details and not has_error and description_lines:
                    full_description = '\n'.join(description_lines).strip()
                    results.append({
                        'fb_id': current_fb_id,
                        'description': full_description
                    })
                
                # Начинаем новый
                current_fb_id = match_entering.group(1)
                expecting_details = True
                has_error = False
                collecting_description = False
                description_lines = []
                continue
            
            # Проверяем на ошибки
            if expecting_details and pattern_error.search(line):
                has_error = True
                expecting_details = False
                continue
            
            # Начало moreDetails
            if expecting_details and pattern_more_details.search(line):
                # moreDetails найден, теперь ищем description
                continue
            
            # Ищем строку с description
            if expecting_details and not collecting_description:
                match_desc = pattern_description.search(line)
                if match_desc:
                    collecting_description = True
                    desc_text = match_desc.group(1)
                    
                    # Проверяем, заканчивается ли description на этой же строке
                    if desc_text.endswith("',") or desc_text.endswith("'"):
                        cleaned = clean_description_line(desc_text.rstrip("',"))
                        if cleaned:
                            description_lines.append(cleaned)
                        # Description закончен
                        full_description = '\n'.join(description_lines).strip()
                        results.append({
                            'fb_id': current_fb_id,
                            'description': full_description
                        })
                        # Сброс
                        expecting_details = False
                        collecting_description = False
                        description_lines = []
                    else:
                        cleaned = clean_description_line(desc_text)
                        if cleaned:
                            description_lines.append(cleaned)
                continue
            
            # Продолжаем собирать многострочный description
            if collecting_description:
                # Проверяем, это последняя строка description?
                if line.endswith("',") or (line.endswith("'") and not line.endswith("\\'")):
                    cleaned = clean_description_line(line.rstrip("',"))
                    if cleaned:  # Добавляем только непустые строки
                        description_lines.append(cleaned)
                    # Description закончен
                    full_description = '\n'.join(description_lines).strip()
                    results.append({
                        'fb_id': current_fb_id,
                        'description': full_description
                    })
                    # Сброс
                    expecting_details = False
                    collecting_description = False
                    description_lines = []
                else:
                    cleaned = clean_description_line(line)
                    if cleaned:  # Добавляем только непустые строки
                        description_lines.append(cleaned)
    
    # Обработка последнего элемента, если файл закончился
    if current_fb_id and expecting_details and not has_error and description_lines:
        full_description = '\n'.join(description_lines).strip()
        results.append({
            'fb_id': current_fb_id,
            'description': full_description
        })
    
    return results


def main():
    # Папка с логами
    logs_dir = Path('apify_logs')
    
    if not logs_dir.exists():
        print(f"Папка {logs_dir} не найдена. Создай папку и положи туда лог-файлы.")
        return
    
    # Находим все текстовые файлы в папке
    log_files = list(logs_dir.glob('*.txt')) + list(logs_dir.glob('*.log'))
    
    if not log_files:
        print(f"В папке {logs_dir} нет лог-файлов (.txt или .log)")
        return
    
    print(f"Найдено лог-файлов: {len(log_files)}")
    
    all_results = {}
    
    for log_file in log_files:
        print(f"\nОбработка: {log_file.name}")
        results = parse_apify_log(log_file)
        print(f"  Извлечено записей: {len(results)}")
        
        # Группируем по файлу
        all_results[log_file.name] = results
    
    # Сохраняем результаты
    output_file = 'parsed_apify_logs.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ Результаты сохранены в {output_file}")
    
    # Статистика
    total = sum(len(results) for results in all_results.values())
    print(f"📊 Всего извлечено записей: {total}")
    
    # Показываем первые 2 записи для проверки
    print("\n📝 Пример первых записей:")
    for filename, results in all_results.items():
        if results:
            print(f"\n{filename}:")
            for item in results[:2]:
                print(f"  fb_id: {item['fb_id']}")
                print(f"  description: {item['description'][:100]}...")
            break


if __name__ == '__main__':
    main()
