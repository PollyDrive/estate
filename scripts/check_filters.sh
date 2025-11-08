#!/bin/bash
# Скрипт для проверки результатов фильтрации

cd /Users/krvtzz/Code/estate

echo "═══════════════════════════════════════════════════════════"
echo "📊 СТАТИСТИКА ФИЛЬТРАЦИИ"
echo "═══════════════════════════════════════════════════════════"
echo ""

# Общая статистика
echo "🔢 Всего обработано объявлений:"
docker-compose logs bot 2>&1 | grep "Total listings processed:" | tail -1

echo ""
echo "🆕 Новых объявлений найдено:"
docker-compose logs bot 2>&1 | grep "New listings:" | tail -1

echo ""
echo "✅ Отправлено в Telegram:"
docker-compose logs bot 2>&1 | grep "Notifications sent:" | tail -1

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "🔍 ДЕТАЛИ ФИЛЬТРАЦИИ"
echo "═══════════════════════════════════════════════════════════"
echo ""

# Level 0
echo "📍 Level 0 (базовые фильтры):"
echo "   PASSED: $(docker-compose logs bot 2>&1 | grep -c "Level 0 PASSED")"
echo "   FAILED: $(docker-compose logs bot 2>&1 | grep -c "Level 0 FAILED")"

echo ""
echo "   Причины отклонения Level 0:"
docker-compose logs bot 2>&1 | grep "Level 0 FAILED:" | sed 's/.*FAILED: /     - /' | sort | uniq -c | sort -rn

echo ""

# Level 1 (Groq)
echo "🤖 Level 1 (Groq - проверка кухни, AC, WiFi):"
echo "   PASSED: $(docker-compose logs bot 2>&1 | grep -c "Level 1 PASSED")"
echo "   FAILED: $(docker-compose logs bot 2>&1 | grep -c "Level 1 FAILED")"

echo ""
echo "   Причины отклонения Level 1:"
docker-compose logs bot 2>&1 | grep "Level 1 FAILED:" | sed 's/.*FAILED: /     - /' | sort | uniq -c | sort -rn

echo ""

# Level 2 (Claude)
echo "🧠 Level 2 (Claude - детальный анализ):"
echo "   PASSED: $(docker-compose logs bot 2>&1 | grep -c "Level 2 PASSED")"
echo "   FAILED: $(docker-compose logs bot 2>&1 | grep -c "Level 2 FAILED")"

echo ""
echo "   Причины отклонения Level 2:"
docker-compose logs bot 2>&1 | grep "Level 2 FAILED:" | sed 's/.*FAILED: /     - /' | sort | uniq -c | sort -rn

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "📞 ТЕЛЕФОНЫ"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "Найдено телефонных номеров: $(docker-compose logs bot 2>&1 | grep -c "Phone number found:")"

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "💾 БАЗА ДАННЫХ"
echo "═══════════════════════════════════════════════════════════"
echo ""

docker-compose exec postgres psql -U realty_user -d realty_bot -c "
SELECT 
    COUNT(*) as total_listings,
    COUNT(CASE WHEN sent_to_telegram = true THEN 1 END) as sent_to_telegram,
    COUNT(CASE WHEN phone_number IS NOT NULL THEN 1 END) as with_phone
FROM fb_listings;
" 2>/dev/null | grep -v "^-" | grep -v "row"

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "Используйте: docker-compose logs bot | grep 'Level 1 PASSED' -B 5"
echo "для просмотра объявлений, прошедших фильтр"
echo "═══════════════════════════════════════════════════════════"
