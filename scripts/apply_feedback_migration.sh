#!/bin/bash
# Safe migration script for feedback_cycle
# Applies migration with pre-checks and rollback capability

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================="
echo "Feedback Cycle Migration Script"
echo "========================================="
echo ""

# 1. Check environment
if [ -z "$DATABASE_URL" ]; then
    echo -e "${RED}ERROR: DATABASE_URL not set${NC}"
    echo "Load environment first:"
    echo "  export \$(cat .env | grep -v '^#' | xargs)"
    exit 1
fi

if [ -z "$POSTGRES_SCHEMA" ]; then
    echo -e "${YELLOW}WARNING: POSTGRES_SCHEMA not set, defaulting to '_4BR'${NC}"
    export POSTGRES_SCHEMA="_4BR"
fi

echo -e "${GREEN}✓${NC} Environment loaded"
echo "  Schema: $POSTGRES_SCHEMA"
echo ""

# 2. Pre-migration checks
echo "Running pre-migration checks..."

psql "$DATABASE_URL" -t -c "
SET search_path TO \"$POSTGRES_SCHEMA\", public;
SELECT
    'Listings: ' || COUNT(*)::text
FROM listings;
" | grep -v '^$'

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Pre-check failed: Cannot connect to database${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} Database accessible"
echo ""

# 3. Check if migration already applied
echo "Checking if migration already applied..."

FEEDBACK_EXISTS=$(psql "$DATABASE_URL" -t -c "
SELECT EXISTS(
    SELECT 1 FROM pg_tables
    WHERE schemaname = '$POSTGRES_SCHEMA'
      AND tablename = 'feedback'
);" | tr -d ' ')

if [ "$FEEDBACK_EXISTS" = "t" ]; then
    echo -e "${YELLOW}⚠ Migration appears to be already applied (feedback table exists)${NC}"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Migration cancelled"
        exit 0
    fi
fi

# 4. Create backup
echo "Creating backup..."
BACKUP_FILE="backups/backup_before_feedback_$(date +%Y%m%d_%H%M%S).sql"
mkdir -p backups

psql "$DATABASE_URL" -f backups/backup_tables_before_feedback.sql > "$BACKUP_FILE" 2>&1

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC} Backup created: $BACKUP_FILE"
else
    echo -e "${RED}✗ Backup failed!${NC}"
    exit 1
fi
echo ""

# 5. Apply migration
echo "Applying migration..."
echo "  File: db/migration_feedback_cycle.sql"
echo ""

psql "$DATABASE_URL" -f db/migration_feedback_cycle.sql

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ Migration applied successfully!${NC}"
else
    echo ""
    echo -e "${RED}✗ Migration failed!${NC}"
    echo ""
    echo "To rollback:"
    echo "  psql \"\$DATABASE_URL\" -c \"DROP TABLE IF EXISTS \\\"$POSTGRES_SCHEMA\\\".feedback;\""
    echo "  psql \"\$DATABASE_URL\" -c \"DROP TABLE IF EXISTS \\\"$POSTGRES_SCHEMA\\\".batch_runs;\""
    echo "  psql \"\$DATABASE_URL\" -c \"ALTER TABLE \\\"$POSTGRES_SCHEMA\\\".listings DROP COLUMN IF EXISTS telegram_message_id;\""
    echo "  psql \"\$DATABASE_URL\" -c \"ALTER TABLE \\\"$POSTGRES_SCHEMA\\\".listings DROP COLUMN IF EXISTS llm_model;\""
    exit 1
fi

# 6. Post-migration verification
echo ""
echo "Verifying migration..."

VERIFICATION=$(psql "$DATABASE_URL" -t -c "
SET search_path TO \"$POSTGRES_SCHEMA\", public;

SELECT
    'feedback table: ' || EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = '$POSTGRES_SCHEMA' AND tablename = 'feedback')::text || '\n' ||
    'batch_runs table: ' || EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = '$POSTGRES_SCHEMA' AND tablename = 'batch_runs')::text || '\n' ||
    'telegram_message_id column: ' || EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = '$POSTGRES_SCHEMA' AND table_name = 'listings' AND column_name = 'telegram_message_id')::text || '\n' ||
    'llm_model column: ' || EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = '$POSTGRES_SCHEMA' AND table_name = 'listings' AND column_name = 'llm_model')::text
;
")

echo "$VERIFICATION" | grep -v '^$'

ALL_TRUE=$(echo "$VERIFICATION" | grep -c "true")

if [ "$ALL_TRUE" -eq 4 ]; then
    echo ""
    echo -e "${GREEN}✓ All components verified successfully!${NC}"
    echo ""
    echo "========================================="
    echo "Migration Complete!"
    echo "========================================="
    echo ""
    echo "Next steps:"
    echo "  1. Install aiogram: pip install aiogram"
    echo "  2. Start feedback bot: python src/feedback_bot.py"
    echo "  3. Run stage5: python scripts/run_stage5.py"
    echo ""
    echo "Documentation:"
    echo "  - Quick start: docs/feedback_cycle_quickstart.md"
    echo "  - Full setup: docs/feedback_cycle_setup.md"
    echo ""
else
    echo ""
    echo -e "${YELLOW}⚠ Verification incomplete. Check output above.${NC}"
fi
