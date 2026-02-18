#!/usr/bin/env python3
"""
Apply feedback_cycle migration with backup
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Colors
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
NC = '\033[0m'

def run_sql(sql_file=None, sql_command=None):
    """Run SQL file or command using psql"""
    db_url = os.getenv('DATABASE_URL')

    if not db_url:
        print(f"{RED}ERROR: DATABASE_URL not set{NC}")
        sys.exit(1)

    if sql_file:
        cmd = ['psql', db_url, '-f', sql_file]
    elif sql_command:
        cmd = ['psql', db_url, '-c', sql_command]
    else:
        raise ValueError("Either sql_file or sql_command required")

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0, result.stdout, result.stderr


def main():
    print("=" * 50)
    print("Feedback Cycle Migration Script")
    print("=" * 50)
    print()

    schema = os.getenv('POSTGRES_SCHEMA', '_4BR')
    db_url = os.getenv('DATABASE_URL')

    if not db_url:
        print(f"{RED}✗ DATABASE_URL not set{NC}")
        print("Make sure .env file exists and contains DATABASE_URL")
        sys.exit(1)

    print(f"{GREEN}✓{NC} Environment loaded")
    print(f"  Schema: {schema}")
    print()

    # 1. Pre-migration check
    print("Running pre-migration check...")

    success, stdout, stderr = run_sql(sql_command=f"""
        SET search_path TO "{schema}", public;
        SELECT COUNT(*) as listings_count FROM listings;
    """)

    if not success:
        print(f"{RED}✗ Cannot connect to database{NC}")
        print(stderr)
        sys.exit(1)

    print(f"{GREEN}✓{NC} Database accessible")
    print()

    # 2. Check if already applied
    print("Checking if migration already applied...")

    success, stdout, stderr = run_sql(sql_command=f"""
        SELECT EXISTS(
            SELECT 1 FROM pg_tables
            WHERE schemaname = '{schema}'
              AND tablename = 'feedback'
        );
    """)

    if 't' in stdout:
        print(f"{YELLOW}⚠ Migration appears already applied (feedback table exists){NC}")
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            print("Migration cancelled")
            sys.exit(0)

    print(f"{GREEN}✓{NC} Ready to apply migration")
    print()

    # 3. Create backup
    print("Creating backup...")

    backup_dir = Path("backups")
    backup_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = backup_dir / f"backup_before_feedback_{timestamp}.sql"

    success, stdout, stderr = run_sql(sql_file="backups/backup_tables_before_feedback.sql")

    # Save output
    with open(backup_file, 'w') as f:
        f.write(f"-- Backup before feedback_cycle migration\n")
        f.write(f"-- Date: {datetime.now()}\n")
        f.write(f"-- Schema: {schema}\n\n")
        f.write(stdout)

    print(f"{GREEN}✓{NC} Backup created: {backup_file}")
    print()

    # 4. Apply migration
    print("Applying migration...")
    print("  File: db/migration_feedback_cycle.sql")
    print()

    success, stdout, stderr = run_sql(sql_file="db/migration_feedback_cycle.sql")

    if not success:
        print(f"{RED}✗ Migration failed!{NC}")
        print(stderr)
        print()
        print("To rollback:")
        print(f"  DROP TABLE IF EXISTS \"{schema}\".feedback;")
        print(f"  DROP TABLE IF EXISTS \"{schema}\".batch_runs;")
        print(f"  ALTER TABLE \"{schema}\".listings DROP COLUMN IF EXISTS telegram_message_id;")
        print(f"  ALTER TABLE \"{schema}\".listings DROP COLUMN IF EXISTS llm_model;")
        sys.exit(1)

    print(stdout)
    print(f"{GREEN}✓ Migration applied successfully!{NC}")
    print()

    # 5. Verification
    print("Verifying migration...")

    success, stdout, stderr = run_sql(sql_command=f"""
        SET search_path TO "{schema}", public;

        SELECT
            'feedback table: ' ||
            EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = '{schema}' AND tablename = 'feedback')::text AS feedback,
            'batch_runs table: ' ||
            EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = '{schema}' AND tablename = 'batch_runs')::text AS batch_runs,
            'telegram_message_id column: ' ||
            EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = 'listings' AND column_name = 'telegram_message_id')::text AS msg_id,
            'llm_model column: ' ||
            EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = 'listings' AND column_name = 'llm_model')::text AS llm_model
        ;
    """)

    print(stdout)

    if stdout.count('true') >= 4:
        print(f"{GREEN}✓ All components verified successfully!{NC}")
        print()
        print("=" * 50)
        print("Migration Complete!")
        print("=" * 50)
        print()
        print("Next steps:")
        print("  1. Install aiogram: pip install aiogram")
        print("  2. Start feedback bot: python src/feedback_bot.py")
        print("  3. Run stage5: python scripts/run_stage5.py")
        print()
        print("Documentation:")
        print("  - Quick start: docs/feedback_cycle_quickstart.md")
        print("  - Full setup: docs/feedback_cycle_setup.md")
    else:
        print(f"{YELLOW}⚠ Verification incomplete{NC}")


if __name__ == '__main__':
    main()
