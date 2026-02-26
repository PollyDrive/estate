"""
Pipeline statistics builder.
Used by /stats command and optionally by stage5.
"""


def build_pipeline_stats_message(db, chat_id: int) -> str:
    """
    Build cumulative pipeline stats message for a specific chat profile.
    Shows full breakdown by stage so all counts add up.
    """
    db.cursor.execute("SELECT COUNT(*) FROM listings")
    listings_total = int(db.cursor.fetchone()[0] or 0)

    db.cursor.execute("SELECT COUNT(*) FROM listing_non_relevant")
    non_relevant = int(db.cursor.fetchone()[0] or 0)

    db.cursor.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE status IN ('stage1', 'stage1_new', 'stage2')) AS early,
            COUNT(*) FILTER (WHERE status = 'stage2_failed') AS filter_failed,
            COUNT(*) FILTER (WHERE status = 'stage3_failed') AS llm_failed,
            COUNT(*) FILTER (WHERE status = 'stage3_room_only') AS room_only,
            COUNT(*) FILTER (WHERE status = 'stage4') AS stage4,
            COUNT(*) FILTER (WHERE status = 'stage4_duplicate') AS dup,
            COUNT(*) FILTER (WHERE status = 'stage5_sent') AS sent
        FROM listings
        """
    )
    row = db.cursor.fetchone() or (0, 0, 0, 0, 0, 0, 0)
    early, filter_failed, llm_failed, room_only, stage4, dup, sent = [int(x or 0) for x in row]

    db.cursor.execute(
        "SELECT COUNT(*) FROM listing_profiles WHERE chat_id = %s AND sent_at IS NOT NULL",
        (chat_id,)
    )
    sent_this_chat = int(db.cursor.fetchone()[0] or 0)

    total = listings_total + non_relevant
    return (
        f"Всего: {total} (listings: {listings_total} + non_relevant: {non_relevant})\n\n"
        "Этапы:\n"
        f"• не попали в pipeline: {non_relevant}\n"
        f"• в очереди (stage1/2): {early}\n"
        f"• не прошли автофильтры: {filter_failed}\n"
        f"• не прошли LLM: {llm_failed}\n"
        f"• room-only: {room_only}\n"
        f"• прошли всё, ждут отправки: {stage4}\n"
        f"• дубликатов: {dup}\n"
        f"• отправлено (всего): {sent}\n"
        f"• в этот чат: {sent_this_chat}"
    )
