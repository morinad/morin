from .clickhouse import Clickhouse


def make_db(subd, bot_token, chat_list, message_type, host, port, username, password, database, start, add_name, err429, backfill_days, platform):
    subd_lower = (subd or '').strip().lower()
    if subd_lower in ('postgres', 'postgresql', 'pg'):
        from .postgres import Postgres
        return Postgres(bot_token, chat_list, message_type, host, port, username, password, database, start, add_name, err429, backfill_days, platform)
    return Clickhouse(bot_token, chat_list, message_type, host, port, username, password, database, start, add_name, err429, backfill_days, platform)
