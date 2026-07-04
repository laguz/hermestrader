import pytest
from hermes.db.repositories.logs import LogsRepository

@pytest.fixture
def schema_db(make_db):
    return make_db(schema=True)

async def test_logs_flag_orphans_bulk_uniqueness(schema_db):
    repo = LogsRepository(schema_db)
    
    # We pass a set of 30 symbols to flag_orphans.
    # Without monotonic unique timestamp generator, this would raise a UniqueViolation error in Postgres.
    symbols = {f"SYM_{i}" for i in range(30)}
    await repo.flag_orphans(symbols)
    
    # Verify they were all written successfully
    logs = await repo.recent_logs(limit=100)
    for sym in symbols:
        assert f"orphan position: {sym}" in logs
