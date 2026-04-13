from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine
import sqlite3

settings = Settings(feishu_webhook_url='https://example.com/hook')
engine = DataEngine(settings)

symbols = engine.get_all_symbols()[:30]
print('SYMBOLS_TO_SYNC=', len(symbols))
summary = engine.sync_all(symbols)
print('SUMMARY=', summary)

conn = sqlite3.connect(settings.db_path)
cur = conn.cursor()
count = cur.execute('SELECT COUNT(*) FROM stock_daily').fetchone()[0]
print('TOTAL_ROWS=', count)
conn.close()
