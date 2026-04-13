from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine
import time, json

settings = Settings(feishu_webhook_url='https://example.com/hook')
engine = DataEngine(settings)

symbols = []
for i in range(8):
    symbols = engine.get_all_symbols()
    if symbols:
        break
    print(f'get_all_symbols retry {i+1}/8')
    time.sleep(5)

if not symbols:
    raise SystemExit('获取全市场代码失败，请稍后重试')

symbols = sorted(set(symbols))
print('TOTAL_SYMBOLS=', len(symbols))

ok = skip = fail = 0
failed = []

for idx, s in enumerate(symbols, start=1):
    r = engine.sync_symbol(s)
    if r.status == 'success':
        ok += 1
    elif r.status == 'skip':
        skip += 1
    else:
        fail += 1
        failed.append(s)

    if idx % 200 == 0:
        print('PROGRESS=', {'done': idx, 'total': len(symbols), 'success': ok, 'skip': skip, 'fail': fail})

print('SUMMARY=', {'success': ok, 'skip': skip, 'fail': fail})
with open('data/failed_symbols_20260313.json', 'w', encoding='utf-8') as f:
    json.dump(failed, f, ensure_ascii=False, indent=2)
print('failed saved to data/failed_symbols_20260313.json')
