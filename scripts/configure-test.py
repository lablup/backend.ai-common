from pathlib import Path

test_sets = {
    'src/ai/backend/common/redis': 'redis',
    'src/ai/backend/common': 'general',
    'tests/redis': 'redis',
    'tests': 'general',
}
active_sets = set()

with open('.changed-files.txt', 'r') as f:
    for line in f:
        for k, v in test_sets.items():
            if line.startswith(k):
                active_sets.add(v)

for item in active_sets:
    Path(f'.{item}.do-test').touch()
