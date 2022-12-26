from os import getcwd
from typing import List
from pathlib import Path
from reactivex.testing import ReactiveTest
import orjson

def from_sample(filename: str, start=0, end=100000, start_time=200, time_interval=10) -> List:
    path = Path(f'{getcwd()}/tests/samples/{filename}')
    extension = path.suffix[1:]
    if extension == 'jsonl':
        items = [orjson.loads(line) for line in path.read_text().splitlines()[start:end]]
    elif extension == 'json':
        items = orjson.loads(path.read_text())
    return [
        ReactiveTest.on_next(start_time + (i+1)*time_interval, item)
        for i, item in enumerate(
            items
        )
    ]