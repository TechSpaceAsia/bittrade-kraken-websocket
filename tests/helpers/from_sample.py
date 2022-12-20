from os import getcwd
from typing import List
from pathlib import Path
from reactivex.testing import ReactiveTest
import orjson

def from_sample(filename: str, start=0, end=100000, start_time=200) -> List:
    return [
        ReactiveTest.on_next(start_time + (i+1)*10, orjson.loads(line))
        for i, line in enumerate(
            Path(f'{getcwd()}/tests/samples/{filename}').read_text().splitlines()[start:end]
        )
    ]