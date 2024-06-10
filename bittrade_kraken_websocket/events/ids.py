import uuid
from datetime import datetime

import reactivex


def gen():
    while True:
        yield uuid.uuid4().int & (1 << 64) - 1


def gen_seq():
    start = 1
    while True:
        current = datetime.now().strftime("%H%m%d")
        yield f"{current}{start}"
        start = start + 1
        if start > 999:
            start = 0


id_iterator = gen()
id_seq_iterator = gen_seq()
id_generator = reactivex.from_iterable(id_iterator)
