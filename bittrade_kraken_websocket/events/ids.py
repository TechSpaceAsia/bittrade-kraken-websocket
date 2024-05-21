import uuid
from datetime import datetime

import reactivex


def gen():
    while True:
        yield uuid.uuid4().int & (1 << 64) - 1


def gen_seq():
    start = 1
    while True:
        current = datetime.now().strftime("%Y%m%d%H%M")
        yield f"{current}{start}"
        start = start + 1


id_iterator = gen()
id_seq_iterator = gen_seq()
id_generator = reactivex.from_iterable(id_iterator)
