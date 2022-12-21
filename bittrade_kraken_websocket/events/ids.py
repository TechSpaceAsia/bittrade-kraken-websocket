import uuid

import reactivex


def gen():
    while True:
        yield uuid.uuid4().int & (1 << 64) - 1
id_generator = reactivex.from_iterable(gen())