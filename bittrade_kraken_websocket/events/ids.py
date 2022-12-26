import uuid

import reactivex


def gen():
    while True:
        yield uuid.uuid4().int & (1 << 64) - 1

id_iterator = gen()
id_generator = reactivex.from_iterable(id_iterator)
