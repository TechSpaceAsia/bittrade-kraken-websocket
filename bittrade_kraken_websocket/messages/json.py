from typing import Dict, List

import orjson
from reactivex import operators, Observable


def to_json():
    """This operator drops messages that can't be json decoded."""

    def _to_json(source: Observable[str]) -> Observable[Dict | List]:
        def subscribe(observer, scheduler=None):
            def on_next(value):
                try:
                    observer.on_next(orjson.loads(value))
                except orjson.JSONDecodeError:
                    pass

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler)

        return Observable(subscribe)

    return _to_json
