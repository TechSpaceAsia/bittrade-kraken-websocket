from reactivex import compose
from reactivex.operators import publish, ref_count


def share():
    return compose(
        publish(),
        ref_count()
    )