from reactivex.testing.subscription import Subscription


def from_to(start, end):
    return Subscription(float(start), float(end))