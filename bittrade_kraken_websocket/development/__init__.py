from reactivex import Observer
from logging import getLogger

logger = getLogger(__name__)


def debug_observer(prefix: str):
    def fn(x, scope):
        logger.debug(f'[{prefix}][{scope}] - {x}')

    return Observer(
        lambda x: fn(x, 'NEXT'),
        lambda x: fn(x, 'ERROR'),
        lambda x: fn(x, 'COMPLETE'),
    )


def info_observer(prefix: str):
    def fn(x, scope):
        logger.info(f'[{prefix}][{scope}] - {x}')

    return Observer(
        lambda x: fn(x, 'NEXT'),
        lambda x: fn(x, 'ERROR'),
        lambda: fn(None, 'COMPLETE'),
    )