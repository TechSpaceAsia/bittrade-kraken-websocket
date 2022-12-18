from reactivex import Observer
from logging import getLogger
from rich.console import Console
import traceback
import sys

logger = getLogger(__name__)
console = Console()

def debug_observer(prefix: str):
    def fn(x, scope):
        logger.debug(f'[{prefix}][{scope}] - {x}')
        if scope == 'ERROR':
            traceback.print_exception(*sys.exc_info())

    return Observer(
        lambda x: fn(x, 'NEXT'),
        lambda x: fn(x, 'ERROR') and console.print_exception(),
        lambda: fn(None, 'COMPLETE'),
    )


def info_observer(prefix: str):
    def fn(x, scope):
        logger.info(f'[{prefix}][{scope}] - {x}')

    return Observer(
        lambda x: fn(x, 'NEXT'),
        lambda x: fn(x, 'ERROR'),
        lambda: fn(None, 'COMPLETE'),
    )