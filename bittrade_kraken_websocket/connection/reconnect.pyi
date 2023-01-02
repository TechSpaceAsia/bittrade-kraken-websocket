from _typeshed import Incomplete
from collections.abc import Generator
from reactivex import Observable
from typing import Callable, Optional

def kraken_patterns() -> Generator[Incomplete, None, None]: ...
def retry_with_backoff(stabilized: Optional[Observable] = ..., delays_pattern: Callable[[], Generator[float, None, None]] = ...): ...
