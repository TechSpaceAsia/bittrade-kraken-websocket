from reactivex import operators

HEARTBEAT = '{"event":"heartbeat"}'

def _is_not_heartbeat(m: str):
    return m != HEARTBEAT
def ignore_heartbeat():
    return operators.filter(_is_not_heartbeat)