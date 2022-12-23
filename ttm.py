import time
from functools import wraps


def measure_time(func):
    @wraps(func)
    def warapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter() - start
        print(f'{func.__name__} elapced in {end:.6f} sec')
        return result

    return warapper
