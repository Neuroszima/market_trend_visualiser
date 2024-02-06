from typing import Callable


class time_interval_sanitizer:
    """
    class decorator that checks if time_interval function argument happens to be one of the allowed values
    """
    ALLOWED_INTERVALS = ["1min", "1day"]

    def __call__(self, function: Callable):
        def function_wrapper(*args, **kwargs):
            print(args, kwargs)
            # arg check, and following kwarg check if arg does not find anything
            if "time_interval" not in kwargs:
                if args[-1] in self.ALLOWED_INTERVALS:
                    result = function(*args, **kwargs)
                    return result
                raise ValueError("Improper argument for this query. Possible intervals: ('1min', '1day')")
            else:
                if kwargs["time_interval"] not in self.ALLOWED_INTERVALS:
                    raise ValueError("Improper argument for this query. Possible intervals: ('1min', '1day')")
            result = function(*args, **kwargs)
            return result

        return function_wrapper