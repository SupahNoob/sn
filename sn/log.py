import logging


_level_name  = '%(levelname)'
_datetime    = '%(asctime)'
_logger_name = '%(name)'
_log_message = '%(message)'
_path_name   = '%(pathname)'
_file_name   = '%(filename)'
_module_name = '%(module)'
_func_name   = '%(funcName)'
_line_number = '%(lineno)'


# formats must still specify customizations!
#
#    %.2s  means "truncate the string to 2 characters"
#    %10s  means "right-align the string to 10 characters"
#    %-10s means "left-align the string to 10 characters"
#
NOTEBOOK_LOG_FMT = f'[{_level_name}.1s] {_datetime}s | {_logger_name}s | {_file_name}s#L{_line_number}4s {_func_name}s | {_log_message}s'
FILE_LOG_FMT = f'[{_level_name}-8s] {_datetime}s - {_logger_name}s - {_module_name}s.{_func_name}s#L{_line_number}4s - {_log_message}s'


def basic_setup(
    name: str='local_script',
    *,
    format: str=NOTEBOOK_LOG_FMT,
    datefmt: str='%H:%M:%S',
    level: str='INFO'
):
    """
    Applies some recommended logging principles.
    
    A StreamHandler will be set up with the format being defined and the date
    format ommitting the actual date. StreamHandlers are typically used to get a
    quick understanding of how the program is performing, and if you've
    forgotten what date it is, we might have a problem. ;)

    Parameters
    ----------
    name : str = [default: 'local_script']
        name of the returned logger

    format : str = [default: log.NOTEBOOK_LOG_FMT]
        LogRecord format to generate messages in

    datefmt : str = [default: '%H%M%S']
        format of the datetime (asctime) in LogRecords

    level : str = [default: 'INFO']
        log level name

    Returns
    -------
    log : logging.Logger
    """
#     if 
    
    log = logging.getLogger(name)
    logging.basicConfig(format=format, datefmt=datefmt, level=level)
    return log


class LazyStr:
    """
    Defer evaluation of str(some_func()).
    
    The standard library Logging package defers string evaluation until the
    moment the LogRecord is actually created. By contrast, Python's 3.6+
    fstrings perform eager-evaluation of strings. If you were to call some_func
    within an fstring, it would be evaluated and then the string formatted. The
    Logging package performs the opposite operation... but how does that work
    with callables?
    
    This is the problem LazyStr solves.
    
    Attributes
    ----------
    fn : callable
        the callable to evaluate

    *args, **kwargs
        any position or keyword arguments to pass to fn
    """
    __slots__ = ('fn', 'args', 'kwargs')
    
    def __init__(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return f'{self.fn(*self.args, **self.kwargs)}'
