from typing import Union
import functools
import logging
from fitanalysis.Configuration import Configuration

class Logger:
    """
    A class to create and manage loggers.
    @author: Ankit Sinha
    ...

    Methods
    -------
    get_logger(name=None)
        Returns a logger with the specified name.
    """
    _logging_level = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL,
        'NOTSET': logging.NOTSET,
        'OFF': 'OFF'
    }

    def __init__(self):
        """
        Initializes the Logger class with default logging level.
        """
        cfg = Configuration.get_config()
        level = cfg("Logger","level")
        if not level:
            level = logging.DEBUG
        elif level in self._logging_level.keys():
            level = self._logging_level[level]
        else:
            raise KeyError(f"**** ERROR: Logging level {level} not recognized. Please check the configuration file.")
        
        fn=cfg("Logger","filename")
       
        if cfg('Logger','format'):
            format=cfg('Logger','format')
        else:
            format="%(asctime)s;%(levelname)s;%(message)s"
        if cfg('Logger','datefmt'):
            datefmt=cfg('Logger','datefmt')
        else:    
            datefmt="%Y-%m-%d %H:%M:%S"
        if fn:
            logging.basicConfig(filename=fn, encoding='utf-8', level=level, format=format, datefmt=datefmt)
        else:
            logging.basicConfig(level=level, format=format, datefmt=datefmt)

    @staticmethod
    def get_logging_level(level:str):
        """
        Returns the logging level for the specified level.

        Parameters
        ----------
        level : str
            The level of the message.

        Returns
        -------
        logging_level : int
            The logging level.
        """
        return Logger._logging_level[level.upper()]
    
    def get_logger(self, name=None):
        """
        Returns a logger with the specified name.

        Parameters
        ----------
        name : str, optional
            The name of the logger. If not specified, returns the root logger.

        Returns
        -------
        logger : logging.Logger
            The logger object.
        """
        return logging.getLogger(name)
    
    
def get_default_logger():
    """
    Returns a default logger.

    Returns
    -------
    logger : logging.Logger
        The default logger object.
    """
    return Logger().get_logger()

def logmsg(msg, level='INFO', my_logger: Union[Logger, logging.Logger] = None):
    """
    Logs a message with the specified level.

    Parameters
    ----------
    msg : str
        The message to be logged.
    level : str, optional
        The level of the message. Defaults to INFO.
    my_logger : Logger or logging.Logger, optional
        The logger object to be used. If not specified, uses the default logger.
    """
    if my_logger is None:
        logger = get_default_logger()
    else:
        if isinstance(my_logger, Logger):
            logger = my_logger.get_logger()
        else:
            logger = my_logger
    level = Logger.get_logging_level(level)
    logger.log(level, msg)


def log(_func=None, *, my_logger: Union[Logger, logging.Logger] = None):
    """
    A decorator to log function calls and exceptions.

    If no logger is specified, uses the default logger. 
    Can be used with the logger as the argument @log(my_logger=MyLogger())

    @author: Ankit Sinha

    Parameters
    ----------
    _func : function, optional
        The function to be decorated.
    my_logger : Logger or logging.Logger, optional
        The logger object to be used. If not specified, uses the default logger.

    Returns
    -------
    wrapper : function
        The decorated function.
    """
    def decorator_log(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if my_logger is None:
                logger = get_default_logger()
            else:
                if isinstance(my_logger, Logger):
                    logger = my_logger.get_logger(func.__name__)
                else:
                    logger = my_logger
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)
            logger.debug(f"function {func.__name__} called with args {signature}")
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
                raise e
        return wrapper

    if _func is None:
        return decorator_log
    else:
        return decorator_log(_func)