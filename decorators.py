import time
import logging
from functools import wraps

def retry(retries=3, delay=5, logger=None):
    """
    Декоратор для автоматической реконнекции при возникновении ошибки.
    
    :param retries: максимальное количество попыток выполнения функции
    :param delay: задержка между попытками в секундах
    :param logger: логгер для вывода сообщений, если None, создается стандартный логгер
    """
    if logger is None:
        # Создаём стандартный логгер, если он не передан
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < retries:
                try:
                    result = func(*args, **kwargs)
                    logger.info(f"Вызов функции {func.__name__} прошел успешно.")
                    return result  # Возвращаем результат только при успешном выполнении
                except Exception as e:
                    attempts += 1
                    logger.error(f"Ошибка в функции {func.__name__}: {e}")
                    if attempts < retries:
                        logger.info(f"Попытка {attempts}/{retries} не удалась. Повтор через {delay} секунд...")
                        time.sleep(delay)
                    else:
                        logger.error(f"Превышено максимальное количество попыток для функции {func.__name__}.")
                        raise  # Пробрасываем исключение после исчерпания всех попыток
        return wrapper
    return decorator
