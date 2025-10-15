class ClickHouseClientError(Exception):
    """Базовое исключение для клиента ClickHouse"""
    pass

class ClickHouseConnectionError(ClickHouseClientError):
    """Ошибка подключения к ClickHouse"""
    pass

class ClickHouseQueryError(ClickHouseClientError):
    """Ошибка выполнения запроса"""
    pass

class ClickHouseConfigError(ClickHouseClientError):
    """Ошибка конфигурации"""
    pass

class ClickHouseTimeoutError(ClickHouseClientError):
    """Таймаут операции"""
    pass