import abc
from typing import List, Dict, Any


class JobAPI(abc.ABC):
    """Абстрактный класс для работы с API сервиса вакансий"""

    @abc.abstractmethod
    def connect(self) -> bool:
        """Подключение к API сервиса"""
        pass

    @abc.abstractmethod
    def get_vacancies(self, search_query: str, **kwargs) -> List[Dict[str, Any]]:
        """Получение списка вакансий по поисковому запросу"""
        pass
