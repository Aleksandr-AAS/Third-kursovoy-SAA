import requests
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import re
import time
from src.hh_abc import JobAPI


class HeadHunterAPI(JobAPI):
    """Класс для работы с API HeadHunter"""

    def __init__(self):
        self.base_url = "https://api.hh.ru"
        self.session = requests.Session()
        self.connected = False

        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Referer": "https://hh.ru/",
                "Origin": "https://hh.ru/",
            }
        )

    def connect(self) -> bool:
        """
        Подключение к API hh.ru с проверкой статус-кода
        Returns:
            bool: True если подключение успешно (статус-код 200), False в противном случае
        """
        try:
            response = self.session.get(f"{self.base_url}/", timeout=10)

            # Явная проверка статус-кода
            if response.status_code == 200:
                self.connected = True
                return True
            else:
                print(
                    f"Ошибка подключения к API hh.ru. Статус-код: {response.status_code}"
                )
                print(f"Ответ сервера: {response.text[:500]}")
                self.connected = False
                return False

        except requests.RequestException as e:
            print(f"Ошибка подключения к API hh.ru: {e}")
            self.connected = False
            return False

    def get_vacancies(self, search_query: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Получение вакансий с hh.ru по поисковому запросу

        Args:
            search_query: Поисковый запрос (вводится пользователем)
            **kwargs: Дополнительные параметры:
                - area: Регион (по умолчанию 113 - Россия)
                - per_page: Количество вакансий на странице (по умолчанию 100)
                - page: Номер страницы (по умолчанию 0)
                - only_with_salary: Только с указанием зарплаты (по умолчанию False)
                - salary: Минимальная зарплата

        Returns:
            List[Dict[str, Any]]: Список вакансий
        """
        # Вызываем метод подключения перед отправкой запроса
        if not self.connect():
            print("Не удалось подключиться к API hh.ru")
            return []

        # Параметры запроса
        params = {
            "text": search_query,
            "area": kwargs.get("area", 113),  # 113 - Россия
            "per_page": min(
                kwargs.get("per_page", 100), 100
            ),  # API ограничивает 100 на страницу
            "page": kwargs.get("page", 0),
            "locale": "RU",
            "search_field": "name",  # Искать по названию вакансии
        }

        # Обработка параметра only_with_salary
        if kwargs.get("only_with_salary"):
            params["only_with_salary"] = True

        # Обработка параметра salary
        if kwargs.get("salary"):
            params["salary"] = kwargs.get("salary")
            # Если указана зарплата, включаем фильтр only_with_salary
            params["only_with_salary"] = True

        try:
            print("Отправляю запрос к API hh.ru...")
            print(f"Параметры: {params}")

            response = self.session.get(
                f"{self.base_url}/vacancies",
                params=params,
                timeout=15,
            )

            # Проверяем статус-код ответа
            print(f"Статус ответа: {response.status_code}")

            if response.status_code != 200:
                print(
                    f"Ошибка при получении вакансий. Статус-код: {response.status_code}"
                )
                print(f"Ответ сервера: {response.text[:500]}")
                return []

            data = response.json()

            # Преобразуем вакансии в удобный формат
            vacancies = self._parse_vacancies(data.get("items", []))

            print(f"Получено {len(vacancies)} вакансий по запросу '{search_query}'")
            print(f"Всего найдено: {data.get('found', 0)} вакансий")

            # Добавляем задержку для соблюдения rate limit
            time.sleep(0.5)

            return vacancies

        except requests.RequestException as e:
            print(f"Ошибка при получении вакансий: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"Ошибка при обработке JSON: {e}")
            return []

    def _parse_vacancies(self, raw_vacancies: List[Dict]) -> List[Dict[str, Any]]:
        parsed_vacancies = []
        for vacancy in raw_vacancies:
            salary = self._parse_salary(vacancy.get("salary"))
            published_at = vacancy.get("published_at", "")
            if published_at:
                try:
                    published_at = datetime.fromisoformat(
                        published_at.replace("Z", "+00:00")
                    )
                    published_at = published_at.strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, AttributeError):
                    published_at = ""

            parsed_vacancy = {
                "id": vacancy.get("id"),
                "name": vacancy.get("name", ""),
                "url": vacancy.get("alternate_url", ""),
                "company_id": vacancy.get("employer", {}).get("id"),  # добавляем
                "company": vacancy.get("employer", {}).get("name", ""),
                "company_url": vacancy.get("employer", {}).get("alternate_url", ""),
                "salary_from": salary.get("from"),
                "salary_to": salary.get("to"),
                "salary_currency": salary.get("currency"),
                "salary_gross": salary.get("gross"),
                "experience": vacancy.get("experience", {}).get("name", ""),
                "employment": vacancy.get("employment", {}).get("name", ""),
                "schedule": vacancy.get("schedule", {}).get("name", ""),
                "description": self._clean_html(vacancy.get("description", "")),
                "snippet": vacancy.get("snippet", {}),
                "area": vacancy.get("area", {}).get("name", ""),
                "published_at": published_at,
                "query": "",
            }
            parsed_vacancies.append(parsed_vacancy)
        return parsed_vacancies

    def _parse_salary(self, salary_data: Optional[Dict]) -> Dict[str, Any]:
        """Парсинг данных о зарплате"""
        if not salary_data:
            return {}

        return {
            "from": salary_data.get("from"),
            "to": salary_data.get("to"),
            "currency": salary_data.get("currency"),
            "gross": salary_data.get("gross"),
        }

    def _clean_html(self, text: str) -> str:
        """Очистка HTML тегов из текста"""
        if not text:
            return ""

        # Удаляем HTML теги
        clean = re.sub("<[^<]+?>", "", text)
        # Заменяем HTML сущности
        clean = (
            clean.replace("&nbsp;", " ")
            .replace("&quot;", '"')
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
        )
        # Удаляем лишние пробелы
        clean = " ".join(clean.split())
        return clean

    def get_all_vacancies(
        self, search_query: str, per_page: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Получение всех вакансий (с пагинацией)

        Args:
            search_query: Поисковый запрос
            per_page: Количество вакансий на странице

        Returns:
            List[Dict[str, Any]]: Список всех вакансий
        """
        all_vacancies = []
        page = 0

        while True:
            print(f"Загружаю страницу {page + 1}...")
            vacancies = self.get_vacancies(
                search_query=search_query, per_page=per_page, page=page, area=113
            )

            if not vacancies:
                break

            all_vacancies.extend(vacancies)

            # Проверяем, есть ли еще страницы
            if len(vacancies) < per_page:
                break

            page += 1
            # Небольшая задержка между страницами
            time.sleep(1)

        return all_vacancies
