import os
import psycopg2
from psycopg2 import sql, extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

load_dotenv()


class DatabaseManager:
    def __init__(self):
        self.db_params = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_NAME", "hh_vacancies"),
        }
        self.admin_params = self.db_params.copy()
        self.admin_params.pop("database")
        self.admin_params["database"] = "postgres"

    def create_database(self):
        """
        Создаёт базу данных, если она ещё не существует.
        Подключается к стандартной БД 'postgres' и выполняет CREATE DATABASE.
        """
        try:
            conn = psycopg2.connect(**self.admin_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()

            # Проверяем существование базы данных
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (self.db_params["database"],),
            )
            exists = cur.fetchone()

            if not exists:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(self.db_params["database"])
                    )
                )
                print(f"База данных '{self.db_params['database']}' успешно создана.")
            else:
                print(f"База данных '{self.db_params['database']}' уже существует.")

            cur.close()
            conn.close()
        except Exception as e:
            print(f"Ошибка при создании базы данных: {e}")
            raise

    def create_tables(self):
        """
        Создаёт таблицы companies и vacancies, если они ещё не существуют.
        Устанавливает внешний ключ.
        """
        create_companies_table = """
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            url VARCHAR(255)
        );
        """

        create_vacancies_table = """
        CREATE TABLE IF NOT EXISTS vacancies (
            id INTEGER PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            name VARCHAR(255) NOT NULL,
            url VARCHAR(255),
            salary_from INTEGER,
            salary_to INTEGER,
            salary_currency VARCHAR(10),
            salary_gross BOOLEAN,
            experience VARCHAR(50),
            employment VARCHAR(50),
            schedule VARCHAR(50),
            description TEXT,
            area VARCHAR(100),
            published_at TIMESTAMP,
            query VARCHAR(255)
        );
        """

        # Индексы для ускорения поиска
        create_indexes = """
        CREATE INDEX IF NOT EXISTS idx_vacancies_company_id ON vacancies(company_id);
        CREATE INDEX IF NOT EXISTS idx_vacancies_published_at ON vacancies(published_at);
        """

        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()

            cur.execute(create_companies_table)
            cur.execute(create_vacancies_table)
            cur.execute(create_indexes)

            conn.commit()
            print("Таблицы успешно созданы или уже существуют.")
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Ошибка при создании таблиц: {e}")
            raise

    def get_connection(self):
        return psycopg2.connect(**self.db_params)

    def save_company(self, company_data: dict) -> int:
        """
        Сохраняет компанию в таблицу companies.
        Если компания с таким id уже существует, обновляет её название и url.
        Возвращает id компании.
        """
        query = """
        INSERT INTO companies (id, name, url)
        VALUES (%(id)s, %(name)s, %(url)s)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            url = EXCLUDED.url
        RETURNING id;
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, company_data)
                company_id = cur.fetchone()[0]
                conn.commit()
                return company_id

    def save_vacancy(self, vacancy_data: dict):
        """
        Сохраняет вакансию в таблицу vacancies.
        Если вакансия с таким id уже есть – пропускает (можно заменить на UPDATE).
        """
        query = """
        INSERT INTO vacancies (
            id, company_id, name, url, salary_from, salary_to,
            salary_currency, salary_gross, experience, employment,
            schedule, description, area, published_at, query
        ) VALUES (
            %(id)s, %(company_id)s, %(name)s, %(url)s, %(salary_from)s,
            %(salary_to)s, %(salary_currency)s, %(salary_gross)s, %(experience)s,
            %(employment)s, %(schedule)s, %(description)s, %(area)s,
            %(published_at)s, %(query)s
        )
        ON CONFLICT (id) DO NOTHING;
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, vacancy_data)
                conn.commit()

    def get_top_companies(self, limit: int = 10) -> list[dict]:
        """
        Возвращает список топ‑limit компаний по количеству вакансий.
        Каждый элемент словаря: id, name, url, vacancy_count.
        """
        query = """
        SELECT
            c.id,
            c.name,
            c.url,
            COUNT(v.id) AS vacancy_count
        FROM companies c
        LEFT JOIN vacancies v ON c.id = v.company_id
        GROUP BY c.id, c.name, c.url
        ORDER BY vacancy_count DESC
        LIMIT %s;
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute(query, (limit,))
                return cur.fetchall()

    def keep_only_top_companies(self, limit: int = 10):
        """
        Удаляет из таблиц companies и vacancies все записи, кроме компаний,
        входящих в топ-limit по количеству вакансий.
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Получаем id компаний, входящих в топ-limit
                cur.execute(
                    """
                    SELECT c.id
                    FROM companies c
                    LEFT JOIN vacancies v ON c.id = v.company_id
                    GROUP BY c.id
                    ORDER BY COUNT(v.id) DESC
                    LIMIT %s
                """,
                    (limit,),
                )
                top_ids = [row[0] for row in cur.fetchall()]

                if not top_ids:
                    print("Нет компаний для сохранения.")
                    return

                # Удаляем вакансии, не относящиеся к топ-компаниям
                cur.execute(
                    "DELETE FROM vacancies WHERE company_id NOT IN %s",
                    (tuple(top_ids),),
                )
                # Удаляем компании, не вошедшие в топ
                cur.execute(
                    "DELETE FROM companies WHERE id NOT IN %s", (tuple(top_ids),)
                )
                conn.commit()
                print(f"Оставлено {len(top_ids)} компаний (топ-{limit}).")

    def get_companies_and_vacancies_count(self) -> list[dict]:
        """
        Возвращает список всех компаний и количество вакансий у каждой.
        """
        query = """
            SELECT c.name, COUNT(v.id) as vacancies_count
            FROM companies c
            LEFT JOIN vacancies v ON c.id = v.company_id
            GROUP BY c.name
            ORDER BY vacancies_count DESC;
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute(query)
                return cur.fetchall()

    def get_all_vacancies(self) -> list[dict]:
        """
        Возвращает список всех вакансий с указанием названия компании,
        названия вакансии, зарплаты и ссылки на вакансию.
        Зарплата форматируется в удобочитаемый вид.
        """
        query = """
            SELECT
                c.name AS company_name,
                v.name AS vacancy_name,
                v.url,
                v.salary_from,
                v.salary_to,
                v.salary_currency
            FROM vacancies v
            JOIN companies c ON v.company_id = c.id
            ORDER BY c.name;
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute(query)
                rows = cur.fetchall()
                for row in rows:
                    salary = self._format_salary(
                        row["salary_from"], row["salary_to"], row["salary_currency"]
                    )
                    row["salary"] = salary
                    # Удаляем исходные поля, чтобы не загромождать вывод
                    del row["salary_from"]
                    del row["salary_to"]
                    del row["salary_currency"]
                return rows

    def get_avg_salary(self) -> float:
        """
        Возвращает среднюю зарплату по вакансиям (учитываются только вакансии с указанной зарплатой).
        Для каждой вакансии берётся среднее арифметическое от from и to,
        если указано только одно поле — используется оно.
        """
        query = """
            SELECT AVG(
                CASE
                    WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL
                        THEN (salary_from + salary_to) / 2.0
                    WHEN salary_from IS NOT NULL THEN salary_from
                    WHEN salary_to IS NOT NULL THEN salary_to
                    ELSE NULL
                END
            ) as avg_salary
            FROM vacancies
            WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL;
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()[0]
                return float(result) if result is not None else 0.0

    def get_vacancies_with_higher_salary(self) -> list[dict]:
        """
        Возвращает список вакансий, у которых зарплата выше средней.
        """
        avg_salary = self.get_avg_salary()
        if avg_salary == 0:
            return []

        query = """
            SELECT
                c.name AS company_name,
                v.name AS vacancy_name,
                v.url,
                v.salary_from,
                v.salary_to,
                v.salary_currency,
                CASE
                    WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL
                        THEN (salary_from + salary_to) / 2.0
                    WHEN salary_from IS NOT NULL THEN salary_from
                    WHEN salary_to IS NOT NULL THEN salary_to
                    ELSE NULL
                END AS computed_salary
            FROM vacancies v
            JOIN companies c ON v.company_id = c.id
            WHERE
                (salary_from IS NOT NULL OR salary_to IS NOT NULL)
                AND
                CASE
                    WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL
                        THEN (salary_from + salary_to) / 2.0
                    WHEN salary_from IS NOT NULL THEN salary_from
                    WHEN salary_to IS NOT NULL THEN salary_to
                    ELSE NULL
                END > %s
            ORDER BY computed_salary DESC;
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute(query, (avg_salary,))
                rows = cur.fetchall()
                for row in rows:
                    salary = self._format_salary(
                        row["salary_from"], row["salary_to"], row["salary_currency"]
                    )
                    row["salary"] = salary
                    del row["salary_from"]
                    del row["salary_to"]
                    del row["salary_currency"]
                    del row["computed_salary"]
                return rows

    def get_vacancies_with_keyword(self, keyword: str) -> list[dict]:
        """
        Возвращает список вакансий, в названии которых содержится keyword (регистронезависимо).
        """
        query = """
            SELECT
                c.name AS company_name,
                v.name AS vacancy_name,
                v.url,
                v.salary_from,
                v.salary_to,
                v.salary_currency
            FROM vacancies v
            JOIN companies c ON v.company_id = c.id
            WHERE LOWER(v.name) LIKE LOWER(%s)
            ORDER BY v.name;
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                cur.execute(query, (f"%{keyword}%",))
                rows = cur.fetchall()
                for row in rows:
                    salary = self._format_salary(
                        row["salary_from"], row["salary_to"], row["salary_currency"]
                    )
                    row["salary"] = salary
                    del row["salary_from"]
                    del row["salary_to"]
                    del row["salary_currency"]
                return rows

    def _format_salary(self, salary_from, salary_to, currency) -> str:
        """Вспомогательный метод для форматирования зарплаты."""
        if salary_from is not None and salary_to is not None:
            return f"от {salary_from} до {salary_to} {currency or ''}".strip()
        elif salary_from is not None:
            return f"от {salary_from} {currency or ''}".strip()
        elif salary_to is not None:
            return f"до {salary_to} {currency or ''}".strip()
        else:
            return "не указана"
