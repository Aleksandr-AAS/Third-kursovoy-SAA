import os
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()


class DatabaseManager:
    """Класс для управления подключением и созданием базы данных PostgreSQL"""

    def __init__(self):
        # Параметры подключения (основная БД)
        self.db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'hh_vacancies')
        }
        # Для подключения к стандартной БД (postgres) без указания целевой БД
        self.admin_params = self.db_params.copy()
        self.admin_params.pop('database')
        self.admin_params['database'] = 'postgres'

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
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.db_params['database'],))
            exists = cur.fetchone()

            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(self.db_params['database'])
                ))
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
        """Возвращает соединение к целевой базе данных для дальнейшей работы."""
        return psycopg2.connect(**self.db_params)