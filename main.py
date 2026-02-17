from db_manager import DatabaseManager
from src.hh_api import HeadHunterAPI


def extract_company_from_vacancy(vacancy: dict) -> dict:
    return {
        "id": vacancy.get("company_id"),
        "name": vacancy.get("company"),
        "url": vacancy.get("company_url"),
    }


def extract_vacancy_data(vacancy: dict, search_query: str) -> dict:
    return {
        "id": vacancy.get("id"),
        "company_id": vacancy.get("company_id"),
        "name": vacancy.get("name"),
        "url": vacancy.get("url"),
        "salary_from": vacancy.get("salary_from"),
        "salary_to": vacancy.get("salary_to"),
        "salary_currency": vacancy.get("salary_currency"),
        "salary_gross": vacancy.get("salary_gross"),
        "experience": vacancy.get("experience"),
        "employment": vacancy.get("employment"),
        "schedule": vacancy.get("schedule"),
        "description": vacancy.get("description"),
        "area": vacancy.get("area"),
        "published_at": vacancy.get("published_at"),
        "query": search_query,
    }


def run_interface(db_manager):
    """Интерактивный интерфейс для работы с данными о вакансиях."""
    while True:
        print("\n" + "=" * 50)
        print("МЕНЮ:")
        print("1. Показать компании и количество вакансий")
        print("2. Показать все вакансии")
        print("3. Показать среднюю зарплату")
        print("4. Показать вакансии с зарплатой выше средней")
        print("5. Поиск вакансий по ключевому слову")
        print("0. Выход")
        choice = input("Выберите действие: ").strip()

        if choice == "1":
            companies = db_manager.get_companies_and_vacancies_count()
            if not companies:
                print("Нет данных о компаниях.")
            else:
                print("\n--- Компании и количество вакансий ---")
                for idx, comp in enumerate(companies, 1):
                    print(f"{idx}. {comp['name']} — {comp['vacancies_count']} вакансий")
        elif choice == "2":
            vacancies = db_manager.get_all_vacancies()
            if not vacancies:
                print("Нет вакансий.")
            else:
                print("\n--- Все вакансии ---")
                for idx, vac in enumerate(vacancies, 1):
                    print(
                        f"{idx}. {vac['company_name']} | {vac['vacancy_name']} | {vac['salary']} | {vac['url']}"
                    )
        elif choice == "3":
            avg_salary = db_manager.get_avg_salary()
            print(
                f"\nСредняя зарплата по всем вакансиям: {avg_salary:.2f} (в условных единицах)"
            )
        elif choice == "4":
            high_salary_vacancies = db_manager.get_vacancies_with_higher_salary()
            if not high_salary_vacancies:
                print("Нет вакансий с зарплатой выше средней.")
            else:
                print("\n--- Вакансии с зарплатой выше средней ---")
                for idx, vac in enumerate(high_salary_vacancies, 1):
                    print(
                        f"{idx}. {vac['company_name']} | {vac['vacancy_name']} | {vac['salary']}"
                    )
        elif choice == "5":
            keyword = input("Введите ключевое слово для поиска: ").strip()
            if keyword:
                vacancies = db_manager.get_vacancies_with_keyword(keyword)
                if not vacancies:
                    print(f"Вакансии с ключевым словом '{keyword}' не найдены.")
                else:
                    print(f"\n--- Вакансии с ключевым словом '{keyword}' ---")
                    for idx, vac in enumerate(vacancies, 1):
                        print(
                            f"{idx}. {vac['company_name']} | {vac['vacancy_name']} | {vac['salary']} | {vac['url']}"
                        )
            else:
                print("Ключевое слово не может быть пустым.")
        elif choice == "0":
            print("Выход из программы.")
            break
        else:
            print("Неверный ввод. Попробуйте снова.")


def main():

    db_manager = DatabaseManager()
    db_manager.create_database()
    db_manager.create_tables()

    hh_api = HeadHunterAPI()
    search_query = "Python разработчик"
    vacancies = hh_api.get_vacancies(search_query)

    if not vacancies:
        print("Не удалось получить вакансии.")
        return

    print(f"Получено {len(vacancies)} вакансий. Сохраняем в БД...")

    for vac in vacancies:
        company = extract_company_from_vacancy(vac)
        if company["id"]:
            db_manager.save_company(company)
            vacancy_record = extract_vacancy_data(vac, search_query)
            db_manager.save_vacancy(vacancy_record)
        else:
            print(f"Вакансия {vac.get('id')} пропущена: отсутствует company_id")

    print("Данные успешно сохранены.")

    # --- НОВЫЙ ШАГ: оставляем только топ-10 компаний ---
    db_manager.keep_only_top_companies(10)
    run_interface(db_manager)

    # Проверяем результат
    # top_companies = db_manager.get_top_companies(10)
    # print("\nТоп-10 компаний по количеству вакансий:")
    # for company in top_companies:
    #     print(f"{company['id']}\t{company['name'][:20]}\t\t{company['vacancy_count']}")


############################################
#
# # Примеры использования новых методов
# print("\n--- Компании и количество вакансий ---")
# companies_count = db_manager.get_companies_and_vacancies_count()
# for idx, item in enumerate(companies_count, start=1):
#     print(f"{idx}. {item['name']}: {item['vacancies_count']} вакансий")
# print(f"Всего компаний: {len(companies_count)}")
# # for item in companies_count:
# #     print(f"{item['name']}: {item['vacancies_count']} вакансий")
#
# print("\n--- Все вакансии (первые 5) ---")
# all_vacancies = db_manager.get_all_vacancies()
# for vac in all_vacancies[:5]:
#     print(f"{vac['company_name']} | {vac['vacancy_name']} | {vac['salary']} | {vac['url']}")
#
# print("\n--- Средняя зарплата ---")
# avg_salary = db_manager.get_avg_salary()
# print(f"Средняя зарплата: {avg_salary:.2f} руб.")
#
# print("\n--- Вакансии с зарплатой выше средней (первые 5) ---")
# higher_salary = db_manager.get_vacancies_with_higher_salary()
# for vac in higher_salary[:5]:
#     print(f"{vac['company_name']} | {vac['vacancy_name']} | {vac['salary']}")
#
# keyword = "python"
# print(f"\n--- Вакансии с ключевым словом '{keyword}' ---")
# keyword_vacancies = db_manager.get_vacancies_with_keyword(keyword)
# for vac in keyword_vacancies[:5]:
#     print(f"{vac['company_name']} | {vac['vacancy_name']} | {vac['salary']}")

if __name__ == "__main__":
    main()
