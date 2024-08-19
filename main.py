from src.hh_api import HeadHunterAPI
from src.json_classes import JSONSaver
from src.database_classes import DBCreator, DBManager
from config import config

import os


def main():
    """
    Функция для взаимодействия с пользователем, использующая остальные части программы для заполнения базы данных
    и получения информации из них
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # С помощью кода из config получает параметры для доступа к базе данных
    params = config(filename=base_dir + '/Job_database_course5/database.ini', section='postgresql')

    db = DBCreator('headhunter', params)
    # Создает новую базу данных с указанным именем
    db.create_database()
    # Создает таблицы
    db.create_tables()
    # Подключается к API и получает данные по вакансиям и работодателям
    hh = HeadHunterAPI
    hh_vac = hh.get_vacancies()
    hh_emp = hh.get_employees()

    # Сохраняет данные в соответствующих файлах
    vacancy_saver = JSONSaver(file_name="vacancies.json")
    employers_saver = JSONSaver(file_name="employers.json")

    vacancy_saver.save_data(hh_vac)
    employers_saver.save_data(hh_emp)

    # Заполняет ранее созданные таблицы данными из API
    json_vac = vacancy_saver.get_data()
    json_emp = employers_saver.get_data()
    db.fill_in_tables(json_emp, json_vac)
    # Создает класс для запросов информации у БД
    dbmanager = DBManager(params)

    # Часть для взаимодействия с пользователем
    print('Данные успешно загружены!\nЧтобы продолжить работу, пожалуйста, выберите одно из действий ниже:')
    print('1 - список всех компаний и количество вакансий у каждой компании\n'
          '2 - список вакансий с указанием названия компании, названия вакансии, зарплаты и ссылки на вакансию\n'
          '3 - получить среднюю зарплату по вакансиям\n'
          '4 - список всех вакансий, у которых зарплата выше средней по всем вакансиям\n'
          '5 - список всех вакансий, в названии которых содержатся ключевое слово\n'
          '0 - закончить работу программы\n')

    while True:
        query = input()
        if query == '1':
            print(dbmanager.get_companies_and_vacancies_count())
        elif query == '2':
            print(dbmanager.get_all_vacancies())
        elif query == '3':
            print(dbmanager.get_avg_salary())
        elif query == '4':
            print(dbmanager.get_vacancies_with_higher_salary())
        elif query == '5':
            keyword = input('Введите ключевое слово:')
            print(dbmanager.get_vacancies_with_keyword(keyword))
        elif query == '0':
            dbmanager.close_connection()
            break
        else:
            print('Такой опции нет!')


if __name__ == '__main__':
    main()
