import requests
from abc import ABC, abstractmethod


class AbstractAPI(ABC):
    @abstractmethod
    def get_vacancies(self):
        pass

    @abstractmethod
    def get_employees(self):
        pass


"""id компаний на ХХ, которые будут запрошены с API"""
employer_ids = [665449, 5554827, 32501, 3776, 4233, 4181, 39305, 1329, 478, 1122462]


class HeadHunterAPI(AbstractAPI):
    def __init__(self):
        pass

    @classmethod
    def get_vacancies(cls):
        """Метод запрашивает список вакансий по айди работодателя"""
        print('Получаем данные о вакансиях... ')
        vacancies = []
        for vacancy_id in employer_ids:
            url = f'https://api.hh.ru/vacancies?employer_id={vacancy_id}'
            vacancy_data = requests.get(url, params={'page': 0, 'per_page': 100}).json()
            for each in vacancy_data['items']:
                vacancies.append(each)
        return vacancies

    @classmethod
    def get_employees(cls):
        """Метод запрашивает данные о работодателе по айди"""
        print('и работодателях... ')
        employers = []
        for emp_id in employer_ids:
            url = f'https://api.hh.ru/employers/{emp_id}'
            employer_data = requests.get(url).json()
            employers.append(employer_data)
        return employers
