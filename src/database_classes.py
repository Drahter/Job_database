import psycopg2
from abc import ABC


class AbstractDB(ABC):
    pass


class DBCreator(AbstractDB):
    """Класс для создания датабаз, таблиц, и их заполнения"""
    def __init__(self, database_name: str, params: dict):
        self.database_name = database_name
        self.params = params

    def create_database(self):
        """Метод удаляет старые данные, если они имеются, и создает новую базу данных с указанным в классе названием"""
        print('Создаём базу данных...')
        conn = psycopg2.connect(dbname='postgres', **self.params)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(f'DROP DATABASE IF EXISTS {self.database_name}')
        cur.execute(f'CREATE DATABASE {self.database_name}')

        conn.close()

    def create_tables(self):
        """Метод создает две таблицы для их дальнейшего заполнения"""
        print('Создаём таблицы...')
        conn = psycopg2.connect(dbname=self.database_name, **self.params)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE employers (
                    employer_id INTEGER PRIMARY KEY,
                    employer_name TEXT,
                    url TEXT,
                    open_vacancies INTEGER
                )
            """)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE vacancies (
                    vacancy_id INTEGER PRIMARY KEY,
                    vacancy_name VARCHAR(255),
                    vacancy_area VARCHAR(255),
                    salary INTEGER,
                    employer_id INTEGER,
                    vacancy_url VARCHAR(255)
                );
                ALTER TABLE vacancies 
                ADD FOREIGN KEY (employer_id) REFERENCES employers(employer_id)
            """)

        conn.commit()
        conn.close()

    def fill_in_tables(self, emp_data, vac_data):
        """Метод заполняет таблицы БД данными, которые в него передаются в виде списка словарей"""
        print('Вносим данные в таблицы...')
        conn = psycopg2.connect(dbname=self.database_name, **self.params)

        with conn.cursor() as cur:
            for emp in emp_data:
                cur.execute("""
                    INSERT INTO employers ( employer_id, employer_name, url, open_vacancies)
                    VALUES (%s, %s, %s, %s)
                    """, (emp['id'], emp['name'], emp['alternate_url'], emp['open_vacancies'])
                            )

        with conn.cursor() as cur:
            for vac in vac_data:
                if vac['salary'] is None or vac['salary']['from'] is None:
                    cur.execute("""
                       INSERT INTO vacancies (vacancy_id, vacancy_name, vacancy_area, salary, employer_id, vacancy_url)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       """,
                                (vac['id'], vac['name'], vac['area']['name'], 0, vac['employer']['id'],
                                 vac['alternate_url']))
                else:
                    cur.execute("""
                        INSERT INTO vacancies (vacancy_id, vacancy_name, vacancy_area, salary, employer_id, vacancy_url)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                                (vac['id'], vac['name'], vac['area']['name'], vac['salary']['from'],
                                 vac['employer']['id'], vac['alternate_url']))

        conn.commit()
        conn.close()


class DBManager:
    """Класс для запросов к базе данных"""
    def __init__(self, params):
        self.conn = psycopg2.connect(dbname='headhunter', **params)
        self.cur = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        """Метод выводит таблицу всех работодателей и количество их вакансий"""
        self.cur.execute("""
        SELECT employers.employer_name, COUNT(vacancies.employer_id)
        FROM employers
        JOIN vacancies USING (employer_id)
        GROUP BY employers.employer_name
        ORDER BY COUNT DESC
        """)

        return self.cur.fetchall()

    def get_all_vacancies(self):
        """Метод выводит данные по всем вакансиям, сортируя их по работодателям"""
        self.cur.execute("""
        SELECT employers.employer_name, vacancy_name, salary, vacancy_url
        FROM vacancies
        JOIN employers USING (employer_id)
        ORDER BY salary desc
        """)
        return self.cur.fetchall()

    def get_avg_salary(self):
        """Метод для получения средней зарплаты среди всех вакансий в базе"""
        self.cur.execute("""
        SELECT avg(salary) from vacancies
        """)
        return self.cur.fetchall()

    def get_vacancies_with_higher_salary(self):
        """Метод возвращает все вакансии, зарплата которых превышает среднюю"""
        self.cur.execute("""
        SELECT vacancy_name, salary
        FROM vacancies
        WHERE salary > (SELECT AVG(salary) from vacancies)
        ORDER BY salary DESC
        """)
        return self.cur.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """Метод возвращает вакансии по ключевому слову, которое указывает пользователь"""
        request_sql = """
        SELECT * FROM vacancies
        WHERE LOWER (vacancy_name) LIKE %s
        """
        self.cur.execute(request_sql, ('%' + keyword.lower() + '%',))
        return self.cur.fetchall()

    def close_connection(self):
        """Метод для закрытия соединения с БД при завершении работы программы"""
        print('Спасибо за использование данной программы!')
        self.conn.close()
