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
        print('Создаём базу данных...')
        conn = psycopg2.connect(dbname='postgres', **self.params)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(f'DROP DATABASE IF EXISTS {self.database_name}')
        cur.execute(f'CREATE DATABASE {self.database_name}')

        conn.close()

    def create_tables(self):
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
                )
            """)

        conn.commit()
        conn.close()

    def fill_in_tables(self, emp_data, vac_data):
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
        self.cur.execute("""
        SELECT employers.employer_name, COUNT(vacancies.employer_id)
        FROM employers
        JOIN vacancies USING (employer_id)
        GROUP BY employers.employer_name
        ORDER BY COUNT DESC
        """)

        return self.cur.fetchall()

    def get_all_vacancies(self):
        self.cur.execute("""
        SELECT employers.employer_name, vacancy_name, salary, vacancy_url
        FROM vacancies
        JOIN employers USING (employer_id)
        ORDER BY salary desc
        """)
        return self.cur.fetchall()

    def get_avg_salary(self):
        self.cur.execute("""
        SELECT avg(salary) from vacancies
        """)
        return self.cur.fetchall()

    def get_vacancies_with_higher_salary(self):
        self.cur.execute("""
        SELECT vacancy_name, salary
        FROM vacancies
        GROUP BY vacancy_name, salary
        having salary > (SELECT AVG(salary) FROM vacancies)
        ORDER BY salary DESC
        """)
        return self.cur.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        request_sql = """
        SELECT * FROM vacancies
        WHERE LOWER (vacancy_name) LIKE %s
        """
        self.cur.execute(request_sql, ('%' + keyword.lower() + '%',))
        return self.cur.fetchall()

    def close_connection(self):
        print('Спасибо за использование данной программы!')
        self.conn.close()
