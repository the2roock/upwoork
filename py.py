import pymysql

from datetime import datetime
from selenium import webdriver
from bs4 import BeautifulSoup
from time import sleep
from os import path


def save_data_to_db(data):
    db = 'upwork_test_db'
    host = 'localhost'
    user = 'upwork_test'
    password = 'upwork_test'

    with pymysql.connect(database=db, host=host, user=user, password=password) as connection:
        with connection.cursor() as cursor:
            count_getted_elements = len(data)
            count_added_elements = 0
            time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for element in data:
                sql_query = "SELECT EXISTS(SELECT upwork_id FROM test_table WHERE upwork_id = \'{}\')".format(element['upwork_id'])
                cursor.execute(sql_query)
                if cursor.fetchone()[0] == 1:
                    print('element identified by {} is exists'.format(element['upwork_id']))
                    continue

                sql_query = f"INSERT INTO test_table(upwork_id, title, description, time) VALUES(\'{element['upwork_id']}\', \'{element['title']}\', \'{element['description']}\', \'{time}\')"
                cursor.execute(sql_query)
                count_added_elements += 1
            sql_query = f"INSERT INTO logs(time, was_added, was_getted) VALUES(\'{time}\', {count_added_elements}, {count_getted_elements})"
            cursor.execute(sql_query)

        connection.commit()

def get_html(url):
    driver = webdriver.Chrome('./chromedriver/chromedriver')

    try:
        driver.get(url=url)
        sleep(3)
        result = driver.page_source
    except Exception as e:
        print(e)
    finally:
        driver.close()
        driver.quit()
    return result

def save_page(html, file_name):
    with open(file_name, 'wt') as file:
        file.write(driver.page_source)


def get_html_from_file(file_name):
    with open(file_name, 'rt') as file:
        return file.read()


def scrap(html):
    result = []
    soup = BeautifulSoup(html, 'lxml')
    sections = soup.find_all('section', class_='up-card-section up-card-list-section up-card-hover')
    for section in sections:
        data = {}

        data['upwork_id'] = section['id']
        data['title'] = section.find('div', class_='row my-10').find('h4').text.replace('\'', '')
        data['description'] = section.find('div', class_='mb-10').find('div', class_='mt-10').text.replace('\'', '')

        print('{}: {}'.format(data['upwork_id'], data['title']))
        result.append(data)
    return result

def check_page(html):
    soup = BeautifulSoup(html, 'lxml')
    if soup.find('title').text == 'Freelance Jobs - Upwork':
        return True
    return False



def main():
    i = 1
    while True:

        directory_path = './pages'

        url = 'https://www.upwork.com/nx/jobs/search/?sort=recency&per_page=50'

        html = get_html(url=url)

        if not check_page(html=html):
            print('Bad page found')
            if not path.exists('log.txt'):
                with open('log.txt', 'w'):
                    pass
            with open('log.txt', 'at') as file:
                time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                file.write(f'{time}\tBad page')
            continue

        print('page in progress')
        # save_page(url=url, file_name=f'{directory_path}/page.html')
        # html = get_html_from_file(f'{directory_path}/page.html')
        data = scrap(html)
        save_data_to_db(data)
        i += 1
        sleep(18.5)



if __name__ == '__main__':
    main()
