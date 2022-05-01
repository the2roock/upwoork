import pymysql

from datetime import datetime
from selenium import webdriver
from bs4 import BeautifulSoup
from time import sleep


def save_data_to_db(data):
    db = 'upwork_test_db'
    host = 'localhost'
    user = 'upwork_test'
    password = 'upwork_test'

    with pymysql.connect(database=db, host=host, user=user, password=password) as connection:
        with connection.cursor() as cursor:
            for element in data:
                sql_query = "SELECT EXISTS(SELECT upwork_id FROM test_table WHERE upwork_id = \'{}\')".format(element['upwork_id'])
                cursor.execute(sql_query)
                if cursor.fetchone()[0] == 1:
                    print('element identified by {} is exists'.format(element['upwork_id']))
                    continue

                time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sql_query = f"INSERT INTO test_table(upwork_id, title, description, time) VALUES(\'{element['upwork_id']}\', \'{element['title']}\', \'{element['description']}\', \'{time}\')"
                cursor.execute(sql_query)


        connection.commit()


def save_page(url, file_name):
    driver = webdriver.Chrome('../chromedriver/chromedriver')

    try:
        driver.get(url=url)
        sleep(2)
        with open(file_name, 'wt') as file:
            file.write(driver.page_source)
    except Exception as e:
        print(e)
    finally:
        driver.close()
        driver.quit()


def get_html_from_file(file_name):
    with open(file_name, 'rt') as file:
        return file.read()


def scrap(html):
    result = []
    soup = BeautifulSoup(html, 'lxml')
    sections = soup.find_all('section', class_='up-card-section up-card-list-section up-card-hover')
    for section in sections:
        data = {}

        data['upwork_id'] = section.['id']
        data['title'] = section.find('div', class_='row my-10').find('h4').text.replace('\'', '')
        data['description'] = section.find('div', class_='mb-10').find('div', class_='mt-10').text.replace('\'', '')

        result.append(data)
    return result


def main():
    url = 'https://www.upwork.com/nx/jobs/search/?sort=recency&per_page=50&page={}'

    directory_path = './pages'
    for page in range(1, 501):
        save_page(url=url.format(page), file_name=f'{directory_path}/page_{page}.html')
        print(f'Page [{page}] --')
        sleep(4)
        html = get_html_from_file(f'{directory_path}/page_{page}.html')
        data = scrap(html)
        with open('shos.txt', 'wt') as file:
            file.write(str(data))
        save_data_to_db(data)
    sleep(3000)



if __name__ == '__main__':
    main()
