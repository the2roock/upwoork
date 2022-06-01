import json
import multiprocessing

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from fake_useragent import UserAgent

from datetime import datetime
from time import sleep
from random import choice

from config import Config
from db_connection import db_connection
import check_html
import bots


geckodriver_path = '/usr/bin/geckodriver'

def get_html(url, time_out=4):
    options = Options()
    options.add_argument(f'user-agent={UserAgent().random}')
    options.add_argument('--headless')
    driver = webdriver.Firefox(executable_path=geckodriver_path, options=options)
    try:
        driver.get(url=url)
        sleep(time_out)
        result = driver.page_source
    except Exception as e:
        result = ''
        print('Cant driver.get()')
        print(e)
    finally:
        driver.close()
        driver.quit()
    with open('html.html', 'wt') as file:
        file.write(result)
    return result

def scrap_search_page():
    while True:
        url = 'https://www.upwork.com/nx/jobs/search/?sort=recency&per_page=50'
        print('Search page in progress')

        html = get_html(url=url, time_out=8)
        soup = BeautifulSoup(html, 'lxml')
        try:
            if not check_html.check_title(title=soup.find('title').text.strip()):
                write_to_logfile('Bad Search Page')
                await asyncio.sleep(90)
                continue
        except:
            await asyncio.sleep(90)
            continue

        result = []

        sections = soup.find_all('section', class_='up-card-section up-card-list-section up-card-hover')
        if sections == []:
            continue

        domain = 'https://www.upwork.com/freelance-jobs/apply/'
        for section in sections:
            data = {}

            # uid
            data['uid'] = section.find('h4', class_='my-0 p-sm-right job-tile-title').find('a')['href'].split('~')[1][:-1]

            # title
            data['title'] = section.find('h4', class_='my-0 p-sm-right job-tile-title').text.replace('\'', '').strip()
            if 'Do not apply' in data['title']:
                print('do not apply')
                continue

            # description
            data['description'] = section.find('span', attrs={'data-test': 'job-description-text'}).text.replace('\'', '').strip()

            # price
            data['price'] = {}
            if 'Fixed-price' == section.find('strong', attrs={'data-test': 'job-type'}).text:
                data['price']['isFixed'] = True
                try:
                    data['price']['value'] = float(section.find('span', attrs={'data-test': 'budget'}).text.strip()[1:].replace(',', ''))
                except:
                    data['price']['value'] = 0
            else:
                data['price']['isFixed'] = False

                budget = section.find('strong', attrs={'data-test': 'job-type'}).text.strip().split('$')
                if len(budget) == 1:
                    data['price']['isFixed'] = True
                    data['price']['value'] = 0
                elif len(budget) == 2:
                    data['price']['value'] = {'min': float(budget[1].replace('-', '')), 'max': float(budget[1])}
                else:
                    data['price']['value'] = {'min': float(budget[1].replace('-', '')), 'max': float(budget[2])}


            # experience
            try:
                data['experience'] = section.find('span', attrs={'data-test': 'contractor-tier'}).text
            except:
                data['experience'] = 'Without experience'

            # duration
            try:
                data['duration'] = section.find('span', attrs={'data-test': 'duration'}).text
            except:
                data['duration'] = 'Without duration'

            try:
                data['tags'] = [{'title': tag.text.replace('\'', ''), 'uid': tag['href'].split('=')[-1]} for tag in section.find('div', class_='up-skill-container').find('div', class_='up-skill-wrapper').find_all('a')]
            except:
                data['tags'] = 0
            data['url'] = domain + section.find('div', class_='row my-10').find('h4').find('a')['href'].split('/')[-2] + '/'
            result.append(data)

        save_project_first_data_to_db(result)
        sleep(90)


def save_project_first_data_to_db(datas):
    with db_connection() as connection:
        with connection.cursor() as cursor:
            count_getted_elements = len(datas)
            count_added_elements = 0
            for data in datas:
                time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                with open('project.json', 'w') as file:
                    json.dump(data, file, indent=2)
                # is job exists in db
                sql_query = f"SELECT EXISTS(SELECT id FROM job WHERE link=\'{data['url']}\')"
                cursor.execute(sql_query)
                if cursor.fetchone()[0] == 1:
                    continue

                # new record to table job
                sql_query = f"""INSERT INTO job(name, description, link) VALUES(\'{data['title'].replace("'", '')}\', \'{data['description'].replace("'", '')}\', \'{data['url'].replace("'", '')}\')"""
                cursor.execute(sql_query)

                # get job id
                sql_query = f"SELECT id FROM job WHERE link=\'{data['url']}\'"
                cursor.execute(sql_query)
                data['id'] = cursor.fetchone()[0]

                # append job to advanced scrapping
                bots.bot_send(result)
                count_added_elements += 1

                tags_id = []
                if data['tags'] != 0:
                    for tag in data['tags']:
                        slug = tag['title'].lower().replace(' ', '_')
                        sql_query = f"SELECT EXISTS(SELECT id FROM skill WHERE slug='{slug}')"
                        cursor.execute(sql_query)
                        if cursor.fetchone()[0] == 0:
                            sql_query = f"INSERT INTO skill(name, slug) VALUES(\'{tag['title']}\', \'{slug}\')"
                            cursor.execute(sql_query)
                        sql_query = f"SELECT id FROM skill WHERE slug=\'{slug}\'"
                        cursor.execute(sql_query)
                        tags_id.append(cursor.fetchone()[0])

                # add tags to meta_job
                sql_query = f"INSERT INTO meta_job(id_job, meta_key, meta_value) VALUES({data['id']}, 'skill', \'{str(tags_id)}\')"
                cursor.execute(sql_query)

                # add price to meta_job
                sql_query = f"""INSERT INTO meta_job(id_job, meta_key, meta_value) VALUES({data['id']}, 'price', \'{{isFixed: {data['price']['isFixed']}, cost: {str(data['price']['value']).replace("'",'')}}}\')"""
                cursor.execute(sql_query)

                # add experience to meta_job
                if data['experience'] != 'Without experience':
                    sql_query = f"INSERT INTO meta_job(id_job, meta_key, meta_value) VALUES({data['id']}, 'experience', '{data['experience'].lower()}')"
                    cursor.execute(sql_query)
                connection.commit()
        print('{} of {} was added'.format(count_added_elements, count_getted_elements))


def main():
    multiprocessing.Process(target=scrap_search_page).start()
    multiprocessing.Process(target=bots.bot_config).start()
    bots.send_messages()

if __name__ == '__main__':
    main()
