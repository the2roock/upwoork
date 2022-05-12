import asyncio
import pymysql
import json
import multiprocessing
import requests

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from fake_useragent import UserAgent

from datetime import datetime
from os import path, popen
from time import sleep

from config import Config

def db_connection():
    return pymysql.connect(host=Config.db_host, user=Config.db_user, database=Config.db_name, password=Config.db_password)

def scrap():
    print('scrap started')
    task_urls = []
    URL_sender = 'https://api.telegram.org/bot{}/'.format(Config.bot_sender_token)

    tor_binary_path_driver = './tor/tor-browser_en-US/Browser/firefox'

    geckodriver_path = 'usr/bin/geckodriver'

    popen(tor_binary_path_driver)
    options = Options()
    options.add_argument(f'user-agent={UserAgent().random}')
    options.add_argument('--headless')

    firefox_capabilities = webdriver.DesiredCapabilities.FIREFOX
    firefox_capabilities['marionette'] = True
    firefox_capabilities['proxy'] = {
      "proxyType": "MANUAL",
      'socksProxy': 'localhost:9050',
      "socksVersion": 5
    }



    def get_html(url):
        print(len(task_urls))
        try:
            driver = webdriver.Firefox(capabilities=firefox_capabilities, options=options, executable_path=geckodriver_path)
            driver.get(url=url)
            sleep(2.5)
            result = driver.page_source
            print('OK')
        except Exception as e:
            print(e)
        finally:
            driver.close()
            driver.quit()

        with open('html.html', 'wt') as file:
            file.write(result)
        return result


    def check_project_page(h1):
        if not('This job is a private listing' in h1) and not('Do not apply' in h1) and not('This job is no longer available' in h1):
            return (True, )
        return (False, h1)


    def check_search_page(title):
        if 'Freelance Jobs - Upwork' in title:
            return True
        return False


    def is_interrupted(h1):
        if 'Your connection was interrupted' in h1:
            return True
        return False


    def write_to_logfile(data=''):
        if not path.exists('log.txt'):
            with open('log.txt', 'w'):
                pass
        with open('log.txt', 'at') as file:
            time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            file.write(f'{time}\t{data}\n')


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
                    sql_query = f"""INSERT INTO job(name, description, link) VALUES(\'{data['title'].replace("'", '')}\', \'{data['description'].replace("'", '')}\', \'{data['url']}\')"""
                    cursor.execute(sql_query)

                    # get job id
                    sql_query = f"SELECT id FROM job WHERE link=\'{data['url']}\'"
                    cursor.execute(sql_query)
                    data['id'] = cursor.fetchone()[0]

                    # append job to advanced scrapping
                    task_urls.append({'title': data['title'], 'url': data['url']})
                    count_added_elements += 1

                    tags_id = []
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
                    sql_query = f"""INSERT INTO meta_job(id_job, meta_key, meta_value) VALUES({data['id']}, 'price', \'{{isFixed: {data['price']['is_fixed']}, cost: {str(data['price']['value']).replace("'",'')}}}\')"""
                    cursor.execute(sql_query)

                    # add experience to meta_job
                    if data['experience'] != None:
                        sql_query = f"INSERT INTO meta_job(id_job, meta_key, meta_value) VALUES({data['id']}, 'experience', '{data['experience'].lower()}')"
                        cursor.execute(sql_query)
            connection.commit()
            print('{} of {} was added'.format(count_added_elements, count_getted_elements))

    def save_advanced_project_data_to_db(data):
        with db_connection() as connection:
            with connection.cursor() as cursor:
                if data['client']['location'] != None:
                    sql_query = f"SELECT EXISTS(SELECT id FROM country WHERE slug=\'{data['client']['location'].lower().replace(' ', '_')}\')"
                    cursor.execute(sql_query)
                    if cursor.fetchone()[0] == 0:
                        sql_query = f"INSERT INTO country(name, slug) VALUES(\'{data['client']['location']}\', \'{data['client']['location'].lower().replace(' ', '_')}\')"
                        cursor.execute(sql_query)
                    sql_query = f"SELECT id FROM country WHERE slug=\'{data['client']['location'].lower().replace(' ', '_')}\'"
                    cursor.execute(sql_query)
                    sql_query = f"INSERT INTO meta_job(id_job, meta_key, meta_value) VALUES({data['id']}, 'country', \'{cursor.fetchone()[0]}\')"
                    cursor.execute(sql_query)
            connection.commit()


    def bot_send(data):
        with open('json.json', 'w') as file:
            json.dump(data, file, indent=2)
        with db_connection().cursor() as cursor:
            sql_query = "SELECT id, id_user, name FROM filters"
            cursor.execute(sql_query)
            filters = [{'id': filter[0], 'id_user': filter[1], 'name': filter[2]} for filter in cursor.fetchall()]

            for filter in filters:
                sql_query = f"SELECT id_option, option_value FROM filter_elements WHERE id_filter={filter['id']}"
                cursor.execute(sql_query)
                filter_elements = [{'id_option': element[0], 'option_value': element[1]} for element in cursor.fetchall()]
                filter_percent_skill = 0
                job_weight = 0
                work_time_flag = True

                for filter_element in filter_elements:
                    sql_query = f"SELECT func FROM option_for_filter WHERE id={filter_element['id_option']}"
                    cursor.execute(sql_query)
                    func = cursor.fetchone()[0]

                    if func == 'work_time':
                        value =  [element.split(':') for element in filter_element['option_value'].split('-')]
                        time = datetime.now()
                        minutes = []
                        minutes.append(int(value[0][0])*24 + int(value[0][1]))
                        if value[1] == ['00', '00']:
                            minutes.append(1440)
                        else:
                            minutes.append(int(value[1][0])*24 + int(value[1][1]))

                        if not (minutes[0] <= (time.hour*24 + time.minute) <= minutes[1]):
                            work_time_flag = False



                    elif func == 'fixed_price':
                        if data['price']['isFixed']:
                            if check_filter_fixed_price(data['price']['cost'], filter_element['option_value']):
                                job_weight += 1

                    elif func == 'price':
                        if check_filter_price(data['price'], filter_element['option_value']):
                            job_weight += 1

                    elif func == 'country':
                        sql_query = f"SELECT id FROM country WHERE slug={data['location'].lower().replace(' ', '_')}"
                        try:
                            cursor.execute(sql_query)
                            country_id = cursor.fetchone()[0]
                            if not check_filter_country(country_id, filter_element['option_value']):
                                pass
                        except:
                            pass

                    elif func == 'hourly_price':
                        if not data['price']['isFixed']:
                            if check_filter_hourly_price(data['price']['cost'], filter_element['option_value']):
                                job_weight +=1

                    elif func == 'skill':
                        filter_skills_id = [int(element) for element in filter_element['option_value'][1:-1].split(', ')]
                        percent_skill = check_filter_skills(data['tags'], filter_skills_id)
                        if percent_skill >= 75:
                            job_weight += 1

                        for skill_id in filter_skills_id:
                            sql_query = f"SELECT name FROM skill WHERE id={skill_id}"
                            cursor.execute(sql_query)
                            if cursor.fetchone()[0] in data['title']:
                                job_weight += 1
                                break

                    elif func == 'percent_skill':
                        filter_percent_skill = float(filter_element['option_value'])


                if not work_time_flag:
                    continue

                if not check_filter_percent_skill(percent_skill, filter_percent_skill):
                    pass


                message = filter['name'] + '\n'

                if job_weight == 1:
                    message += '🟩\n\n'
                elif job_weight == 2:
                    message += '🟧 🟧\n\n'
                elif job_weight == 3:
                    message += '🟥 🟥 🟥\n\n'
                else:
                    continue

                message += 'Link:\n\t{}\n\n'.format(data['url'])

                if data['price']['isFixed']:
                    message += 'Price:\n\t${}\n\n'.format(data['price']['cost'])
                else:
                    message += 'Price:\n\t${}-${}\n\n'.format(data['price']['cost']['min'], data['price']['cost']['max'])

                if 80 <= percent_skill <= 100:
                    message += 'Skill rate:\n\t5'
                elif 65 <= percent_skill < 80:
                    message += 'Skill rate:\n\t4'
                elif 50 <= percent_skill < 65:
                    message += 'Skill rate:\n\t3'
                if 40 <= percent_skill < 50:
                    message += 'Skill rate:\n\t2'
                if 0 <= percent_skill < 40:
                    message += 'Skill rate:\n\t1'

                message += '\n\nSkills:\n'

                for skill in data['tags']:
                    message += '\t#{}\n'.format(skill['slug'])

                message += '\nCountry:\n{}'.format(data['client']['location'])

                sql_query = f"SELECT code FROM user WHERE id={filter['id_user']}"
                cursor.execute(sql_query)
                chat_id = cursor.fetchone()[0]

                send_message(chat_id=chat_id, text=message)


    def check_filter_fixed_price(value, filter_text):
        filter = [float(filter_text[1:-1].split(', ')[0].split(': ')[-1]), float(filter_text[1:-1].split(', ')[1].split(': ')[-1])]
        if (min(filter) <= value <= max(filter)):
            return True
        return False

    def check_filter_hourly_price(value, filter_text):
        filter = [float(filter_text[1:-1].split(', ')[0].split(': ')[-1]), float(filter_text[1:-1].split(', ')[1].split(': ')[-1])]
        if min(filter) <= (value['max']-value['min'])/2 + value['min'] <= max(filter):
            return True
        return False

    def check_filter_price(value, filter_text):
        if value['isFixed']:
            return check_filter_fixed_price(value['cost'], filter_text)
        else:
            return check_filter_hourly_price(value['cost'], filter_text)

    def check_filter_skills(value, filter):
        count = 0
        skills_id = [element['id'] for element in value]
        for skill_id in filter:
            if skill_id in skills_id:
                count += 1
        return count/len(filter) * 100

    def check_filter_percent_skill(value, filter):
        if value >= filter:
            return True
        return False

    def check_filter_country(value, filter):
        if value == filter:
            return True
        return False





    def send_message(chat_id, text):
        url = URL_sender + 'sendMessage'
        data = {'chat_id': chat_id, 'text':text}
        requests.post(url, json=data)

    async def scrap_search_page():
        while True:
            url = 'https://www.upwork.com/nx/jobs/search/?sort=recency&per_page=50'

            html = get_html(url=url)
            soup = BeautifulSoup(html, 'lxml')
            try:
                if not check_search_page(title=soup.find('title').text.strip()):
                    write_to_logfile('Bad Search Page')
                    continue
            except:
                continue

            print('Search page in progress')

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
                    data['price']['is_fixed'] = True
                    try:
                        data['price']['value'] = float(section.find('span', attrs={'data-test': 'budget'}).text.strip()[1:].replace(',', ''))
                    except:
                        data['price']['value'] = 0
                else:
                    data['price']['is_fixed'] = False

                    # try:
                    budget = section.find('strong', attrs={'data-test': 'job-type'}).text.strip().split('$')
                    if len(budget) == 1:
                        data['price']['is_fixed'] = True
                        data['price']['value'] = 0
                    elif len(budget) == 2:
                        data['price']['value'] = {'min': float(budget[1].replace('-', '')), 'max': float(budget[1])}
                    else:
                        data['price']['value'] = {'min': float(budget[1].replace('-', '')), 'max': float(budget[2])}
                        # except:
                        #     data['price']['value'] = None

                # experience
                try:
                    data['experience'] = section.find('span', attrs={'data-test': 'contractor-tier'}).text
                except:
                    data['experience'] = None

                # duration
                try:
                    data['duration'] = section.find('span', attrs={'data-test': 'duration'}).text
                except:
                    data['duration'] = None


                data['tags'] = [{'title': tag.text.replace('\'', ''), 'uid': tag['href'].split('=')[-1]} for tag in section.find('div', class_='up-skill-container').find('div', class_='up-skill-wrapper').find_all('a')]
                data['url'] = domain + section.find('div', class_='row my-10').find('h4').find('a')['href'].split('/')[-2] + '/'
                result.append(data)

            save_project_first_data_to_db(result)

            await asyncio.sleep(60)


    async def scrap_project_page():
        while True:
            if task_urls:
                task = task_urls.pop(0)

                print('project page in progress', task['url'])
                html = get_html(url=task['url'])
                soup = BeautifulSoup(html, 'lxml')

                if str(soup) == '<html><head></head><body></body></html>':
                    task_urls.append(task)
                    continue

                if is_interrupted(h1=soup.find('h1').text.strip()):
                    task_urls.append(task)
                    write_to_logfile('{}: {}'.format('Your connection was interrupted', task['url']))
                    continue

                check = check_project_page(h1=soup.find('h1').text.strip())
                if not check[0]:
                    write_to_logfile('{}: {}'.format(check[1], task['url']))
                    continue

                if soup.find('h1').text == 'Find the best freelance jobs':
                    continue

                if soup.find('title').text == 'Access to this page has been denied.':
                    continue

                if soup.find('h1').text == 'Job not found':
                    continue


                result = {}

                # url
                result['url'] = task['url']

                result['uid'] = task['url'].split('~')[1][:-1]
                with db_connection().cursor() as cursor:
                    # project id
                    sql_query = f"SELECT id FROM job WHERE link=\'{result['url']}\'"
                    cursor.execute(sql_query)
                    result['id'] = cursor.fetchone()[0]

                    # title
                    sql_query = f"SELECT name FROM job WHERE id={result['id']}"
                    cursor.execute(sql_query)
                    result['title'] = cursor.fetchone()[0]

                    sections = soup.find_all('section', class_='up-card-section')[:-1]
                    # main tag
                    result['main_tag'] = sections[0].find('div', class_='cfe-ui-job-breadcrumbs d-inline-block mr-10').text.strip()

                    # location
                    result['location'] = sections[0].find('div', class_='mt-20 d-flex align-items-center location-restriction').find('span').text.strip()

                    # description
                    sql_query = f"SELECT description FROM job WHERE id={result['id']}"
                    cursor.execute(sql_query)
                    result['description'] = cursor.fetchone()[0]

                    # price
                    result['price'] = {}

                    sql_query = f"SELECT meta_value FROM meta_job WHERE id_job={result['id']} AND meta_key='price'"
                    cursor.execute(sql_query)
                    price = cursor.fetchone()[0]
                    if 'True' in price:
                        result['price'] = {'isFixed': True, 'cost': float(price[1:-1].split(', ')[-1].split(': ')[-1])}
                    else:
                        result['price'] = {'isFixed': False, 'cost': {'min': float(price[1:-1].split(', ')[1].split(': ')[-1][:-1]), 'max': float(price[1:-1].split(', ')[2].split(': ')[-1][:-1])}}

                    # tags
                    sql_query = f"SELECT meta_value FROM meta_job WHERE id_job={result['id']} AND meta_key='skill'"
                    cursor.execute(sql_query)
                    tags_id = [int(element) for element in cursor.fetchone()[0][1:-1].split(', ')]
                    result['tags'] = []
                    for tag_id in tags_id:
                        sql_query = f"SELECT name, slug FROM skill WHERE id={tag_id}"
                        cursor.execute(sql_query)
                        r = cursor.fetchone()
                        result['tags'].append({'name': r[0], 'slug': r[1], 'id': tag_id})

                    # project_type
                    try:
                        result['project_type'] = soup.find('ul', class_='cfe-ui-job-features p-0 fluid-layout-md').find_all('li')[-1].find('div', class_='header').find('strong').text.strip()
                    except:
                        result['project_type'] = None

                    # activity_on_this_job
                    result['activity_on_this_job'] = {}
                    for li in soup.find('ul', class_='list-unstyled mb-0').find_all('li'):

                        # proposals
                        if li.find('span').text.strip() == 'Proposals':
                            try:
                                result['activity_on_this_job']['proposals'] = li.find('div', class_='d-none d-md-block').find('span').text.strip()
                            except:
                                result['activity_on_this_job']['proposals'] = None

                        # interviewing
                        elif li.find('span').text.strip() == 'Interviewing':
                            try:
                                result['activity_on_this_job']['interviewing'] = int(li.find('div', class_='d-none d-md-block').find('span').text.strip())
                            except:
                                result['activity_on_this_job']['interviewing'] = None

                        # invites sent
                        elif li.find('span').text.strip() == 'Invites sent':
                            try:
                                result['activity_on_this_job']['invites_sent'] = int(li.find('div', class_='d-none d-md-block').find('span').text.strip())
                            except:
                                result['activity_on_this_job']['invites_sent'] = None

                        # unanswered_invites
                        elif li.find('span').text.strip() == 'Unanswered invites':
                            try:
                                result['activity_on_this_job']['unanswered_invites'] = int(li.find('div', class_='d-none d-md-block').find('span').text.strip())
                            except:
                                result['activity_on_this_job']['unanswered_invites'] = None

                        # last vieved by client
                        elif li.find('span').text.strip() == 'Last viewed by client':
                            try:
                                result['activity_on_this_job']['last_vieved_by_client'] = li.find('div', class_='d-none d-md-block').find('span').text.strip()
                            except:
                                result['activity_on_this_job']['last_vieved_by_client'] = None


                    # client
                    result['client'] = {}

                        # location
                    try:
                        result['client']['location'] = soup.find('li', attrs={'data-qa': 'client-location'}).find('strong').text.strip()
                    except:
                        result['client']['location'] = None

                        # client job posting stats
                    # try:
                    result['client']['job_posting_stats'] = {}
                        # jobs posted
                    result['client']['job_posting_stats']['jobs_posted'] = int(soup.find('li', attrs={'data-qa': 'client-job-posting-stats'}).find('strong').text.strip().split(' ')[0])

                    hire_rate_open_jobs = soup.find('li', attrs={'data-qa': 'client-job-posting-stats'}).find('div').text.strip().split(',')

                        # hire rate
                    result['client']['job_posting_stats']['hire_rate'] = int(hire_rate_open_jobs[0].strip().split('%')[0])

                        # open jobs
                    result['client']['job_posting_stats']['open_jobs'] = int(hire_rate_open_jobs[1].strip().split(' ')[0])
                    # except:
                    #     result['client']['job_posting_stats'] = None

                        # client company profile
                    try:
                        result['client']['company_profile'] = soup.find('li', attrs={'data-qa': 'client-company-profile'}).find('strong').text.strip()
                    except:
                        result['client']['company_profile'] = None

                        # client spend
                    try:
                        result['client']['spend'] = soup.find('strong', attrs={'data-qa': 'client-spend'}).find('span').text.split(' ')[0]
                    except:
                        result['client']['spend'] = None

                        #client hires
                    try:
                        hires = soup.find('div', attrs={'data-qa': 'client-hires'}).text.strip()
                        result['client']['hires'] = int(hires.split(',')[0].split(' ')[0])
                        result['client']['active_hires'] = int(hires.split(',')[1].split(' ')[0])
                    except:
                        result['client']['hires'] = None
                        result['client']['active_hires'] = None


                save_advanced_project_data_to_db(result)
                bot_send(result)

            await asyncio.sleep(0.1)


    async def start_scrap():

        task1 = asyncio.create_task(scrap_search_page())
        task2 = asyncio.create_task(scrap_project_page())

        await asyncio.gather(task1, task2)


    asyncio.run(start_scrap())


def bot_config():
    print('bot_config started')
    token = Config.bot_config_token
    URL_config = 'https://api.telegram.org/bot{}/'.format(token)
    message_id = 0
    while True:
        # getUpdates
        URL = URL_config + 'getUpdates'
        try:
            r = requests.get(URL).json()[-1]
        except:
            continue

        with open('message.json', 'w') as file:
            json.dump(r, file, indent=2)

        if not r['ok']:
            continue

        new_message_id = r['update_id']
        if new_message_id == message_id:
            continue

        message_id = new_message_id
        chat_id = r['message']['chat']['id']
        with db_connection() as connection:
            with connection.cursor() as cursor:
                sql_query = f"SELECT EXISTS(SELECT id FROM user WHERE code={chat_id})"
                cursor.execute(sql_query)
                if cursor.fetchone()[0] == 0:
                    time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    sql_query = f"INSERT INTO user(code, name, time_last_update) VALUES({chat_id}, '{r['message']['from']['username']}', '{time}')"
                    cursor.execute(sql_query)
        sleep(3)


def main():
    multiprocessing.Process(target=scrap).start()
    multiprocessing.Process(target=bot_config).start()


if __name__ == '__main__':
    main()
