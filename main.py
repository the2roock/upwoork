import asyncio
import pymysql
import json
import multiprocessing
import requests

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from telebot import TeleBot

from datetime import datetime
from os import path
from time import sleep
from ast import literal_eval

from config import Config

def db_connection():
    return pymysql.connect(host=Config.db_host, user=Config.db_user, database=Config.db_name, password=Config.db_password)

def scrap():
    print('scrap started')
    task_urls = []
    URL = 'https://api.telegram.org/bot{}/'.format(Config.bot_sender_token)

    def get_html(url):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36')
        options.add_argument("no-sandbox")
        options.add_argument("--disable-extensions")
        driver = webdriver.Chrome(executable_path='./chromedriver', chrome_options=options)

        try:
            driver.get(url=url)
            sleep(3)
            result = driver.page_source
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


    def save_advanced_project_data_to_db(data):
        with db_connection() as connection:
            with connection.cursor() as cursor:
                sql_query = f"SELECT EXISTS(SELECT id FROM country WHERE slug=\'{data['client']['location'].lower().replace(' ', '_')}\')"
                cursor.execute(sql_query)
                if cursor.fetchone()[0] == 0:
                    sql_query = f"INSERT INTO country(name, slug) VALUES(\'{data['client']['location']}\', \'{data['client']['location'].lower().replace(' ', '_')}\')"
                    cursor.execute(sql_query)
                sql_query = f"SELECT id FROM country WHERE slug=\'{data['client']['location'].lower().replace(' ', '_')}\'"
                cursor.execute(sql_query)
                sql_query = f"INSERT INTO meta_job(id_job, meta_key, meta_value) VALUES({data['id']}, 'country', \'{cursor.fetchone()[0]}\')"
                cursor.execute(sql_query)


    def bot_send(data):
        with open('json.json', 'w') as file:
            json.dump(data, file, indent=2)
        with db_connection().cursor() as cursor:
            sql_query = "select chat_id from user where status=1"
            cursor.execute(sql_query)
            chats_id = cursor.fetchall()
            for chat_id in chats_id:
                filter1 = {'filter_number': 1, 'price': {'is_fixed': True, 'value':{'min': 3, 'max': None}}, 'tags': {'tags': {'SEO', 'Data Entry', 'Developer', 'Writing', 'Word'}, 'percent': 0}, 'countries': None, 'client_rate': None}
                filter2 = {'filter_number': 2, 'price': {'is_fixed': False, 'value':{'min': 10, 'max': 100}}, 'tags': None, 'countries': None, 'client_rate': {'min': 40, 'max': None}}
                filters = [filter1, filter2]

                for filter in filters:
                    message = f'{filter["filter_number"]}\nurl:\n\t' + data['url'] + '\n' + '\nprice:\n\t'
                    project_weight = 0

                    # price
                    if filter['price'] == None:
                        if data['price']['is_fixed']:
                            message += '\tFixed: {}\n'.format(data['price']['value']['value'])
                        else:
                            message += '\tHourly: ${} - ${}\n'.format(data['price']['value']['left_lim'], data['price']['value']['right_lim'])
                    else:
                        if data['price']['is_fixed'] != filter['price']['is_fixed']:
                            continue
                        if data['price']['is_fixed']:
                            if filter['price']['value'] != 0:
                                if filter['price']['value']['min'] != None:
                                    if data['price']['value']['value'] < filter['price']['value']['min']:
                                        continue
                                if filter['price']['value']['max'] != None:
                                    if data['price']['value']['value'] > filter['price']['value']['max']:
                                        continue
                            project_weight += 1
                            message += '\tFixed: {}\n'.format(data['price']['value']['value'])
                        else:
                            if filter['price']['value'] != 0:
                                if filter['price']['value']['min'] != None:
                                    if data['price']['value']['left_lim'] < filter['price']['value']['min']:
                                        continue
                                if filter['price']['value']['max'] != None:
                                    if data['price']['value']['right_lim'] > filter['price']['value']['max']:
                                        continue
                            project_weight += 1
                            message += '\tHourly: ${} - ${}\n'.format(data['price']['value']['left_lim'], data['price']['value']['right_lim'])

                    # tags
                    message += '\nSkills: \n'
                    if filter['tags'] == None:
                        for tag in set(data['tags']):
                            message += '\t' + tag + '\n'
                    else:
                        tag_count = 0
                        for tag in set(data['tags']):
                            if tag in filter['tags']['tags']:
                                tag_count += 1
                            message += '\t' + tag + '\n'
                        tag_percent = tag_count/len(filter['tags']['tags']) * 100
                        if tag_percent < filter['tags']['percent']:
                            continue
                        if 80 <= tag_percent <= 100:
                            message += '\nTag rate: 5\n'
                        elif 65 <= tag_percent < 80:
                            message += '\nTag rate: 4\n'
                        elif 50 <= tag_percent < 65:
                            message += '\nTag rate: 3\n'
                        elif 40 <= tag_percent < 50:
                            message += '\nTag rate: 2\n'
                        elif 0 <= tag_percent < 40:
                            message += '\nTag rate: 1\n'

                        if tag_percent >= 75:
                            project_weight += 1

                        for tag in filter['tags']['tags']:
                            if tag.lower() in data['title'].lower():
                                project_weight += 1
                                break

                    if project_weight == 1:
                        message = '🟩\n' + message
                    elif project_weight == 2:
                        message = '🟧 🟧\n' + message
                    elif project_weight == 3:
                        message = '🟥 🟥 🟥\n' + message
                    # client country
                    if filter['countries'] != None:
                        if not (data['client']['location'] in filter['countries']):
                            continue
                    message += '\nCountry: ' + data['client']['location']

                    # client rate
                    if filter['client_rate'] != None:
                        if filter['client_rate']['min'] != None:
                            if data['client']['job_posting_stats']['hire_rate'] < filter['client_rate']['min']:
                                continue
                        if filter['client_rate']['max'] != None:
                            if data['client']['job_posting_stats']['hire_rate'] > filter['client_rate']['max']:
                                continue

                    send_message(URL=URL, chat_id=chat_id[0], text=message)

    def send_message(URL, chat_id, text):
        url = URL + 'sendMessage'
        data = {'chat_id': chat_id, 'text':text}
        requests.post(url, json=data)

    async def scrap_search_page():
        while True:
            url = 'https://www.upwork.com/nx/jobs/search/?sort=recency&per_page=50'

            html = get_html(url=url)
            soup = BeautifulSoup(html, 'lxml')

            if not check_search_page(title=soup.find('title').text.strip()):
                write_to_logfile('Bad Search Page')
                await asyncio.sleep(34)

            print('Search page in progress')

            result = []
            sections = soup.find_all('section', class_='up-card-section up-card-list-section up-card-hover')
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

                try:
                    data['tags'] = [{'title': tag.text.replace('\'', ''), 'uid': tag['href'].split('=')[-1]} for tag in section.find('div', class_='up-skill-container').find('div', class_='up-skill-wrapper').find_all('a')]
                except:
                    data['tags'] = [{'title': '0', 'uid': '0'}]
                data['url'] = domain + section.find('div', class_='row my-10').find('h4').find('a')['href'].split('/')[-2] + '/'
                result.append(data)

            save_project_first_data_to_db(result)

            await asyncio.sleep(34)


    async def scrap_project_page():
        while True:
            if task_urls:
                task = task_urls.pop(0)

                print('project page in progress', task['url'])
                html = get_html(url=task['url'])
                soup = BeautifulSoup(html, 'lxml')

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
                    result['tags'] = {}
                    for tag_id in tags_id:
                        sql_query = f"SELECT name, slug FROM skill WHERE id={tag_id}"
                        cursor.execute(sql_query)
                        r = cursor.fetchone()
                        result['tags']['name'] = r[0]
                        result['tags']['slug'] = r[1]


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

                with open('project_data.json', 'w') as file:
                    json.dump(result, file, indent=2, ensure_ascii=False)
                save_advanced_project_data_to_db(result)
                # bot_send(result)
            await asyncio.sleep(0.1)


    async def start_scrap():

        task1 = asyncio.create_task(scrap_search_page())
        task2 = asyncio.create_task(scrap_project_page())

        await asyncio.gather(task1, task2)


    asyncio.run(start_scrap())


def bot_config():
    print('bot_config started')
    token = Config.bot_config_token

    bot = TeleBot(token)
    @bot.message_handler(content_types=['text'])
    def main(message):
        chat_id = message.chat.id
        with db_connection() as connection:
            with connection.cursor() as cursor:
                ################### /start  #####################
                if message.text == '/start':
                    pass





                elif message.text == '/start scrap':
                    sql_query = f"SELECT EXISTS(SELECT id FROM user WHERE chat_id=\'{chat_id}\')"
                    cursor.execute(sql_query)
                    if cursor.fetchone()[0] == 0:
                        sql_query = f"select max(id) from user"
                        cursor.execute(sql_query)
                        try:
                            user_id = cursor.fetchone()[0] + 1
                        except:
                            user_id = 1

                        sql_query = f"insert into user(id, chat_id, status) values({user_id}, {chat_id}, 1)"
                        cursor.execute(sql_query)
                    else:
                        sql_query = f"update user set status=1 where chat_id={chat_id}"
                        cursor.execute(sql_query)
                    bot.send_message(chat_id, 'OK, please wait...')

                ################### /stop  #####################
                elif message.text == '/stop':
                    sql_query = f"update user set status=0 where chat_id={chat_id}"
                    cursor.execute(sql_query)
                    bot.send_message(chat_id, 'OK')
                else:
                    bot.send_message(chat_id, 'to start enter /start\nto stop enter /stop')
            connection.commit()

    bot.polling(none_stop=True)


def main():
    multiprocessing.Process(target=scrap).start()
    multiprocessing.Process(target=bot_config).start()


if __name__ == '__main__':
    main()
