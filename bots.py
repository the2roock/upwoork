import json
import requests

from db_connection import db_connection
from datetime import datetime
from time import sleep
from os import path

from config import Config

URL_sender = 'https://api.telegram.org/bot{}/'.format(Config.bot_sender_token)


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
            data_cost = 0
            skill_flag = False

            for filter_element in filter_elements:
                sql_query = f"SELECT func FROM option_for_filter WHERE id={filter_element['id_option']}"
                cursor.execute(sql_query)
                func = cursor.fetchone()[0]

                if func == 'work_time':
                    value =  [element.split(':') for element in filter_element['option_value'].split('-')]
                    time = datetime.now()
                    minutes = []
                    minutes.append(int(value[0][0])*60 + int(value[0][1]))
                    if value[1] == ['00', '00']:
                        minutes.append(1440)
                    else:
                        minutes.append(int(value[1][0])*60 + int(value[1][1]))
                    if not (minutes[0] <= (time.hour*60 + time.minute) <= minutes[1]):
                        work_time_flag = False

                elif func == 'fixed_price':
                    if data['price']['isFixed']:
                        if check_filter_fixed_price(data['price']['cost'], filter_element['option_value']):
                            data_cost += 1
                            job_weight += 1


                elif func == 'price':
                    if check_filter_price(data['price'], filter_element['option_value']):
                        data_cost += 1
                        job_weight += 1

                elif func == 'hourly_price':
                    if not data['price']['isFixed']:
                        if check_filter_hourly_price(data['price']['cost'], filter_element['option_value']):
                            data_cost += 1
                            job_weight += 1

                elif func == 'country':
                    sql_query = f"SELECT id FROM country WHERE slug={data['location'].lower().replace(' ', '_')}"
                    cursor.execute(sql_query)
                    country_id = cursor.fetchone()[0]
                    if check_filter_country(country_id, filter_element['option_value']):
                        data_cost += 1

                elif func == 'skill':
                    filter_skills_id = [int(element) for element in filter_element['option_value'][1:-1].split(', ')]
                    percent_skill = check_filter_skills(data['tags'], filter_skills_id)
                    if percent_skill >= 75:
                        job_weight += 1
                    if percent_skill > 1:
                        data_cost += percent_skill / 10

                    for skill_id in filter_skills_id:
                        sql_query = f"SELECT name FROM skill WHERE id={skill_id}"
                        cursor.execute(sql_query)
                        if cursor.fetchone()[0].lower() in data['title'].lower():
                            job_weight += 1
                            data_cost += 1
                            break

                elif func == 'percent_skill':
                    filter_percent_skill = float(filter_element['option_value'])
                    if check_filter_percent_skill(percent_skill, filter_percent_skill):
                        data_cost += 1

            print(f"{filter['id_user']}: {filter['name']}, {data_cost}")
            if (data_cost < 3) and (job_weight < 2):
                continue

            message = filter['name'] + '\n'

            if job_weight == 1:
                message += '🟩\n\n'
            elif job_weight == 2:
                message += '🟧 🟧\n\n'
            elif job_weight == 3:
                message += '🟥 🟥 🟥\n\n'


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

            file_name = './waiting_message/user_{}.json'.format(filter['id_user'])
            if not work_time_flag:
                if not path.exists(file_name):
                    with open(file_name, 'w') as file:
                        file.write('{"messages":[]}')
                with open(file_name, 'r') as file:
                    waiting_messages = json.load(file)
                    waiting_messages["messages"].append({"message": message})
                with open(file_name, 'w') as file:
                    json.dump(waiting_messages, file)
            else:
                if path.exists(file_name):
                    with open(file_name, 'r') as file:
                        messages = json.load(file_name)["messages"]
                    for element in messages:
                        send_message(chat_id=filter['id_user'], text=element['message'])
                else:
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
    response = requests.post(url, json=data)
    with open('response.json', 'w') as file:
        json.dump(response.json(), file, indent=2)


def bot_config():
    print('bot_config started')
    token = Config.bot_config_token
    URL_config = 'https://api.telegram.org/bot{}/'.format(token)
    message_id = 0
    while True:
        # getUpdates
        URL = URL_config + 'getUpdates'
        try:
            r = requests.get(URL).json()
        except:
            continue

        if not r['ok']:
            continue

        try:
            message = r['result'][-1]['message']
        except:
            continue

        with open('message.json', 'w') as file:
            json.dump(r, file, indent=2)


        new_message_id = r['result'][-1]['update_id']
        if new_message_id == message_id:
            continue

        message_id = new_message_id
        chat_id = message['chat']['id']
        with db_connection() as connection:
            with connection.cursor() as cursor:
                sql_query = f"SELECT EXISTS(SELECT id FROM user WHERE code={chat_id})"
                cursor.execute(sql_query)
                if cursor.fetchone()[0] == 0:
                    time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    sql_query = f"INSERT INTO user(code, name, time_last_update) VALUES({chat_id}, '{message['from']['username']}', '{time}')"
                    cursor.execute(sql_query)
            connection.commit()
        sleep(3)
