from airflow.providers.telegram.hooks.telegram import TelegramHook
import os
from dotenv import load_dotenv, find_dotenv

def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='telegram_conn_id', token='7304991854:AAGiGaq5fPFgIWCbvFIaB6GPub2CLy9F4_U', chat_id='-4273708210')
    dag_info = context['dag']
    run_id = context['run_id']
    message = f'Даг {dag_info} с номером запуска run_id={run_id} завершился с ошибкой.'
    hook.send_message({
        'chat_id': '-4273708210',
        'text': message,
        'parse_mode': None
    })
 
 
def send_telegram_success_message(context):
    hook = TelegramHook(telegram_conn_id='telegram_conn_id', token='7304991854:AAGiGaq5fPFgIWCbvFIaB6GPub2CLy9F4_U', chat_id='-4273708210')
    dag_info = context['dag']
    run_id = context['run_id']
    message = f'Даг {dag_info} с номером запуска run_id={run_id} завершился успешно.'
    hook.send_message({
        'chat_id': '-4273708210',
        'text': message,
        'parse_mode': None
    })

