import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import telegram
import io
import sys
import os

default_args = {
    'owner': 'ri-g',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 17)
    }

schedule_interval = '*/15 * * * *'

connection = {'host': '###',
                      'database':'###',
                      'user':'###', 
                      'password':'###'
                     }

token = '###'
chat_id = '###'
bot = telegram.Bot(token=token)

def check_1(df, a=3, n=5):
    
    df['q25'] = df['user'].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df['user'].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df['user'].iloc[-1] < df['low'].iloc[-1] or df['user'].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

def check_2(df, m=None, a=3, n=5):
    
    df['q25'] = df[m].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[m].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[m].iloc[-1] < df['low'].iloc[-1] or df[m].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df


def make_plot_1(df, act, group, label=None):
    plt.figure(figsize=(8,8))
    plt.suptitle('Динамика метрики {0} в срезе {1}'.format(act, group))
    plt.plot(df['time_15'], df['user'], label=label)
    plt.plot(df['time_15'], df['up'], label='Верхняя граница')
    plt.plot(df['time_15'], df['low'], label='Нижняя граница')
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m, %H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(byhour=None, interval=4, tz=None))
    plt.gcf().autofmt_xdate()
    plt.xlabel('time')
    plt.ylabel(act)
    plt.legend(loc="best")
    plt.show()
    plot_object_1 = io.BytesIO()
    plt.savefig(plot_object_1)
    plot_object_1.seek(0)
    plt.close
    
    return plot_object_1

def make_plot_2(df, m, group, label=None):
    plt.figure(figsize=(8,8))
    plt.suptitle('Динамика метрики {0} в срезе {1}'.format(m, group))
    plt.plot(df['time_15'], df[m], label=label)
    plt.plot(df['time_15'], df['up'], label='Верхняя граница')
    plt.plot(df['time_15'], df['low'], label='Нижняя граница')
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m, %H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(byhour=None, interval=4, tz=None))
    plt.gcf().autofmt_xdate()
    plt.xlabel('time')
    plt.ylabel(m)
    plt.legend(loc="best")
    plt.show()
    plot_object_2 = io.BytesIO()
    plt.savefig(plot_object_2)
    plot_object_2.seek(0)
    plt.close
    
    return plot_object_2


def make_msg_1(df, act, group, label=None):
    msg_1 = '''Метрика {metric} в срезе {group} для {label}:\n текущее значение {current_val:.2f}\n отклонение от предыдущего значения {last_val_diff:.2%} \n @yours_test_bot'''.format(metric=act, group=group, label=label, current_val = df['user'].iloc[-1], last_val_diff = 1 - (df['user'].iloc[-1]/df['user'].iloc[-2]))
                    
    return msg_1


def make_msg_2(df, m, group, label=None):
    msg_2 = '''Метрика {metric} в срезе {group} для {label}:\n текущее значение {current_val:.2f}\n отклонение от предыдущего значения {last_val_diff:.2%} \n @yours_test_bot'''.format(metric=m, group=group, label=label, current_val = df[m].iloc[-1], last_val_diff = 1 - (df[m].iloc[-1]/df[m].iloc[-2]))
                    
    return msg_2

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_task_8_1_1():

    @task()
    def load_users():
        query_users = """
        SELECT toStartOfFifteenMinutes(time) AS time_15, 
        toDate(time) AS date, 
        formatDateTime(time_15, '%R') as hm,
        user_id as user, 
        action,
        os, 
        country,
        source
        FROM {db}.feed_actions
        WHERE date = today() and time < toStartOfFifteenMinutes(now())
        ORDER BY time_15
         """
        df_users = ph.read_clickhouse(query_users, connection=connection)
        return df_users
    
    @task()
    def load_message():     
        query_message = """
        SELECT toStartOfFifteenMinutes(time) AS time_15, 
        toDate(time) AS date, 
        user_id as user,
        reciever_id as mess,
        os,
        country
        FROM {db}.message_actions
        WHERE date = today() and time < toStartOfFifteenMinutes(now())
        ORDER BY time_15
        """
        df_message = ph.read_clickhouse(query_message, connection=connection)
    
        return df_message
    
    @task()
    def load_ctr(): 
        query_ctr = """
        SELECT toDate(time) AS date, sum(action = 'view') as views, sum(action = 'like') as likes, likes/views as ctr
        FROM {db}.feed_actions
        WHERE date >= today() - 8 and date <= today() - 1
        GROUP BY date
        ORDER BY date
        """
        
        df_ctr = ph.read_clickhouse(query_ctr, connection=connection)
  
        return df_ctr
    
    @task()
    def transform_users(df_users):
        url = '####'
        
        os_list = ['Android', 'iOS']
        country_list_top = ['Russia', 'Ukraine', 'Belarus', 'Kazakhstan']
        action_list = ['user', 'like', 'view']
        group_list = ['os', 'country', 'source']
        source_list = ['ads','organic']
        
        for act in action_list:
            for group in group_list:
                if group == 'os':
                    for os_ in os_list:
                        if act == 'user':
                            df_ = df_users.groupby(['time_15', group], as_index=False)['user'].nunique()
                            df_ = df_[df_.os == os_]
                        else:
                            df_ = df_users.groupby(['time_15', group, 'action'], as_index=False)['user'].count()
                            df_ = df_[df_.action == act][df_[df_.action == act].os == os_]

                        #проверка на вхождение в интервал 
                        df = df_[['time_15','user']].copy()
                        is_alert, df = check_1(df)    
                        if is_alert == 1:

                            msg_1 = make_msg_1(df, act, group, label=os_)                    
                            plot_1 = make_plot_1(df, act, group, label=os_)
                            bot.sendMessage(chat_id=chat_id, text=msg_1) 
                            bot.sendMessage(chat_id=chat_id, text='[Ссылка на дашборд]({0})'.format(url), parse_mode='MarkdownV2')
                            bot.sendPhoto(chat_id=chat_id, photo=plot_1)

                elif group == 'country':
                    for country_ in country_list_top:
                        if act == 'user':
                            df_ = df_users.groupby(['time_15', group], as_index=False)['user'].nunique()
                            df_ = df_[df_.country == country_]
                        else:
                            df_ = df_users.groupby(['time_15', group, 'action'], as_index=False)['user'].count()
                            df_ = df_[df_.action == act][df_[df_.action == act].country == country_]

                        #проверка на вхождение в интервал 
                        df = df_[['time_15','user']].copy()
                        is_alert, df = check_1(df)     
                        if is_alert == 1:

                            msg_1 = make_msg_1(df, act, group, label=country_)
                            bot.sendMessage(chat_id=chat_id, text=msg_1) 
                            bot.sendMessage(chat_id=chat_id, text='[Ссылка на дашборд]({0})'.format(url), parse_mode='MarkdownV2')
                            
                            plot_1 = make_plot_1(df, act, group, label=country_)
                            bot.sendPhoto(chat_id=chat_id, photo=plot_1)
                
                elif group == 'source':  
                    for source_ in source_list:
                        if act == 'user':
                            df_ = df_users.groupby(['time_15', group], as_index=False)['user'].nunique()
                            df_ = df_[df_.source == source_]
                        else:
                            df_ = df_users.groupby(['time_15', group, 'action'], as_index=False)['user'].count()
                            df_ = df_[df_.action == act][df_[df_.action == act].source == source_]

                        #проверка на вхождение в интервал 
                        df = df_[['time_15','user']].copy()
                        is_alert, df = check_1(df)     
                        if is_alert == 1:

                            msg_1 = make_msg_1(df, act, group, label=source_)
                            bot.sendMessage(chat_id=chat_id, text=msg_1) 
                            bot.sendMessage(chat_id=chat_id, text='[Ссылка на дашборд]({0})'.format(url), parse_mode='MarkdownV2')
                            
                            plot_1 = make_plot_1(df, act, group, label=source_)
                            bot.sendPhoto(chat_id=chat_id, photo=plot_1)
                            
        
    @task()
    def transform_mess(df_message):
        
        url = '####'
        
        
        os_list = ['Android', 'iOS']
        country_list_top = ['Russia', 'Ukraine', 'Belarus', 'Kazakhstan']
        group_list = ['os', 'country']
        mess_list = ['user', 'mess']

        for mess_ in mess_list:
            for group in group_list:
                if group == 'os':
                    for os_ in os_list:
                        if mess_list == 'user':
                            df_ = df_message.groupby(['time_15', group], as_index=False)['user'].nunique()
                            df_ = df_[df_.os == os_]
                        else:
                            df_ = df_message.groupby(['time_15', group], as_index=False)[mess_].count()
                            df_ = df_[df_.os == os_]

                        #проверка на вхождение в интервал 
                        df = df_[['time_15', mess_]].copy()
                        is_alert, df = check_2(df, m=mess_)    
                        if is_alert == 1:

                            msg_2 = make_msg_2(df, mess_, group, label=os_)                    
                            bot.sendMessage(chat_id=chat_id, text=msg_2) 
                            bot.sendMessage(chat_id=chat_id, text='[Ссылка на дашборд]({0})'.format(url), parse_mode='MarkdownV2')
                            
                            plot_2 = make_plot_2(df, mess_, group, label=os_)
                            bot.sendPhoto(chat_id=chat_id, photo=plot_2)

                elif group == 'country':
                    for country_ in country_list_top:
                        if mess_list == 'user':
                            df_ = df_message.groupby(['time_15', group], as_index=False)['user'].nunique()
                            df_ = df_[df_.country == country_]
                        else:
                            df_ = df_message.groupby(['time_15', group], as_index=False)[mess_].count()
                            df_ = df_[df_.country == country_]

                        #проверка на вхождение в интервал 
                        df = df_[['time_15', mess_]].copy()
                        is_alert, df = check_2(df, m=mess_)     
                        if is_alert == 1:

                            msg_2 = make_msg_2(df, mess_, group, label=country_)
                            bot.sendMessage(chat_id=chat_id, text=msg_2) 
                            bot.sendMessage(chat_id=chat_id, text='[Ссылка на дашборд]({0})'.format(url), parse_mode='MarkdownV2')
                            
                            plot_2 = make_plot_2(df, mess_, group, label=country_)
                            bot.sendPhoto(chat_id=chat_id, photo=plot_2)
                                    
    @task()
    def check_ctr(df_ctr):   
        df = df_ctr[['date','ctr']].copy()
        is_alert, df = check_2(df, m='ctr')     
        if is_alert == 1:

            plt.figure(figsize=(8,8))
            plt.suptitle('Динамика метрики дневного ctr')
            plt.plot(df['date'], df['ctr'], label='ctr')
            plt.plot(df['date'], df['up'], label='Верхняя граница')
            plt.plot(df['date'], df['low'], label='Нижняя граница')
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m'))
            plt.gca().xaxis.set_major_locator(mdates.DayLocator())
            plt.gcf().autofmt_xdate()
            plt.xlabel('time')
            plt.ylabel('ctr')
            plt.legend(loc="best")
            plt.show()

            msg = '''Метрика ctr:\n текущее значение {current_val:.2f}\n отклонение от предыдущего значения {last_val_diff:.2%} \n @yours_test_bot'''.format(current_val = df['ctr'].iloc[-1], last_val_diff = 1 - (df['ctr'].iloc[-1]/df['ctr'].iloc[-2]))

            print(msg)
        
        
#     @task()
#     def send_plot(chat_id, plot_object):
#         bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
#     @task()
#     def send_msg(chat_id, msg, url):
#         bot.sendMessage(chat_id=chat_id, text=msg) 
#         bot.sendMessage(chat_id=chat_id, text='[Ссылка на дашборд]({0})'.format(url), parse_mode='MarkdownV2')
              
    df_users = load_users()
    df_message = load_message()
    df_ctr = load_ctr()
    transform_users(df_users) >>  transform_mess(df_message) >> check_ctr(df_ctr)
    
    
       
dag_task_8_1_1 = dag_task_8_1_1()   