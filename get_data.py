import pandas as pd
import requests
from datetime import datetime, timedelta


def get_df(url, event):

    headers = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en,en-US;q=0.9,vi;q=0.8,en-GB;q=0.7',
        'cookie': '_ga=GA1.1.1069808236.1728375592; wnt.live.sid=s%3AaekKEWvTKO0r-4EJEvhXsQq1HCX8O6SE.vIKcvpSP%2FL5MPg0G1pWMaJmEVesHuSMJc6OrwZULHDs; _ga_8F96GDL1ZW=GS1.1.1728375590.1.1.1728376962.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0',
        'x-requested-with': 'XMLHttpRequest'
    }

    response = requests.get(url, headers=headers)

    json_data = response.json()

    data = json_data.get('matches')

    df = []
    for i in data:
        row = [
            i.get('uniqueId'), i.get('modifiedUnixTmp'), i.get('tableName'), i.get('roundNumber'), i.get('length'), 
            (datetime.strptime(i.get('dateStart'), '%Y-%m-%d %H:%M:%S') + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S'), 
            (datetime.strptime(i.get('dateEnd'), '%Y-%m-%d %H:%M:%S') + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S'), 
            (datetime.strptime(i.get('dateScheduled'), '%Y-%m-%d %H:%M:%S') + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S'), 
            i.get('status'),
            f'{i.get('players')[0].get('name')} {i.get('players')[0].get('surname')}', f'{i.get('players')[1].get('name')} {i.get('players')[1].get('surname')}',
            i.get('players')[0].get('country').get('name'), i.get('players')[1].get('country').get('name'),
            i.get('scores')[0], i.get('scores')[1],
            event
        ]
        df.append(row)

    return df


def get_csv():
    
    url_list = [
        ['https://www.wntlivescores.com/events/hanoi-open-pool-championship-2024/group-matches/1/1/1728197536', 'Hanoi Open Pool Championship 2024'],
        ['https://www.wntlivescores.com/events/hanoi-open-pool-championship-2024/group-matches/2/1/1728197536', 'Hanoi Open Pool Championship 2024']
        ]

    df = []
    for i in url_list:
        df.extend(get_df(i[0], i[1]))

    df = pd.DataFrame(df, columns=['uniqueId', 'modifiedUnixTmp', 'tableName', 'roundNumber', 'length', 'dateStart', 'dateEnd', 'dateScheduled', 'status', 'player1', 'player2', 'country1', 'country2', 'score1', 'score2', 'event'])

    df.to_csv('wnt_matches.csv', index=False)
    #df.to_csv('wnt_matches.csv', mode='a', header=False, index=False)

get_csv()
# def import_data(dataframe, table_name):
#     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'mindful-rhythm-344416-fd1a2d1aad0f.json'

#     client = bigquery.Client()

#     table_id = 'mindful-rhythm-344416.wnt.' + table_name

#     job_config = bigquery.LoadJobConfig(
#         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
#     )

#     job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)