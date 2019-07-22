import subprocess

import pandas as pd
from flask import Flask
from werkzeug.contrib.cache import SimpleCache

CRT_FILE = '/etc/jupyterhub/jupyterhub.crt'
KEY_FILE = '/etc/jupyterhub/jupyterhub.key'
PORT = 12304
HOST = '0.0.0.0'
JUPYTER_HOST_API_USERS = 'http://127.0.0.1:8081/jupyterhub/hub/api/users'
JUPYTER_HOST_API_TEMPLATE = 'http://127.0.0.1:12301/user/{user}/api/sessions'
JUPYTER_API_TOKEN = 'ну как же'


cache = SimpleCache()
app = Flask(__name__)
style = '''
       <meta http-equiv="refresh" content="10" >
       <style type="text/css">
       .dataframe {
         border: solid 1px #DDEEEE;
         border-collapse: collapse;
         border-spacing: 0;
         font: normal 14px Arial, sans-serif;
       }
       .dataframe thead th {
         background-color: #DDEFEF;
         border: solid 1px #DDEEEE;
         color: #336B6B;
         padding: 4px;
         text-align: left;
         text-shadow: 1px 1px 1px #fff;
       }
       tr:nth-child(odd) {
           background-color: #f8f8f8;
           opacity: .999;
       }
       .dataframe tbody td {
           position: relative;
           border: solid 1px #DDEEEE;
           color: #333;
           padding: 4px;
           text-shadow: 1px 1px 1px #fff;
           text-align: right;
           width: 150px;
       }
       .bg {
           position: absolute;
           left: 0;
           top: 0;
           bottom: 0;
           background-color: #03c03c;
           z-index: -1;
       }
       .by {
           position: absolute;
           left: 0;
           top: 0;
           bottom: 0;
           background-color: #ffd700;
           z-index: -1;
       }
       .br {
           position: absolute;
           left: 0;
           top: 0;
           bottom: 0;
           background-color: #ff4040;
           z-index: -1;
       }
       </style>
'''


def convert_mem(b):
    if 100 > b:
        return "{:.2f}".format(b)
    elif 100*1024 > b > 100:
        return "{:.2f} M".format(b/1024)
    else:
        return "{:.2f} G".format(b/1024/1024)


def get_flag(x):
    if x > 0.5:
        return 'r'
    elif x > 0.2:
        return 'y'
    return 'g'


def calculate_table():
    ps = subprocess.Popen(["ps", "-ax", "--no-headers", "-o", "rss,user"], stdout=subprocess.PIPE).stdout.read()

    ram = pd.DataFrame(
        [x.split() for x in ps.decode('utf-8').split('\n')],
        columns=['mem', 'user']
    )

    ram['user'] = ram['user'].map(
        lambda x: x if (x and (('.' in x) or ('_user' in x))) else '~other~'
    )
    ram['mem'] = ram['mem'].astype(float)

    ram = ram.groupby('user').sum().reset_index().sort_values('mem', ascending=False)
    sum_ram = sum(ram['mem'])

    free_mem = 'rowname ' + subprocess.Popen(
        ["free", "-m"],
        stdout=subprocess.PIPE
    ).stdout.read().strip().decode('utf-8').replace(':', '')

    free_mem_df = pd.DataFrame(
        [x.split() for x in free_mem.split('\n')]
    )
    free_mem_df.columns = free_mem_df.iloc[0]
    free_mem_df = free_mem_df.iloc[1:].set_index('rowname')

    ram['% mem (usage)'] = ram['mem'] / sum_ram
    ram['% mem (all)'] = ram['mem'] / float(free_mem_df['total']['Mem']) / 1024
    ram['mem'] = ram['mem'].map(convert_mem)

    ram = ram.append(
        {
            'mem': "{:.2f} G".format(sum_ram / 1024 / 1024),
            '% mem (all)': sum_ram / float(free_mem_df['total']['Mem']) / 1024
        },
        ignore_index=True
    )

    ram['flag'] = ram['% mem (all)'].map(get_flag)
    ram.iloc[-1, -1] = get_flag(ram.iloc[-1, -2] - 0.4)
    ram['% mem (usage)'] = ['{:.2f}'.format(x*100) if 0 <= x <= 1 else ''  for x in ram['% mem (usage)']]
    ram['% mem (all)'] = ['{:.2f}'.format(x*100) for x in ram['% mem (all)']]
    ram['% mem (all)'] = [
        (
                '<div class="b{}" style="width: {:.2f}%"></div>'.format(y, float(x)) + x
        ) for x, y in zip(ram['% mem (all)'], ram['flag'])
    ]
    return ram.drop('flag', axis=1).to_html(index=False, na_rep='').replace('&lt;', '<').replace('&gt;', '>')


def calculate_cpu():
    ps = subprocess.Popen(["ps", "-ax", "--no-headers", "-o", "%cpu,user"], stdout=subprocess.PIPE).stdout.read()

    cpu = pd.DataFrame(
        [x.split() for x in ps.decode('utf-8').split('\n')],
        columns=['cpu', 'user']
    )

    cpu['user'] = cpu['user'].map(
        lambda x: x if (x and (('.' in x) or ('_user' in x))) else '~other~'
    )
    cpu['cpu'] = cpu['cpu'].astype(float) / 64.

    cpu = cpu.groupby('user').sum().reset_index().sort_values('cpu', ascending=False)
    sum_cpu = sum(cpu['cpu'])

    cpu['cpu (usage)'] = cpu['cpu'] / sum_cpu
    cpu['cpu'] = cpu['cpu'].map(lambda x: "{:.2f}".format(x))

    cpu = cpu.append(
        {
            'cpu': "{:.2f}".format(sum_cpu)
        },
        ignore_index=True
    )
    return cpu.to_html(index=False, na_rep='').replace('&lt;', '<').replace('&gt;', '>')


def get_jupyterhub_table():
    import requests
    import subprocess
    import re
    from datetime import timedelta
    import pandas as pd
    import numpy as np
    def get_jupyterhub_users():
        token = JUPYTER_API_TOKEN
        api_url = JUPYTER_HOST_API_USERS
        r = requests.get(api_url, headers={'Authorization': 'token ' + token})

        r.raise_for_status()
        users = [j['name'] for j in r.json() if j['servers']]
        return users

    def get_kernels(users):
        token = JUPYTER_API_TOKEN
        rl = []
        for user in users:
            api_url = JUPYTER_HOST_API_TEMPLATE
            try:
                r = requests.get(
                    api_url.format(user=user),
                    headers={'Authorization': 'token ' + token},
                    timeout=(1, 1)
                )

                r.raise_for_status()
                l = r.json()
                for j in l:
                    rj = {}
                    for k in j:
                        if type(j[k]) == dict:
                            for kk in j[k]:
                                rj[k+'_'+kk] = j[k][kk]
                        else:
                            rj[k] = j[k]
                    rj['name'] = user
                    rl.append(rj)
            except:
                rl.append({'name': user})
        df = pd.DataFrame(rl)
        df = df[[
            'kernel_connections', 'kernel_execution_state', 'kernel_id', 'kernel_last_activity',
            'name', 'notebook_path', 'type'
        ]]
        df.columns = ['connections', 'execution_state', 'kernel_id', 'last_activity', 'name', 'notebook_path', 'type']
        return df

    def get_ipykernel_launcher():
        output = subprocess.Popen(
            ["ps", "-ax", "--no-headers", "-o", "pid,command"],
            stdout=subprocess.PIPE).stdout.read()
        result = [re.split('\s+', ' ' + x, 2)[1:]
                for x in output.decode('utf-8').split('\n')
                if x and 'ipykernel_launcher' in x]
        result = pd.DataFrame(result, columns=['pid', 'kernel_id'])
        result['kernel_id'] = result['kernel_id'].map(lambda x: re.findall('/kernel-(.*).json', x)[0])
        return result

    def get_pstree():
        output = subprocess.Popen(
            ["ps", "-axf", "--no-headers", "-o", "pid,ppid,rss,%cpu,user,command"],
            stdout=subprocess.PIPE).stdout.read()
        return [re.split('\s+', ' ' + x, 6)[1:] for x in output.decode('utf-8').split('\n') if x]

    def get_by_parent(parent, data):
        result = [get_by_parent(x[0], data) for x in data if (x[1] == parent)]
        return {parent: result} if len(result) > 0 else parent

    def convert_mem(b):
        b = int(b)
        if 100 > b:
            return "{:.2f}".format(b)
        elif 100*1024 > b > 100:
            return "{:.2f} M".format(b/1024)
        else:
            return "{:.2f} G".format(b/1024/1024)

    board = pd.merge(
        get_kernels(get_jupyterhub_users()),
        get_ipykernel_launcher(),
        on='kernel_id',
        how='outer'
    )

    pstree = get_pstree()
    dict_pstree = {x[0]: x[1:] for x in pstree}

    board['tree_pid'] = board['pid'].map(lambda x: get_by_parent(x, pstree))
    board['last_activity'] = pd.to_datetime(board['last_activity']).map(lambda x: x + timedelta(hours=3) if x else None)
    board['ram'] = board['pid'].map(lambda x: convert_mem(dict_pstree[x][1]) if (type(x) != float) or (not np.isnan(x)) else None)

    board['connections'] = board['connections'].fillna('*** потеряшка ***')
    board['notebook_path'] = board['notebook_path'].fillna('*** потеряшка ***')

    board = board.sort_values('name')

    return board.to_html(index=False, na_rep='').replace('&lt;', '<').replace('&gt;', '>')


@app.route('/')
def how():
    table_usage_memory = cache.get('usage_memory')
    if table_usage_memory is None:
        table_usage_memory = calculate_table()
        cache.set('usage_memory', table_usage_memory, timeout=20)

    table_usage_cpu = cache.get('usage_cpu')
    if table_usage_cpu is None:
        table_usage_cpu = calculate_cpu()
        cache.set('usage_cpu', table_usage_cpu, timeout=20)

    table_usage_jupyterhub = cache.get('usage_jupyterhub')
    if table_usage_jupyterhub is None:
        table_usage_jupyterhub = get_jupyterhub_table()
        cache.set('usage_jupyterhub', table_usage_jupyterhub, timeout=20)
    return style + table_usage_memory + '<br>' + table_usage_cpu + '<br>' + table_usage_jupyterhub



ssl_context = (CRT_FILE, KEY_FILE) if (CRT_FILE and KEY_FILE) else None
app.run(
    host=HOST,
    port=PORT,
    ssl_context=ssl_context
)
