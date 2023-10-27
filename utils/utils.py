import numpy as np
import pandas as pd
import random
import string
import datetime


def create_data_version_control(df,name=""):
    dvc_file_name = f"{name}_{gen_dataset_dvc(8)}.csv"
    df.to_csv(dvc_file_name,index=False)
    return dvc_file_name

def gen_dataset_dvc(length=8):
    return f'{get_current_date()}_{get_random_string(length)}'

def get_current_date():
    return f'{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}'

def get_random_string(length=8,str_list=string.ascii_letters):
    return ''.join(random.choice(str_list) for i in range(length))

def addZerosCpf(cpf):
    new_cpf = str(cpf)
    for i in range(11-len(new_cpf)):
        new_cpf = '0'+new_cpf
    return new_cpf

def fc_log_inicio(count_log, max_log, mensagem):
    count_log = count_log + 1
    print(f"{count_log}/{max_log} {mensagem}")
    time_inicio = datetime.datetime.now()
    return count_log, time_inicio

def fc_log_fim(time_inicio):
    tempo = round((datetime.datetime.now() - time_inicio).total_seconds()/60, 2)
    print(f"Tempo de execução: {tempo} min")
    print("--------------------\n")
    
def convert_timestamps_to_string(data_dict):
    converted_dict = data_dict.copy()
    for key, value in data_dict.items():
        if 'timestamp' in value:
            converted_dict[key] = 'string'
        if 'date' in value:
            converted_dict[key] = 'string'
        if 'decimal' in value:
            converted_dict[key] = 'float'
    return converted_dict