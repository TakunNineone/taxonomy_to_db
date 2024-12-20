import gc,os
import datetime,pandas as pd
import sys

import parseDicNew, parseTab, parseMetaInf, parseIFRS_FULL,parseBadFiles,skripts

from sqlalchemy import create_engine,text
from bs4 import  BeautifulSoup
version = 'dko_txnm_v0.3.5'

roles_table_definition_6=pd.read_csv("Сопоставление-ролей-definition-и-table.csv",header=0)
roles_table_definition_5_3=pd.read_csv('bfo_roles_definition_table_5_3.csv',header=0)


print('begin', datetime.datetime.now())
conn_string = f'postgresql+psycopg2://postgres:124kosm21@127.0.0.1/{version}'
db = create_engine(conn_string,isolation_level="AUTOCOMMIT")
conn = db.connect()
print(conn)



with open(f'{os.getcwd()}\\{version}\\description.xml','r',encoding='utf-8') as f:
    period=BeautifulSoup(f,'lxml')
period=f"{period.find('version').text[:4]}-{period.find('version').text[4:6]}-{period.find('version').text[6:8]}"
try:
    nso=os.listdir(f'{os.getcwd()}\\{version}\\www.cbr.ru\\xbrl\\nso')
except:
    nso=[]
nso=[[xx,xx if xx!='operatory' else 'oper']  for xx in nso]


sql_delete = skripts.sql_delete
sql_create_functions = skripts.sql_create_functions
sql_indexes = skripts.sql_indexes
sql_indexes_new= skripts.sql_indexes_new
sql_create_elements_labels = skripts.sql_create_elements_labels
sql_create_preferred_labels = skripts.sql_create_preferred_labels
sql_create_dop_tables = skripts.sql_create_dop_tables

conn.execute(text(sql_delete))
conn.commit()

print('parseXsdBadFiles', version)
ss=parseBadFiles.c_parseBadFiles(version)
df_list=ss.find_folders(period)
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list
gc.collect()

print('parseMetaInf', version)
ss = parseMetaInf.c_parseMeta(version)
df_list = ss.parseentry() | ss.parsecatalog() | ss.parsetaxpackage()
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list
gc.collect()

if nso!=[]:
    # nso=[['purcb','purcb']]
    for rinok in nso:
        print('parseTab', rinok)
        ss1 = parseTab.c_parseTab(version, rinok[0], rinok[1], period)
        df_list1 = ss1.startParse()
        df_list1 = {k: v for k, v in df_list1.items() if v is not None}
        str_headers = ''
        for xx in df_list1.keys():
            headers = [xx.strip() + ' VARCHAR, ' for xx in df_list1.get(xx).keys().values]
            for hh in headers:
                str_headers = str_headers + hh + '\n'
            str_headers = str_headers.strip()[:-1]
            df_list1.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
        del df_list1, ss1
        gc.collect()

        print('parseDic', rinok)
        ss2 = parseDicNew.c_parseDic(version, rinok[0], rinok[1])
        df_list2 = ss2.startParse()
        df_list2 = {k: v for k, v in df_list2.items() if v is not None}
        print(df_list2.keys())
        str_headers = ''
        for xx in df_list2.keys():
            headers = [xx.strip() + ' VARCHAR, ' for xx in df_list2.get(xx).keys().values]
            for hh in headers:
                str_headers = str_headers + hh + '\n'
            str_headers = str_headers.strip()[:-1]
            df_list2.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
        del df_list2, ss2
        gc.collect()

ss = parseDicNew.c_parseDic(version, 'udr\\dim', 'dim')
df_list = ss.startParse()
df_list = {k: v for k, v in df_list.items() if v is not None}
print('udr\\dim', df_list.keys())
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list
gc.collect()

ss = parseDicNew.c_parseDic(version, 'udr\\dom', 'mem')
df_list = ss.startParse()
df_list = {k: v for k, v in df_list.items() if v is not None}
print('udr\\dom', df_list.keys())
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list
gc.collect()

ss = parseDicNew.c_parseDic(version, 'udr\\dom', 'iss-mem')
df_list = ss.startParse()
df_list = {k: v for k, v in df_list.items() if v is not None}
print('udr\\dom', df_list.keys())
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list
gc.collect()




conn.execute(text (sql_indexes))
conn.execute(text (sql_create_functions))
conn.execute(text (sql_create_elements_labels))
conn.execute(text (sql_create_preferred_labels))
conn.execute(text (sql_create_dop_tables))
# if 'final_6' in version:
#     roles_table_definition_6.to_sql('roles_table_definition', conn, if_exists='replace', index=False)
# elif 'final_5_3' in version:
#     roles_table_definition_5_3.to_sql('roles_table_definition', conn, if_exists='replace', index=False)

conn.commit()
conn.close()
print('end', datetime.datetime.now())
