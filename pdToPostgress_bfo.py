import gc,os
import datetime


import parseDicNew, parseTab, parseMetaInf, parseIFRS_FULL,parseBadFiles,skripts

from sqlalchemy import create_engine,text
from bs4 import  BeautifulSoup
version = 'final_6_2_bfo'

print('begin', datetime.datetime.now())
conn_string = f'postgresql+psycopg2://postgres:124kosm21@127.0.0.1/{version}_bfo'
db = create_engine(conn_string)
conn = db.connect()
print(conn)



with open(f'{os.getcwd()}\\{version}\\description.xml','r') as f:
    period=BeautifulSoup(f,'lxml')
period=f"{period.find('version').text[:4]}-{period.find('version').text[4:6]}-{period.find('version').text[6:8]}"


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

if os.path.exists(f'{os.getcwd()}\\{version}\\www.cbr.ru\\xbrl\\bfo\\dict'):
    ss = parseDicNew.c_parseDic(version, 'bfo\\dict', 'dictionary')
    df_list = ss.startParse()
    df_list = {k: v for k, v in df_list.items() if v is not None}
    print('bfo\\dict', df_list.keys())
    str_headers = ''
    for xx in df_list.keys():
        headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
        for hh in headers:
            str_headers = str_headers + hh + '\n'
        str_headers = str_headers.strip()[:-1]
        df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
    del df_list
    gc.collect()

ss = parseIFRS_FULL.c_parseIFRS_FULL(version)
df_list = ss.startParse()
df_list = {k: v for k, v in df_list.items() if v is not None}
print('ifrs-full', df_list.keys())
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list
gc.collect()

if os.path.exists(f'{os.getcwd()}\\{version}\\www.cbr.ru\\xbrl\\eps'):
    ss = parseDicNew.c_parseDic(version, 'eps', 'cbr-coa')
    df_list = ss.startParse()
    df_list = {k: v for k, v in df_list.items() if v is not None}
    print('eps', df_list.keys())
    str_headers = ''
    for xx in df_list.keys():
        headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
        for hh in headers:
            str_headers = str_headers + hh + '\n'
        str_headers = str_headers.strip()[:-1]
        df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
    del df_list
    gc.collect()

if os.path.exists(f'{os.getcwd()}\\{version}\\www.cbr.ru\\xbrl\\bfo\\rep'):
    print('parseTab', 'bfo')
    ss1 = parseTab.c_parseTab(version, 'bfo', 'bfo', period)
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



# conn.execute(text (sql_indexes))
conn.execute(text (sql_create_functions))
conn.execute(text (sql_create_elements_labels))
conn.execute(text (sql_create_preferred_labels))
conn.execute(text (sql_create_dop_tables))

conn.commit()
conn.close()
print('end', datetime.datetime.now())