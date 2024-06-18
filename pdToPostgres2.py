import gc,os
import datetime,pandas as pd
import sys

# import parseDicNew, parseTab, parseMetaInf, parseIFRS_FULL,parseBadFiles,skripts

from sqlalchemy import create_engine,text
from bs4 import  BeautifulSoup
version = 'final_5_2_test'

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
nso=os.listdir(f'{os.getcwd()}\\{version}\\www.cbr.ru\\xbrl\\nso')
nso=[[xx,xx if xx!='operatory' else 'oper']  for xx in nso]

rinoks=[]
for rinok in nso:
    rinoks.append(rinok[1])
rinoks.append('ifrs-full')
rinoks.append('eps')
rinoks.append('bfo')
rinoks.append('dim')
rinoks.append('mem')

dict_to_df_rinoks= {'id':[xx for xx in range(len(rinoks))],'rinok':rinoks}
df_rinoks=pd.DataFrame(dict_to_df_rinoks)
None

