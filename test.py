import pandas as pd
from sqlalchemy import create_engine,text
version = 'final_a_6_1'
conn_string = f'postgresql+psycopg2://postgres:124kosm21@127.0.0.1/{version}'
db = create_engine(conn_string,isolation_level="AUTOCOMMIT")
conn = db.connect()


roles_table_definition_6=pd.read_csv("roles_table_definition_6.csv",header=0,delimiter=';')
roles_table_definition_6.to_sql('roles_table_definition', conn, if_exists='replace', index=False)