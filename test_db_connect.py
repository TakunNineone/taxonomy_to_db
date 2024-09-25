from sqlalchemy import create_engine,text
import datetime

print('begin', datetime.datetime.now())
conn_string = f'postgresql+psycopg2://oksana:12345678@127.0.0.1/final_5_3'
db = create_engine(conn_string,isolation_level="AUTOCOMMIT")
conn = db.connect()
print(conn)