import re

import psycopg2, warnings, gc, os
import pandas as pd
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from xml.dom import minidom

warnings.filterwarnings("ignore")


class date_control():
    def __init__(self):
        self.connect = psycopg2.connect(user="postgres",
                                        password="124kosm21",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="final_6")


    def do_sql(self):
        sql="""
        select distinct full_path,array_agg(array[label_id,text,text_final]) text
        from
        (
        select version,rinok,entity,label_entity,label_id,lang,text,id,
        delete_space_and_tab(text) text_final,full_path
        from elements_labels
        where text is not null --and label_entity='npf-dic-label.xml'
        --and label_id='label_W_prekrashhRegDogV_repoz_perexoda_klienta_v_drug_repozMember_2'
        order by id
        ) l
        where text!=text_final
		group by full_path
        order by full_path
        """

        data_dict=[]
        df = pd.read_sql_query(sql, self.connect)
        for idx,row in df.iterrows():
            data_dict.append({'path':row['full_path'],'text':row['text']})

        for xx in data_dict:
            mydoc = minidom.parse(xx['path'])
            items = mydoc.getElementsByTagName('link:label')
            print(xx['path'])
            for yy in items:
                label_file=yy.getAttribute('xlink:label')
                to_find_file=yy.firstChild.nodeValue if yy.firstChild else ''
                for zz in xx['text']:
                    label_sql=zz[0]
                    to_find=zz[1]
                    to_replace=zz[2]
                    if label_file==label_sql :
                        print(label_file)
                        print(to_find)
                        print(to_find_file,'\n',to_replace)
                        yy.firstChild.nodeValue=to_replace
            mydoc.writexml(open(xx['path'], "w",encoding="utf-8"),indent="", addindent="", encoding="utf-8")




if __name__ == "__main__":
    ss=date_control()
    ss.do_sql()