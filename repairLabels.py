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
                                        database="final_6_1_27062024")


    def do_sql(self):
        sql="""
select full_path,array_agg(array[label_id,text,mod_label_text]) text
from
(
select distinct qname,full_path,label_id,text,mod_label_text
from
(
select qname,full_path,label_id,text,text_mod1,
case when text_mod1 not like 'dim%' and text_mod1 not like 'mem%' then 
upper(substring(text_mod1 from 1 for 1)) || substring(text_mod1 from 2 for length(text_mod1))  else text_mod1 end
mod_label_text
from
(
select qname,full_path,label_id,text,delete_space_and_tab(text) text_mod1
from elements_labels 
where rinok in ('dim','mem')
	
	union all
	
select qname,full_path,label_id,text,delete_space_and_tab(text) text_mod1 
from preferred_labels 
where full_path like '%dim-int-label.xml' or full_path like '%mem-int-label.xml'
) mm
) mm 
where mod_label_text!=text
) mm
group by full_path
        """
#         sql="""
#
# with ll as
# (select version,rinok,parentrole,qname,
# coalesce(pl.full_path,dl.full_path) full_path,
# coalesce(pl.label_id,dl.label_id) label_id,
# coalesce(pl.text,dl.text) label_text
# from
# (
# select l.version,l.rinok,l.entity,l.parentrole,qname,full_path,label_id,text
# from
# (select * from locators where locfrom = 'definition' and rinok='bfo' order by href_id) l
# join (select * from elements_labels es
# where role='http://www.xbrl.org/2003/role/label' and lang='ru' order by id) es on es.id=l.href_id
# 	and (qname like 'ifrs-full:%' or qname like 'ifrs-ru:%')
# ) dl
# left join
# (
# select l.version,l.rinok,l.entity,l.parentrole,qname,full_path,label_id,text
# from
# (select * from locators where locfrom = 'presentation' order by href_id) l
# join (select * from preferred_labels order by id) pl on pl.id=l.href_id and pl.parentrole=l.parentrole
# ) pl using (version,rinok,parentrole,qname)
# order by version,rinok,parentrole,qname
# ),
# ll_fix as
# (
# select version,rinok,parentrole,qname,label_text,case when label_text!=mod_label_text then mod_label_text else '' end mod_label_text,where_latin
# from
# (
# select version,rinok,parentrole,qname,label_text,
# 		upper(substring(label_text from 1 for 1)) || substring(label_text from 2 for length(label_text)) mod_label_text,
# 		get_latin_element_text(replace(replace(label_text,'IAS',''),'IFRS','')) where_latin
# from
# (
# select distinct version,rinok,parentrole,qname,label_text from ll
# ) ll
# ) ll
# where label_text!=mod_label_text --or where_latin is not Null
# 	order by version,rinok,parentrole,qname
# )
#
#
# select full_path,array_agg(array[label_id,label_text,mod_label_text]) text
# from
# (
# select distinct string_agg(distinct ll.rinok,';') rinok,qname,full_path,label_id,
# 		ll_fix.label_text,delete_space_and_tab(ll_fix.mod_label_text) mod_label_text,
# 		array_length(array_agg(distinct parentrole),1) len, string_agg(distinct parentrole,';') roles
# from (select * from ll order by qname) ll
# join ll_fix using (version,rinok,parentrole,qname)
# group by qname,ll_fix.label_text,where_latin,ll_fix.mod_label_text,full_path,label_id
# ) ll group by full_path
#
#
# """
        # sql=r"""
        # select 'C:\MyPyProjects\taxonomy_to_db\final_6_1_labels\www.cbr.ru\xbrl\bfo\dict\dictionary-label.xml' full_path,array[array['label_АктивыАктуарныеПрибылиИзмененияФинансовыхПредположений_2','актуарные прибыли - изменения финансовых предположений','Актуарные прибыли - изменения финансовых предположений']] text
        # """
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
            mydoc.writexml(open(xx['path'], "w",encoding="utf-8"),indent="", addindent="", encoding="utf-8",)




if __name__ == "__main__":
    ss=date_control()
    ss.do_sql()