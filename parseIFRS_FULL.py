import os,gc
import parseToDf
import warnings
warnings.filterwarnings("ignore")
from multiprocessing.pool import ThreadPool

class c_parseIFRS_FULL():

    def __init__(self,taxonomy):

        self.rinok = 'ifrs-full'
        path_folder_new = f'{os.getcwd()}\{taxonomy}\\xbrl.ifrs.org\\taxonomy\\2021-03-24\\full_ifrs\\'
        path_dic_new = f'{path_folder_new}full_ifrs-cor_2021-03-24.xsd'

        path_folder_old = f'{os.getcwd()}\{taxonomy}\\xbrl.ifrs.org\\taxonomy\\2015-03-11\\full_ifrs\\'
        path_dic_old = f'{path_folder_old}full_ifrs-cor_2015-03-11.xsd'

        if os.path.exists(path_folder_new)==True:
            self.path_dic=path_dic_new

        elif os.path.exists(path_folder_old)==True:
            self.path_dic=path_dic_old
        else:
            self.path_dic = 'error'

        print(self.path_dic)

        if self.path_dic!= 'error':
            self.df = parseToDf.c_parseToDf(taxonomy, self.rinok)
        else:
            print('Не могу найти папку IFRS')

    def parseDic(self):
        soup_dic = self.df.parsetag(self.path_dic, 'xsd:schema')
        self.df.parse_tableTags(soup_dic, self.path_dic, 'dic')
        def t1(): self.df.parseElements(soup_dic.find_all_next('xsd:element'),self.path_dic)
        defs=[t1]
        with ThreadPool(processes=11) as pool:
            pool.map(self.df.writeThread, defs)

        del soup_dic
        gc.collect()

    def startParse(self):
        self.parseDic()
        return {'df_elements': self.df.concatDfs(self.df.df_elements_Dic)}

if __name__ == "__main__":
    ss=c_parseIFRS_FULL('final4302_cbr_test')
    dfs=ss.startParse()

    None
