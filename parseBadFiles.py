import os,stat

import pandas as pd

import parseToDf
import warnings
warnings.filterwarnings("ignore")
from multiprocessing.pool import ThreadPool

class c_parseBadFiles():
    def __init__(self,taxonomy):
        self.version=taxonomy

    def has_hidden_attribute(self,filepath):
        return bool(os.stat(filepath).st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN)

    def find_folders(self,period):
        temp=[]
        path_nso_=f'{os.getcwd()}\\{self.version}\\www.cbr.ru\\xbrl\\nso'
        if os.path.exists(path_nso_):
            path_nso=os.listdir(path_nso_)
            xsd_paths_nso=[[f'{os.getcwd()}\\{self.version}\\www.cbr.ru\\xbrl\\nso\\{xx}\\rep\\{period}\\tab\\',xx] for xx in path_nso]
            if path_nso:
                for xx in xsd_paths_nso:
                    for yy in os.listdir(xx[0]):
                        folder_path=f'{xx[0]}\\{yy}'
                        for zz in os.listdir(folder_path):
                            if '.xsd' in zz:
                                xsd=zz
                        for zz in os.listdir(folder_path):

                            is_hidden = self.has_hidden_attribute(f'{folder_path}\\{zz}')
                            temp.append([self.version,xx[1],xsd,zz.replace('operatory','oper'),f'{folder_path.replace(os.getcwd(),"")}\\{zz}',is_hidden])
                            # print(self.version, xx[1], xsd, zz, f'{folder_path.replace(os.getcwd(),"")}\\{zz}', is_hidden)

        xsd_paths_bfo = f'{os.getcwd()}\\{self.version}\\www.cbr.ru\\xbrl\\bfo\\rep\\{period}\\tab\\'
        if os.path.exists(xsd_paths_bfo):
            for yy in os.listdir(xsd_paths_bfo):
                folder_path=f'{xsd_paths_bfo}\\{yy}'
                for zz in os.listdir(folder_path):
                    if '.xsd' in zz:
                        xsd=zz
                for zz in os.listdir(folder_path):
                    # print(xsd,zz,f'{folder_path}\\{zz}')
                    is_hidden=self.has_hidden_attribute(f'{folder_path}\\{zz}')
                    temp.append([self.version,'bfo',xsd,zz,f'{folder_path.replace(os.getcwd(),"")}\\{zz}',is_hidden])

        columns=['version','rinok','entity','file','file_path','is_hidden']
        xsdFilesDF=pd.DataFrame(data=temp,columns=columns)
        None
        return {'df_xsdfiles': xsdFilesDF}

if __name__ == "__main__":
    ss=c_parseBadFiles('final_6_5')
    ss.find_folders('2024-11-01')
    None

