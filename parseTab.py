import gc, datetime
import os,re

import pandas as pd
from bs4 import BeautifulSoup
import warnings
import parseToDf
warnings.filterwarnings("ignore")

class c_parseTab():

    def __init__(self,taxonomy,rinok_folder,rinok,period):
        if rinok_folder=='bfo':
            self.rinok = 'bfo'
            self.version=taxonomy
            self.period=period
            self.path_tax = f'{os.getcwd()}\\{taxonomy}\\'
            self.path_folder = f'{os.getcwd()}\{taxonomy}\\www.cbr.ru\\xbrl\\{rinok_folder}\\rep\\{period}\\'
            self.df = parseToDf.c_parseToDf(taxonomy,rinok)
        else:
            self.rinok=rinok
            self.version = taxonomy
            self.period = period
            self.path_tax=f'{os.getcwd()}\\{taxonomy}\\'
            self.path_folder = f'{os.getcwd()}\{taxonomy}\\www.cbr.ru\\xbrl\\nso\\{rinok_folder}\\rep\\{period}\\'
            self.df=parseToDf.c_parseToDf(taxonomy,rinok)

    def parsesupport(self):
        temp_list=[]
        columns = ['version', 'rinok', 'entity', 'targetnamespace', 'schemalocation', 'namespace']
        path_supp=self.path_folder+'\\ep\\'
        if os.path.isdir(path_supp):
            for xx in os.listdir(path_supp):
                if  'support' in xx:
                    support_file=xx
            path_file = path_supp + support_file
            with open(path_file,'rb') as f:
                ff=f.read()
            soup=BeautifulSoup(ff,'lxml').contents[2].find_next(re.compile('.*schema$'))
            self.df.parseLinkbase(soup, path_file)
            # print(path_file)
            self.df.parse_tableTags(soup,path_file,'ep_support')
            namesps=soup.find_all(re.compile('.*import$'))
            for xx in namesps:
                if 'www.cbr.ru' in xx['namespace'] and f'{self.period}/tab' in xx['namespace']:
                    temp_list.append([self.version, self.rinok, os.path.basename(path_file),
                                       soup['targetnamespace'], xx['schemalocation'], xx['namespace']])
            self.tables=pd.DataFrame(data=temp_list,columns=columns)
            self.df.df_tables_Dic.append(self.tables)
        else:
            self.tables=pd.DataFrame()

    def parsenosupport(self):
        temp_list=[]
        columns = ['version', 'rinok', 'entity', 'targetnamespace', 'schemalocation', 'namespace']
        path_supp = self.path_folder + '\\ep\\'

        if os.path.isdir(path_supp):
            for ep in os.listdir(path_supp):
                if 'support' not in ep:
                    path_file = path_supp + ep
                    with open(path_file, 'rb') as f:
                        ff = f.read()
                    soup = BeautifulSoup(ff, 'lxml').contents[1].find_next(re.compile('.*schema$'))
                    self.df.parseLinkbase(soup,path_file)
                    # print(path_file)
                    self.df.parse_tableTags(soup, path_file, 'ep')
                    namesps = soup.find_all(re.compile('.*import$'))
                    for xx in namesps:
                        if 'www.cbr.ru' in xx['namespace'] and f'{self.period}/tab' in xx['namespace']:
                            temp_list.append([self.version, self.rinok, os.path.basename(path_file),
                                                         soup['targetnamespace'], xx['schemalocation'], xx['namespace']])
            self.tables=pd.DataFrame(data=temp_list,columns=columns)
            self.df.df_tables_Dic.append(self.tables)
        else:
            None


    def parsetabThread(self):
        tabs=[[row['schemalocation'],row['namespace']] for index, row in self.tables.iterrows()]
        if tabs:
            self.parsetab(tabs)


    def parsetab(self,xx):
        start = datetime.datetime.now()
        for i,schemalocationnamespace in enumerate(xx):

            tab_temp=f"{schemalocationnamespace[1]}\\"
            schema_temp=schemalocationnamespace[0]
            schema_temp_2=schema_temp.split('/')[-1]
            path_xsd=self.path_tax+tab_temp.replace('http://','')+ schema_temp_2 #('' if '.xsd' in tab_temp else schema_temp_2)
            soup=self.df.parsetag(path_xsd,'schema')
            self.df.parse_tableTags(soup,path_xsd,'table')
            self.df.parseRoletypes(soup.find_all(re.compile('.*roletype$')),path_xsd)
            self.df.parseLinkbaserefs(soup,path_xsd)
            self.df.parseTableParts(soup,path_xsd)


            linkbaserefs = soup.find_all(re.compile('.*linkbaseref$'))
            formulas = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if
                    re.findall(r'formula\S*.xml', yy['xlink:href'])]
            if formulas:
                for path in formulas:
                    soup_formula = self.df.parsetag(path,'linkbase')
                    self.df.parseLinkbase(soup_formula,path)
                    self.df.parse_tableTags(soup_formula, path, 'formula')
                    self.df.parse_generals(soup_formula.find_all_next(re.compile('.*generalvariable$')),path)
                    self.df.parse_fgenerals(soup_formula.find_all_next(re.compile('.*general$')), path)
                    self.df.parseRolerefs(soup_formula.find_all(re.compile('.*roleref$')),path,'formula')
                    self.df.parseLink(soup_formula.find_all(re.compile('.*link$')), path)
                    self.df.parseArcs(soup_formula.find_all_next(re.compile('.*variablearc$')),path,'formula')
                    self.df.parseArcs(soup_formula.find_all_next(re.compile('.*variablefilterarc$')), path, 'formula')
                    self.df.parseArcs(soup_formula.find_all_next(re.compile('.*variablesetfilterarc$')), path, 'formula')
                    self.df.parseArcs(soup_formula.find_all_next(re.compile(r'^.{0,4}arc$')), path, 'formula')
                    self.df.parseLocators(soup_formula.find_all_next(re.compile(r'^.{0,5}loc$')),path,'formula')
                    self.df.parseLabels(soup_formula.find_all_next(re.compile('.*message$')),path)
                    self.df.parse_assertions(soup_formula.find_all_next(re.compile('.*valueassertion$')),path,'valueassertion'),
                    self.df.parse_assertions(soup_formula.find_all_next(re.compile('.*existenceassertion$')), path, 'existenceassertion')
                    self.df.parse_concepts(soup_formula,path)
                    self.df.parse_factvars(soup_formula,path)
                    self.df.parse_tdimensions(soup_formula,path)
                    self.df.parse_edimensions(soup_formula,path)
                    self.df.parse_aspectcovers(soup_formula.find_all_next(re.compile('.*aspectcover$')),path)
                    self.df.parse_assertionset(soup_formula,path)
                    self.df.parse_orFilters(soup_formula, path)
                    self.df.parse_andFilters(soup_formula, path)
                    self.df.parse_precond(soup_formula,path)
                    self.df.parse_messages(soup_formula,path)
                    self.df.parse_periodinstantfilter(soup_formula,path)
                    self.df.parse_mDimension(soup_formula,path)
            rend = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if
                    re.findall(r'rend\S*.xml',yy['xlink:href'])]
            [self.parserends(r) for r in rend if rend]
            pres = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if
                    re.findall(r'presentation\S*.xml',yy['xlink:href'])]
            [self.parsepres(p) for p in pres if pres]
            defin = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if
                     re.findall(r'definition\S*.xml',yy['xlink:href'])]
            [self.parsedef(d) for d in defin if defin]
            lab = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if
                   re.findall(r'lab\S*.xml',yy['xlink:href'])]
            [self.parselab(l) for l in lab if lab]

            another_file=[f"{tab_temp} -- {yy['xlink:href']}" for yy in linkbaserefs if  re.findall(r'definition\S*.xml',yy['xlink:href'])==[] and
            re.findall(r'lab\S*.xml',yy['xlink:href'])==[] and re.findall(r'presentation\S*.xml',yy['xlink:href'])==[] and re.findall(r'rend\S*.xml',yy['xlink:href'])==[] and re.findall(r'severities\S*.xml',yy['xlink:href'])==[]
                          and re.findall(r'formula\S*.xml',yy['xlink:href'])==[]]
            another_file=[xx for xx in another_file if xx]
            if another_file:
                print(another_file)
            gc.collect()
            finish = datetime.datetime.now()
            print("", end=f"\rPercentComplete: {round((i+1)/len(xx)*100,2)}%, time: {finish - start}")


    def parserends(self,rend):
        self.df.parseRulenodes(rend)
        self.df.parseRulesets(rend)
        self.df.parseAspectnodes(rend)
        self.parserend(rend)

    def parsedef(self,path):
        soup_def=self.df.parsetag(path,'linkbase')
        self.df.parseLinkbase(soup_def, path)
        self.df.parse_tableTags(soup_def, path, 'definition_table')
        self.df.parseRolerefs(soup_def.find_all(re.compile('.*roleref$')),path,'definition')
        self.df.parseLocators(soup_def.find_all(re.compile('.*loc$')),path,'definition')
        self.df.parseArcs(soup_def.find_all(re.compile('.*definitionarc$')),path,'definition')
        # t_all=[t1,t2,t3]
        # for t in t_all:
        #     self.df.writeThread(t)

    def parsepres(self,path):
        soup_pres=self.df.parsetag(path,'linkbase')
        self.df.parseLinkbase(soup_pres, path)
        self.df.parse_tableTags(soup_pres, path, 'presentation_table')
        self.df.parseRolerefs(soup_pres.find_all(re.compile('.*roleref$')),path,'presentation')
        self.df.parseLocators(soup_pres.find_all(re.compile('.*loc$')),path,'presentation')
        self.df.parseArcs(soup_pres.find_all(re.compile('.*presentationarc$')),path,'presentation')

    def parselab(self,path):
        soup = self.df.parsetag(path, 'linkbase')
        self.df.parseLinkbase(soup, path)
        self.df.parse_tableTags(soup, path, 'lab')
        self.df.parseRolerefs(soup.find_all(re.compile('.*roleref$')),path,'lab')
        self.df.parseLocators(soup.find_all(re.compile('.*loc$')),path,'lab')
        self.df.parseLabels(soup.find_all(re.compile('.*label$')),path)
        self.df.parseArcs(soup.find_all(re.compile('.*arc$')),path,'gen:arc')


    def parserend(self,path):
        soup = self.df.parsetag(path, 'linkbase')
        self.df.parseLinkbase(soup, path)
        self.df.parse_tableTags(soup, path, 'rend')
        self.df.parseRolerefs(soup.find_all(re.compile('.*roleref$')),path,'rend')
        self.df.parseTableschemas(soup.find_all(re.compile('.*table$')),path,'table')
        self.df.parseTableschemas(soup.find_all(re.compile('.*breakdown$')),path,'breakdown')
        self.df.parseArcs(soup.find_all(re.compile('.*tablebreakdownarc$')),path,'table:tablebreakdownarc')
        self.df.parseArcs(soup.find_all_next(re.compile('.*definitionnodesubtreearc$')),path,'table:definitionnodesubtreearc')
        self.df.parseArcs(soup.find_all_next(re.compile('.*aspectnodefilterarc$')), path,'table:aspectnodefilterarc')
        self.df.parseArcs(soup.find_all_next(re.compile('.*breakdowntreearc$')), path, 'table:breakdowntreearc')
        self.df.parse_edimensions_rend(soup,path)
        self.df.parseConceptRelationshipNode(soup,path)


    def startParse(self):
        self.parsesupport()
        self.parsetabThread()
        self.parsenosupport()
        gc.collect()
        return {
                'df_rulenodes':self.df.concatDfs(self.df.df_rulenodes_Dic),
                'df_aspectnodes': self.df.concatDfs(self.df.df_aspectnodes_Dic),
                'df_rend_conceptrelnodes': self.df.concatDfs(self.df.df_rend_conceptrelnodes_Dic),
                'df_rulenodes_c':self.df.concatDfs(self.df.df_rulenodes_c_Dic),
                'df_rulenodes_p':self.df.concatDfs(self.df.df_rulenodes_p_Dic),
                'df_rulenodes_e':self.df.concatDfs(self.df.df_rulenodes_e_Dic),
                'df_rulesets':self.df.concatDfs(self.df.df_rulesets_Dic),
                'df_rend_edmembers':self.df.concatDfs(self.df.df_rend_edmembers_Dic),
                'df_rend_edimensions':self.df.concatDfs(self.df.df_rend_edimensions_Dic),
                'df_roletypes':self.df.concatDfs(self.df.df_roletypes_Dic),
                'df_locators':self.df.concatDfs(self.df.df_locators_Dic),
                'df_arcs':self.df.concatDfs(self.df.df_arcs_Dic),
                'df_labels':self.df.concatDfs(self.df.df_labels_Dic),
                'df_rolerefs':self.df.concatDfs(self.df.df_rolerefs_Dic),
                'df_tableschemas':self.df.concatDfs(self.df.df_tableschemas_Dic),
                'df_linkbaserefs':self.df.concatDfs(self.df.df_linkbaserefs_Dic),
                'df_tables': self.df.concatDfs(self.df.df_tables_Dic),
                'df_tableparts': self.df.concatDfs(self.df.df_tableparts_Dic),
                'df_va_edmembers':self.df.concatDfs(self.df.df_va_edmembers_Dic),
                'df_va_edimensions':self.df.concatDfs(self.df.df_va_edimensions_Dic),
                'df_va_tdimensions':self.df.concatDfs(self.df.df_va_tdimensions_Dic),
                'df_va_concepts':self.df.concatDfs(self.df.df_va_concepts_Dic),
                'df_va_factvars':self.df.concatDfs(self.df.df_va_factvars_Dic),
                'df_va_assertions':self.df.concatDfs(self.df.df_va_assertions_Dic),
                'df_va_generals': self.df.concatDfs(self.df.df_va_generals_Dic),
                'df_va_fgenerals': self.df.concatDfs(self.df.df_va_fgenerals_Dic),
                'df_va_aspectcovers': self.df.concatDfs(self.df.df_va_aspectcovers_Dic),
                'df_va_assertionsets': self.df.concatDfs(self.df.df_va_assertionset_Dic),
                'df_va_orfilters': self.df.concatDfs(self.df.df_va_orfilters_Dic),
                'df_va_andfilters': self.df.concatDfs(self.df.df_va_andfilters_Dic),
                'df_va_mdimensions': self.df.concatDfs(self.df.df_va_mdimensions_Dic),
                'df_preconditions': self.df.concatDfs(self.df.df_preconditions_Dic),
                'df_messages': self.df.concatDfs(self.df.df_messages_Dic),
                'df_tabletags': self.df.concatDfs(self.df.df_tabletags_Dic),
                'df_linkbases': self.df.concatDfs(self.df.df_linkbases_Dic),
                'df_va_periods': self.df.concatDfs(self.df.df_periodinstantfilter_Dic),
                'df_va_link': self.df.concatDfs(self.df.df_va_link_Dic)
                }

if __name__ == "__main__":
    ss=c_parseTab('final_6_1_nso','npf','npf','2024-12-30')
    print(datetime.datetime.now())
    tables=ss.startParse()
    print('\n',datetime.datetime.now())
    None