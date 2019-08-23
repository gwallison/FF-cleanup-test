# -*- coding: utf-8 -*-
"""
Created on Sun Jul 21 16:46:40 2019

@author: Gary
"""

import pandas as pd

class Add_bg_columns():
    """This class contains the methods to add the 'curated' fields into
    the data set.  These fields require significant manual supervision to 
    correct typos, consolidate categories etc.  Here, we just use the
    translation files (xlate.csv) to create the new columns
    """
    
    def __init__(self,df):
        self.df = df
        self.fields = ['Supplier'] #,'OperatorName','StateName']
        self.masterdir = './norm_col/'
        
    def _add_cols(self):
        for field in self.fields:
            print(f'Adding column {"bg"+field}')
            fn = self.masterdir+field+'/'+'xlate.csv'
            ref = pd.read_csv(fn,keep_default_na=False,na_values='',quotechar='$')
            self.df['original'] = self.df[field].str.strip().str.lower()
            ref.columns = ['bg'+field,'original']
            ref = ref[~ref.duplicated(subset='original',keep='last')]
            self.df = self.df.merge(ref,on='original',how='left',validate='m:1')
            self.df = self.df.drop('original',axis=1)
        return self.df
    
    def _add_supp_cols(self,col_source='sys_sup_guess',
                       xlate_source='Supplier',
                       final_name='bgSystemSupplier'):
        print(f'Adding column "{final_name}"')
        fn = self.masterdir+xlate_source+'/'+'xlate.csv'
        ref = pd.read_csv(fn,keep_default_na=False,na_values='',quotechar='$')
        self.df['original'] = self.df[col_source].str.strip().str.lower()
        ref.columns = [final_name,'original']
        ref = ref[~ref.duplicated(subset='original',keep='last')]
        self.df = self.df.merge(ref,on='original',how='left',validate='m:1')
        self.df = self.df.drop('original',axis=1)
        return self.df
    
    def add_all_cols(self):
        self._add_cols()
        self._add_supp_cols()
        return self.df