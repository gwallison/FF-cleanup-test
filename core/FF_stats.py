# -*- coding: utf-8 -*-
"""
Created on Sat Jul  6 13:45:09 2019

@author: Gary Allison
"""
import core.Run_Full_Process as rfp
import pandas as pd

class FF_stats():
    
    def __init__(self,df):
        self.raw_df = df
        self.cols = list(self.raw_df.columns)
        
        self.na_lst = ['na','n/a','none','null','unk','nan']
        #self.df = self._get_filtered_raw()
        self.df = self.raw_df

        self._coerce_dtype()
        
    def _get_filtered_raw(self):
        self.flags = pd.read_csv('./out/raw_filtered_guide.csv')
        return pd.merge(self.flags,self.raw_df,on='ingkey',how='left')

    def _coerce_dtype(self):
        """ some columns need to be coerced into their native data type - that
        is, pandas assigns them to the wrong type when importing the raw.  """
        bools = ['FederalWell','IndianWell','IngredientMSDS']
        for b in bools:
            self.df[b] = self.df[b].astype('bool')
        
    def _show_bool_col_stat(self):
        print('{:>25}: {:>12} {:>12}'.format('Field Name','% not empty',
                                             '% True'))
        print('-------------------------------------------------------------------')
        cols = list(self.df.columns)
        tot = len(self.df)
        for col in cols:
            if self.df[col].dtype=='bool':
                perc_non_na = len(self.df[~self.df[col].isna()])/tot *100
                perc_true = str(round(self.df[col].sum()/tot * 100,2))
                print('{:>25}: {:>12} {:>12}'.format(col,
                                                    round(perc_non_na,2),
                                                    perc_true))

    def _show_numeric_col_stat(self):
        print('{:>25}: {:>12} {:>12}'.format('Field Name','% not empty',
                                             '% non zero'))
        print('-------------------------------------------------------------------')
        cols = list(self.df.columns)
        tot = len(self.df)
        for col in cols:
            if (self.df[col].dtype=='float64')|(self.df[col].dtype=='int64'):
                perc_non_na = len(self.df[~self.df[col].isna()])/tot *100
                if perc_non_na >0:
                    non_zero = round(len(self.df[self.df[col]!=0])/tot * 100,2)
                else:
                    non_zero = ' - '
                print('{:>25}: {:>12} {:>12}'.format(col,
                                                    round(perc_non_na,2),
                                                    non_zero))

    def _show_string_col_stat(self):
        print('{:>25}: {:>12} {:>12} {:>12}'.format('Field Name','% not empty',
                                             '% not "N/A"','num unique'))
        print('-------------------------------------------------------------------')
        cols = list(self.df.columns)
        tot = len(self.df)
        for col in cols:
            try:
                if (self.df[col].dtype=='object'):
                    perc_non_na = len(self.df[~self.df[col].isna()])/tot *100
                    if perc_non_na >0:
                        non_zero = round(len(self.df[~self.df[col].str.lower().str.strip().isin(self.na_lst)])/tot * 100,2)
                        num_uni = len(self.df[col].unique())
                    else:
                        non_zero = ' - '
                        num_uni = ' - '
                    print('{:>25}: {:>12} {:>12} {:>12}'.format(col,
                                                        round(perc_non_na,2),
                                                        non_zero,
                                                        num_uni))
            except:
                print('{:>25}: {:>12} {:>12} {:>12}'.format(col,
                                                        '---','---','---'))


    def show_all(self):
        print('\n        ################   Booleans   ####################\n')
        self._show_bool_col_stat()
        print('\n\n        ################   Numeric   ####################\n')
        self._show_numeric_col_stat()              
        print('\n\n        ################   String   ####################\n')
        self._show_string_col_stat()              
        