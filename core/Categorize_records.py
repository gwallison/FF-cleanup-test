# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 07:59:02 2019

@author: Gary Allison
"""
import pandas as pd
import numpy as np
import core.CAS_tools as ct
import pickle
#import core.Parse_raw as praw

class Categorize_records():
    """ This class sorts all data in the dataframe into one of the following
    categories:
        - valid data record (has valid CAS id)
        - proprietary data
        - non-data 
        - hidden data (has quantity but identity is masked in some way)
        
    This work proceeds in phases:
    Phase I: Find records which match CAS authority. 
    
        """
        
    def __init__(self,df,praw):
        """df is the master FF data set being analyzed.
        praw is the Parse_raw object that set up the field_cat sets
        """
        self.df = df
        self.praw = praw
        self.cas_field_cat = self.praw.get_field_cat('CASNumber')
        self.refdir = './CAS_ref/out/'
        self._get_ref_dic()
        # the cas_labels file has a list of all the CASNumbers with a
        # column that identifies the labels that signify the 'proprietary' status
        # Those identifiers were added manually through inspection of the CAS list.
        self.cas_labels_fn = './out/currentData/cas_labels.csv'

    def _get_ref_dic(self):
        # cas reference pickle created by 'CAS Reference dataset.ipynb'
        with open(self.refdir+'cas_ref_dic_pickle.pkl','rb') as f:
            self.cas_ref_dict = pickle.load(f)
        
        accum = 0
        for cas in self.cas_ref_dict.keys():
            accum += len(self.cas_ref_dict[cas])
        print(f'Number of unique CAS_RN in reference dictionary: {len(self.cas_ref_dict)}')
        print(f'Total number of synonyms in reference dictionary: {accum}')
        
        
###  Phase I - find valid records based on legimate CASNumber
        
    def _clean_CAS_for_comparison(self):
        #print('clean cas for comparison')
        self.cas_field_cat.rename({'original':'CASNumber'},inplace=True,axis=1)
        self.cas_field_cat['cas_clean'] = self.cas_field_cat.CASNumber.str.replace(r'[^0-9-]','')
        self.cas_field_cat['zero_corrected'] = self.cas_field_cat.cas_clean.map(lambda x: ct.correct_zeros(x) )
        
    def _mark_if_perfect_match(self):
        self.cas_ref_lst = list(self.cas_ref_dict.keys())
        self.cas_field_cat['perfect_match'] = self.cas_field_cat.zero_corrected.isin(self.cas_ref_lst)
        self.cas_field_cat['bgCAS'] = np.where(self.cas_field_cat.perfect_match,
                                               self.cas_field_cat.zero_corrected,
                                               'cas_unresolved')
        tmp = self.cas_field_cat[['CASNumber','perfect_match','bgCAS']].copy()
        self.df = pd.merge(self.df,tmp,on='CASNumber',how='left',validate='m:1')
        
    def phaseI(self):
        self._clean_CAS_for_comparison()
        self._mark_if_perfect_match()
        print(f'Number of perfect matches: {self.cas_field_cat.perfect_match.sum()}')
        print(f'Total records affected:    {self.df.perfect_match.sum()}\n')
        self.df.DQ_code = np.where(self.df.perfect_match,
                                   self.df.DQ_code.str[:]+'-P',
                                   self.df.DQ_code)
        
        
###  Phase II - Proprietary claims 
    def _add_proprietary_column(self):
        labels = pd.read_csv(self.cas_labels_fn,keep_default_na=False,na_values='')
        prop_lst = list(labels[labels.proprietary==1].clean.str.lower().str.strip().unique())
        self.cas_field_cat['proprietary'] = self.cas_field_cat.CASNumber.str.lower().str.strip().isin(prop_lst)
        tmp = self.cas_field_cat[['CASNumber','proprietary']].copy()
        self.df = pd.merge(self.df,tmp,on='CASNumber',how='left',validate='m:1')
        
    def _add_hiding_column(self):
        labels = pd.read_csv(self.cas_labels_fn,keep_default_na=False,na_values='')
        hiding_lst = list(labels[labels.hiding==1].clean.str.lower().str.strip().unique())
        self.cas_field_cat['un_cas_like'] = self.cas_field_cat.CASNumber.str.lower().str.strip().isin(hiding_lst)
        tmp = self.cas_field_cat[['CASNumber','un_cas_like']].copy()
        self.df = pd.merge(self.df,tmp,on='CASNumber',how='left',validate='m:1')

    def phaseII(self):
        """DQ_code for explicit proprietary is 3
                    for non_cas_like CAS Number but with quantity = 4
                    for non_cas_like CAS Number but absent quantity = 5"""
        self._add_proprietary_column()
        print(f'Total Proprietary records= {self.df.proprietary.sum()}')
        
        self.df.DQ_code = np.where(self.df.proprietary,
                                   self.df.DQ_code.str[:]+'-3',
                                   self.df.DQ_code)
        
        
        self._add_hiding_column()
        cond1 = self.df.PercentHFJob>0
        cond2 = self.df.MassIngredient>0
        has_quant = cond1 | cond2
        not_quant = ~has_quant
        cond3 = self.df.un_cas_like
        self.df.DQ_code = np.where(cond3&has_quant,
                                self.df.DQ_code.str[:]+'-4',
                                self.df.DQ_code)
        self.df.DQ_code = np.where(cond3&not_quant,
                                self.df.DQ_code.str[:]+'-5',
                                self.df.DQ_code)
        print(f'Total Non_caslike but quant = {len(self.df[self.df.DQ_code.str.contains("4",regex=False)])}')
#        print(f'Total Non_caslike but not quant = {len(t[t.DQ_code==5])}')
        
### Phase III - check for duplicates
    def _flag_duplicated_records(self):
        self.df['dup'] = self.df.duplicated(subset=['UploadKey','IngredientName',
                                       'CASNumber','MassIngredient','PercentHFJob'],
                                        keep=False)
        c0 = ~self.df.IngredientKey.isna()
        cP = self.df.DQ_code.str.contains('P',regex=False)
        dups = self.df[(self.df.dup)&(c0)&(cP)].copy()
        c1 = dups.Supplier.str.lower().isin(['listed above'])
        c2 = dups.Purpose.str.lower().str[:9]=='see trade'
        dups['redundant_rec'] = c1&c2
        #print(f' Expected redundant total: {dups.redundant_rec.sum()}, {c1.sum()}, {c2.sum()}')
        #print(f'dups len = {len(dups)}, inKey len = {len(dups.IngredientKey.unique())}')
        self.df = pd.merge(self.df,dups[['IngredientKey','redundant_rec']],
                           on='IngredientKey',how='left',validate='m:1')

    def phaseIII(self):
        """ > 75000 records are duplicated within events apparently due to the
        process of converting the pdf files to the bulk download.  These duplicates
        are identifiable by their 'Supplier' and 'Purpose' fields. Here we identify
        all duplicates (by 5 fields) then flag those that have the supplier/purpose
        characteristic -- DQ_code is R."""
        self._flag_duplicated_records()
        self.df.DQ_code = np.where(self.df.redundant_rec==True,
                                self.df.DQ_code.str[:]+'-R',
                                self.df.DQ_code)
        print(f'Total redundant records flagged: {len(self.df[self.df.DQ_code.str.contains("R",regex=False)])}')
        
    def do_all(self):
        self.phaseI()
        self.phaseII()
        self.phaseIII()
        return self.df