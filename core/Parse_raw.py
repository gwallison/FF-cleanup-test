# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 13:32:22 2019

@author: GAllison

Parse_raw is used to take the raw dataframe, clean up empty cells, create
or update dictionaries of field labels and to separate it into tables, etc

To facilitate an efficient work flow, we keep 'dictionaries' of all the different
categories of selected fields (such as Supplier, Purpose, etc.)  This allows us
to process the much shorter list of items in a dictionary and then later merge that
information with the larger database.  For example, there are only about 3000 unique
CASNumber entries for us to process, but there are more than 4,000,000 data records
in the main dataframe. Working with the dictionary speeds up the process by >1000x.
"""

import pandas as pd
import numpy as np
import csv, pickle


class Parse_raw():
    def __init__(self,outdir = './out/currentData/',
                 ev_list = ['UploadKey','JobEndDate','JobStartDate',
                            'OperatorName', 'DisclosureKey',
                            'TotalBaseWaterVolume','FFVersion'],
                 well_list = ['APINumber','StateNumber', 'StateName','WellName',
                              'CountyNumber','Latitude','Longitude','TVD'],
                 ing_list = ['UploadKey','TradeName','Supplier','Purpose',
                             'IngredientName','CASNumber','PercentHFJob',
                             'MassIngredient','ingkey'],
                 misc_list = ['IngredientKey'], # needed for book-keeping while processing
                 ):
        self.outdir = outdir
        self.ev_list = ev_list
        self.well_list = well_list
        self.ing_list = ing_list
        self.misc_list = misc_list
        self.keep_list = list(set(self.ev_list + self.well_list + self.ing_list + self.misc_list))
        self.blank_in = ['',None,np.NaN]
        self.blank_label = '_empty_entry_'
        self.blank_list = ['CASNumber','IngredientName','OperatorName',
                           'Supplier','Purpose','TradeName','StateName']
        self.field_dic_name = self.outdir+'field_dic_pickle.pkl'

        
    def _adjust_API(self,row):
        """ state numbers < 10 are sometimes a problem; The first two digits
        of the API number should be the state number but they are sometimes
        wrong because the leading zero is dropped. Colorado must start '05'"""
        s = str(row.APINumber)
        #print(s,row.StateNumber,row.CountyNumber)
        if int(s[:2])==row.StateNumber:
            if int(s[2:5])==row.CountyNumber:
                return s[:10]  # this is the normal condition, no adjustment needed
        #print('Adjust_API problem...')
        s = '0'+s
        if int(s[:2])==row.StateNumber:
            if int(s[2:5])==row.CountyNumber:
                #self.fflog.add_to_well_log(s[:10],1)
                return s[:10]  # This should be the match; 
        else:
            # didn't work!
            s = str(row.APINumber)
            #self.fflog.add_to_well_log(s[:10],2)
            print(f'API10 adjustment failed: {row.APINumber}')
            return s[:10]
 
    
    def _normalize_empty_cells(self,df):
        print('Normalizing empty cells')
        for col in self.blank_list:
            if col in df.columns:
                df[col] = np.where(df[col].isin(self.blank_in),
                                  self.blank_label,df[col])
        return df
    
    def _get_csv_df(self,fn):
        """ get pd csv with heavy quoting """
        return pd.read_csv(fn,quotechar='$', quoting=csv.QUOTE_ALL,
                              keep_default_na=False)
    
    def _put_csv_df(self,df,fn):
        """ save pd csv with heavy quoting """
        df.to_csv(fn,quotechar='$', quoting=csv.QUOTE_ALL)
    
    def _update_field_categories(self,df_in):
        col_names = ['CASNumber']
        for col in col_names:
            fn = self.outdir+col+'_field_cat.csv'
            try:
                fc = self._get_csv_df(fn)
            except:
                fc = pd.DataFrame()
            print(f'Updating field categories for {col}, \n -- initial len: {len(fc)}')
            curr_lst = list(fc.original.unique())
            new_lst = list(df_in[col].unique())
            update_lst = []
            for item in new_lst:
                if not item in curr_lst: # need to add it
                    clean = item.strip().lower()
                    update_lst.append({'original':item,'clean':clean,
                                       'bg':'not_classified'})
            if len(update_lst)>0:
                fc = fc.append(update_lst,sort=True,verify_integrity=True)
            print(f' --   final len: {len(fc)}')
            self._put_csv_df(fc,fn)

    def get_field_cat(self,col_name='Supplier'):
        """ fetch the field_cat df for the col_name field."""
        fn = self.outdir+col_name+'_field_cat.csv'
        return self._get_csv_df(fn)        

    def _createAPI10(self,raw_df):
        print('Creating api10')
        api_df = raw_df.groupby('UploadKey',as_index=False)['APINumber',
                                            'StateNumber','CountyNumber'].first()
        api_df['api10'] = api_df.apply(lambda x: self._adjust_API(x),axis=1)
        api_df = api_df[['UploadKey','api10']]
        raw_df = pd.merge(raw_df,api_df,on='UploadKey',validate='m:1')
        return raw_df

    def _make_date_field(self,raw_df):
        print('Converting date')
        # drop the time portion of the datatime
        raw_df['d1'] = raw_df.JobEndDate.str.split().str[0]
        # fix some obvious typos that cause system shutdown
        raw_df['d2'] = raw_df.d1.str.replace('3012','2012')
        # instead of translating ALL records, just do uniques records ...
        tab = pd.DataFrame({'d2':list(raw_df.d2.unique())})
        tab['date'] = pd.to_datetime(tab.d2)
        # ... then merge back to whole data set
        raw_df = pd.merge(raw_df,tab,on='d2',how='left',validate='m:1')
        return raw_df.drop(['d1','d2'],axis=1)
    
# =============================================================================
#     def _get_system_supplier(self,ev_df):
#         return ev_df[ev_df.CASNumber=='Listed Below'].Supplier.value_counts().idxmax()
# =============================================================================
    
    def _flag_system_approach(self,df):
        """ find the records that have been entered using the System Approach"""
        print('Detecting "system approach"')
        ulk = list(df[df.CASNumber=='Listed Below'].UploadKey.unique())
        df['bgSystemApproach'] = df.UploadKey.isin(ulk)
        gb = df[df.CASNumber=='Listed Below'].groupby('UploadKey',as_index=False)['Supplier'].agg(lambda x: x.value_counts().index[0])
        #gb = df[df.CASNumber=='Listed Below'].groupby('UploadKey',as_index=False)['Supplier'].agg(pd.Series.mode)
        gb.columns = ['UploadKey','sys_sup_guess']
        #print(gb.head())
# =============================================================================
#         # Now get the System Supplier
#         t = []
#         for key in ulk:
#             t.append(df[df.UploadKey==key].Supplier.value_counts().idxmax())
#         tt = pd.DataFrame({'UploadKey':ulk,'bgSystemSupplier':t})
# =============================================================================
        df = pd.merge(df,gb,on='UploadKey',how='left',validate='m:1')
        df.sys_sup_guess = np.where(df.sys_sup_guess.isna(),'_not_system_',
                                       df.sys_sup_guess)
        return df
    
    def cleanup(self,raw_df):
        raw_df = self._normalize_empty_cells(raw_df)
        raw_df = self._createAPI10(raw_df)
        raw_df = self._make_date_field(raw_df)
        raw_df = self._flag_system_approach(raw_df)
        self._update_field_categories(raw_df)
        return raw_df

    
    def get_keep_list(self):
        return self.keep_list
    
    def _flag_empty_events(self,raw_df):
        """The earliest FracFocus data on the pdf website is not included in 
        the bulk download.  There are placeholder records in the data set that include
        'header' data such as location, operator name, and even the amount of water
        used.  However, there are no records about the chemical ingredients in 
        the frack.  (Through incredible effort, these early data were scraped from
        the pdfs by the organization, SkyTruth, and are currently available online.)
        The vast majority of these events are FFVersion 1.
        Because no chemical disclosure is given for these events, we flag these data
        for removal 
        from the data set we use for analysis. Keeping them in the data set would
        distort any estimates of 'presence/absence' of materials."""
        raw_df['DQ_code'] = '0' # initialize the 'disqualifying' code field.
        raw_df.DQ_code = np.where(raw_df.IngredientKey.isna(),
                                  raw_df.DQ_code.str[:]+'-1',
                                      raw_df.DQ_code)
        return raw_df
    
    def _flag_duplicate_events(self,raw_df):
        """The FracFocus data set contains multiple versions of some fracking events.
        Here we first find the duplicates (using the API number and the fracking date).
        NOTE:  For this version of the FF database, we mark ALL duplicates for removal.
        While it may be possible to salvage some of the duplicates, there are no direct 
        indicators of the most 'correct' version and, according to Mark Layne,
        sometimes duplicates are not even replacements, but rather additions to 
        previous entries.  Further, the FFVersions are not in 'ingkey' order (it
        appears that FFV2 are the last in the whole set) so it is complicated to 
        find the most correct way to salvage the duplicates.  Because we are
        not keeping FFV1's anyway, this decision results in the loss of about 2%
        of all events.
        """
        t = raw_df[raw_df.DQ_code=='0'][['UploadKey','date','api10','ingkey']].copy()
        t = t.groupby(['UploadKey','date','api10'],as_index=False)['ingkey','UploadKey'].first()
        t = t.sort_values(by='ingkey')
        t['dupes'] = t.duplicated(subset=['api10','date'],keep=False)
        # if keeping the highest ingkey event, use the following line instead
#        t['dupes'] = t.duplicated(subset=['api10','date'],keep='last')

        dupes = list(t[t.dupes].UploadKey.unique())
        raw_df.DQ_code = np.where(raw_df.UploadKey.isin(dupes),
                                  raw_df.DQ_code.str[:]+'-2',
                                  raw_df.DQ_code)
        return raw_df
    
# =============================================================================
#     def _get_duplicate_events(self,raw_df, ignore_FF1=True):
#         """We use this method to fetch a set of all the events that are duplicates 
#         (by api and date), so we may test how they are different.
#         """
#         
#         t = raw_df[['UploadKey','date','api10','ingkey']].copy()
#         t = t.groupby(['UploadKey','date','api10'],as_index=False)['ingkey','UploadKey'].first()
#         t['dupes'] = t.duplicated(subset=['api10','date'],keep=False)
#         #print(f'Number of dupes = {t.dupes.sum()}')
#         dupes = list(t[t.dupes].UploadKey.unique())
#         dupe_df = raw_df[raw_df.UploadKey.isin(dupes)].copy()
#         gb = dupe_df.groupby(['UploadKey','api10','date'],as_index=False)['bgMass'].count()
#         gb2 = gb.groupby(['api10','date'],as_index=False)['UploadKey'].count()
#         gb2['dup_cnt'] = gb2.index
#         return pd.merge(dupe_df,gb2[['api10','date','dup_cnt']],on=['api10','date'],
#                            how='left')
# 
# =============================================================================

    def clean_events(self,raw_df):
        raw_df = self._flag_empty_events(raw_df)
        raw_df = self._flag_duplicate_events(raw_df)
        empty_ev = list(raw_df[raw_df.DQ_code.str.contains('1',regex=False)].UploadKey.unique())
        dup_ev = list(raw_df[raw_df.DQ_code.str.contains('2',regex=False)].UploadKey.unique())
        print(f'Removed events: empty: {len(empty_ev)}, duplicate: {len(dup_ev)}')
        return raw_df

    def make_field_dict(self,raw_df):
        # used as precursor to find unique categories in important fields
        cols = ['Supplier','CASNumber','IngredientName','StateName',
                'Purpose','UploadKey','OperatorName','TradeName']
#        multi = {'casig':['CASNumber','IngredientName']}
                 #'trade_casig':['TradeName','CASNumber','IngredientName']}
        df = raw_df[cols].copy()
        raw_df = None

        # replace blank_in with blank_label  !!! is this necessary??
        for col in cols:
            df[col] = np.where(df[col].isin(self.blank_in),self.blank_label,df[col])
        dic = {}
        for col in cols:
            t = pd.DataFrame(df[[col]])#,columns=['original'])
            if col in ['UploadKey']: # don't 'clean' these
                t['clean'] = t[col]#t.original
            else:
                t['clean'] = t[col].astype('str').str.strip().str.lower()
                
            # after cleanup, some may become 'empty' (were just extraneous char)
            t.clean = np.where(t.clean.isin(self.blank_in),self.blank_label,t.clean)

            gb = t.groupby('clean',as_index=False)[col].count()
            gb.columns = ['clean','cnt']
            dic[col] = gb
            print(f'Number of unique {col}: {len(dic[col])}')
        #t = dic['IngredientName']
# =============================================================================
#         for m in multi.keys():
#             t = pd.DataFrame(df[multi[m]])
#             for col in multi[m]:
#                 t[col] = t[col].astype('str').str.strip().str.lower() 
#                 # after cleanup, some may become 'empty' (were just extraneous char)
#                 t[col] = np.where(t[col].isin(self.blank_in),self.blank_label,t[col])
#             t['cnt'] = t[multi[m][0]] # this is a dummy var to aid in counts
#             gb = t.groupby(multi[m],as_index=False)['cnt'].count()
#             dic[m] = gb
#             print(f'Number of unique {m}: {len(dic[m])}')
# 
# =============================================================================
        with open(self.field_dic_name,'wb') as f:
            pickle.dump(dic,f)

    def get_field_dict(self):
        """load the field_dic from its pickle"""
        with open(self.field_dic_name,'rb') as f:
            return pickle.load(f)
    