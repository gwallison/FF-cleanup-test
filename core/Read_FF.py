# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:15:03 2019

@author: GAllison

This module is used to read all the raw data in from a FracFocus excel zip file

Input is simply the name of the archive file.  We expect that file to be
in the .\sources\ directory of the parent folder.

All variables are read into the 'final' df.  Re-typing and processing is
performed downstream in other modules.

"""
import zipfile
import pandas as pd
#import numpy as np


class Read_FF():
    
    def __init__(self,zname='currentData',dirname='./sources/',
                 keep_list=None):
        self.zname = dirname+zname+'.zip'
        self.keep_list = keep_list
        self._get_raw_cols_to_import()
        self.pickle_fn = './out/raw_df_pickle.pkl'
        
    def _get_raw_cols_to_import(self):
        """just samples one file to retrieve the column list"""
        with zipfile.ZipFile(self.zname) as z:
            infiles = []
            for fn in z.namelist():
                # the files in the FF archive with the Ingredient records
                #  always start with this prefix...
                if fn[:17]=='FracFocusRegistry':
                    infiles.append(fn)
            for fn in infiles[0:1]:
                with z.open(fn) as f:
                    t = pd.read_csv(f,low_memory=False,nrows=2,
                                    keep_default_na=False,na_values='')

        cols = list(t.columns)
        self.import_cols = []
        if self.keep_list == None:
            self.keep_list = cols
        for col in cols:
            if col in self.keep_list: self.import_cols.append(col)

            
    def import_raw(self,num_infiles='all',make_pickle=True):
        """
        `num_files: 'all' (default). Otherwise, use integer to include subset and
        reduce run time.
        
        Because we are interested in documenting the different states of 'missing'
        data, we assign NaN values to only the empty cells (''), and keep characters
        entered as is.  Later in the process, the NaN will be transformed to a 
        string ('_empty_entry_') for string non-numeric fields.
        """
        if self.keep_list==None:
            self.keep_list = self.import_cols
        dflist = []
        with zipfile.ZipFile(self.zname) as z:
            infiles = []
            for fn in z.namelist():
                # the files in the FF archive with the Ingredient records
                #  always start with this prefix...
                if fn[:17]=='FracFocusRegistry':
                    infiles.append(fn)
                    
            if num_infiles=='all': last = len(infiles)
            else: last = num_infiles
            for fn in infiles[:last]:
                with z.open(fn) as f:
                    print(f' -- processing {fn}')
                    if not self.keep_list: print('WARNING: no keep_list in Read_FF')
                    t = pd.read_csv(f,low_memory=False,
                                    usecols=self.import_cols,
                                    # ignore pandas default_na values
                                    keep_default_na=False,na_values='')
                    
                    # this variable is used to make it easier to find the
                    # original source of data.
                    t['raw_filename'] = fn
                    dflist.append(t)
        final = pd.concat(dflist,sort=True)
        final.reset_index(drop=True,inplace=True) #  single integer as index
        final['ingkey'] = final.index.astype(int) # create a basic integer index for easier reference
        if make_pickle:
            final.to_pickle(self.pickle_fn)
        return final
        
    def get_raw_pickle(self):
        print('Fetching raw_df from pickle')
        return  pd.read_pickle(self.pickle_fn)
