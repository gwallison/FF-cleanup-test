# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison
"""
import pandas as pd
import core.Read_FF as rff
import core.Parse_raw as p_raw
import core.Categorize_records as cat_r
import core.Process_mass as proc_mass
import core.Add_bg_columns as abc
import os


class Run_Full_Process():
    """used to process a given archive to a set of tables 
    `zname` is the filename of the zip source file downloaded from FracFocus.org.
    `sourcedir` is the location of the source file."""
    
    def __init__(self,zname='currentData', sourcedir='./sources/'):
        self.zname = zname
        self.sourcedir = sourcedir
        self.outdir = './out/'+zname+'/'
        self.pickle_fn = self.outdir+'FF_full.pkl'
        try:
            os.mkdir(self.outdir) # if it doesn't exist yet...
        except:
            pass
        
    def run_full(self,pickle_out=True,use_pickle=False,new_field_dic=False):
        """ create new data set by importing a full set """
        p = p_raw.Parse_raw(outdir=self.outdir)
        if use_pickle:
            print('Using pickled raw data as input')
            c = self.get_full_pickle(col_list=p.get_keep_list())
        else:
            # if not using pickle, import all columns (don't use keeplist)
            c = rff.Read_FF(zname=self.zname).import_raw()
            c = p.cleanup(c)
            c = p.clean_events(c)
            if new_field_dic:
                p.make_field_dicts(c)
            cr = cat_r.Categorize_records(c,p)
            c = cr.do_all()
            
            add_bg = abc.Add_bg_columns(c)
            c = add_bg.add_all_cols()
            
        
            print(c.columns)
            pm = proc_mass.Process_mass(c)
            c = pm.run()
        
        if pickle_out: # save giant dataframe as a pickle; makes it easy to import later
            print(f'Pickling final. Total records: {len(c)}')
            c.to_pickle(self.pickle_fn)
        else:
            print('Not pickling.')
        return c
    
    def get_full_pickle(self,all_cols=True,
                        col_list=['UploadKey','CASNumber','IngredientName','Purpose','OperatorName',
                                   'Supplier','MassIngredient','PercentHFJob','DQ_code',
                                   'bgCAS','bgMass','JobStartDate','date','StateName','api10',
                                   'bgSupplier']):
        """retrieve the pickled data set, optionally with a subset of columns"""
        df = pd.read_pickle(self.pickle_fn)
        if all_cols: return df.copy()
        return df[col_list].copy()
    
    def get_filtered_pickle(self,keepcodes = 'M|3',removecodes= 'R|1|2|4|5',all_cols=True,
                        col_list=['UploadKey','CASNumber','IngredientName','Purpose','OperatorName',
                                   'Supplier','MassIngredient','PercentHFJob','DQ_code',
                                   'bgCAS','bgMass','JobStartDate','date','StateName','api10',
                                   'bgSupplier']):
        """retrieve the pickled data set, optionally with a subset of columns"""
        df = pd.read_pickle(self.pickle_fn)
        if all_cols: df = df.copy()
        else: df = df[col_list].copy()
        if keepcodes: df = df[df.DQ_code.str.contains(keepcodes)]
        if removecodes: df = df[~df.DQ_code.str.contains(removecodes)]
        return df.copy()

    def make_filtered_raw_guide(self):
        """ save a list of ingkey that is used to filter raw_df for FF_stats"""
        print('making raw_filtered_guide')
        df = pd.read_pickle(self.pickle_fn)
        newdf = df[~df.DQ_code.str.contains('1|2')][['ingkey']].copy()
        newdf.to_csv('./out/raw_filtered_guide.csv',index=False)        