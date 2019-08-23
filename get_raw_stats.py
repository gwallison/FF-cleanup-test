# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison

This script collects and prints the descriptive stats on the raw FF data. It
assumes that the database has already been 'built' with build_database.py
"""
import core.Run_Full_Process as rfp
import core.FF_stats as ffs

c = rfp.Run_Full_Process()
df = c.get_full_pickle(all_cols=True)
print(df.columns,'\n\n\n\')
f = ffs.FF_stats(df)
print('*'*60)
print('*'*20,'  UNFILTERED DATA  ','*'*20)
print('*'*60)
f.show_all()

df = c.get_filtered_pickle(all_cols=True)
f = ffs.FF_stats(df)
print('*'*60)
print('*'*20,'  FILTERED DATA  ','*'*20)
print('*'*60)
f.show_all()
