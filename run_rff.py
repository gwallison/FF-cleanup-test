# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison
"""
#import core.FF_logger as ffl
import gc,csv
import core.Read_FF as read_ff
import core.Run_Full_Process as rfp
import core.Add_bg_columns as abc
#import core.Categorize_records as cr
import core.FF_stats as ffstats
import core.Parse_raw as p_raw

c = rfp.Run_Full_Process()
#df = c.run_full(new_field_dic=False,use_pickle=False,pickle_out=True)
#test = abc.Add_bg_columns(None)
#test._add_cols()
df = c.get_full_pickle(all_cols=True) #,
pr = p_raw.Parse_raw()
dupes = pr._get_duplicate_events(df)


# =============================================================================
# rff = read_ff.Read_FF()
# df = rff.get_raw_pickle()
# rff = None
# print(df.info())
# ffs = ffstats.FF_stats(df)
# ffs.show_all()
# #t = c.new_field_dic()
# #t.info()
# 
# #df = None
# =============================================================================
gc.collect()