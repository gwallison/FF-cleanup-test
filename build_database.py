# -*- coding: utf-8 -*-
"""


Created on Wed Apr 17 10:34:50 2019

@author: GAllison

This script performs the overall task of creating a FracFocus database from
the raw excel collection.  This script calls multiple functions through
the class core.Run_Full_Process
"""
import core.Run_Full_Process as rfp

c = rfp.Run_Full_Process()

df = c.run_full(new_field_dic=False,use_pickle=False,pickle_out=True)

#df = c.run_full(new_field_dic=False,use_pickle=True,pickle_out=False)
#import core.Parse_raw as praw
#pr = praw.Parse_raw()

#df = pr._flag_system_approach(df)