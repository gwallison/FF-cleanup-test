# -*- coding: utf-8 -*-
"""
Created on Thu May 23 15:49:15 2019

@author: GAllison
"""

import norm_col.curator as cur
#import matplotlib.pyplot as plt
 
cur = cur.Curator(col_name='Supplier')
###cur._create_ref_name_from_xlate()
#cur.mop_up(thresh=0.5)
#cur.test_regex('h.{0,3}l')
cur.regex_match()