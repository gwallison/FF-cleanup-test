# -*- coding: utf-8 -*-
"""
Created on Wed Jul  3 16:52:41 2019

@author: Gary Allison
"""

import numpy as np
import pandas as pd

class Process_mass():
    """The methods of this class are used to calculate implied measures of
    mass of chemicals and other related tasks."""
    
    def __init__(self, df):
        self.df = df
        # now use DQ_code to find records that are workable
        print('Starting the Process_mass phase')
        self.df['ok'] = self.df.DQ_code.str.contains('P|3') # perfect match or proprietary
        self.df.ok = np.where(self.df.DQ_code.str.contains('R|1|2|4|5'),
                              False,self.df.ok)
        self.df['no_redund'] = ~self.df.DQ_code.str.contains('R')
        print(f'Num events: {len(self.df[self.df.ok].UploadKey.unique())}')
        print(f'Total records: {len(self.df[self.df.ok])}')
        
    def _total_percent_within_range(self):
        # remove these from calculations
        cnd1 = ~self.df.DQ_code.str.contains('R')
        gb = self.df[cnd1].groupby('UploadKey', as_index=False)['PercentHFJob'].sum()
        cnts = pd.cut(gb.PercentHFJob,[0,1,10,90,95,105,110,10000])
        print('Range of total % for "all" events')
        print(pd.value_counts(cnts))        
        cl = gb.PercentHFJob<=105
        ch = gb.PercentHFJob>=95
        #gb['within_tol'] = cl&ch
        ulist = list(gb[cl&ch].UploadKey.unique()) # all events within tolerance
        self.df.DQ_code = np.where(self.df.UploadKey.isin(ulist),
                                   self.df.DQ_code.str[:]+'-%',
                                   self.df.DQ_code)
        
        
    def _get_carrier_names(self):
        """To find the ingredient records that have the PercentHFJob for the water
        reported in the TotalBaseWaterVolume, we need to find the purpose label 'carrier'
        or any of its variations that point to the record in a given event.  Because there
        are a lot of variations of 'carrier' and 'base fluid', we use a regular expression
        search for all the purpose categories that match.  However, those events that
        cram multiple purposes into a single cell have to be controlled for.  Because
        these multi-component purpose categories are long strings, we limit the
        length of the strings that are allowed in this list."""
        
        cn = pd.DataFrame({'Purpose':list(self.df.Purpose.unique())})
        cn['clean'] = cn.Purpose.str.strip().str.lower()
        cn['is_base'] = cn.clean.str.contains('base',regex=False)
        cn['is_carrier'] = cn.clean.str.contains('carrier',regex=False) | cn.is_base
        cn['is_short'] = cn.clean.str.len()<50
        cn.is_carrier = cn.is_carrier&cn.is_short
        self.df = pd.merge(self.df,cn[['Purpose','is_carrier']],on='Purpose',how='left')
        
    def _get_total_event_mass(self):
        cnd1 = self.df.DQ_code.str.contains('%',regex=False)
        cnd2 = (self.df.PercentHFJob>50) & \
               ((self.df.bgCAS=='7732-18-5') \
                |(self.df.IngredientName.str.lower().str.contains('water',regex=False)) \
                |(self.df.IngredientName.str.lower().str.contains('h2o',regex=False)))
        cnd3 = self.df.is_carrier
        gb1 = self.df[self.df.ok & cnd1].groupby('UploadKey',as_index=False)['TotalBaseWaterVolume'].first()
        gb2 = self.df[self.df.no_redund & cnd1 & (cnd2 | cnd3)].groupby('UploadKey',as_index=False)['PercentHFJob'].sum()
        #print(gb1.columns, gb2.columns)
        mg = pd.merge(gb1,gb2,on='UploadKey',how='left')
        mg['carrier_mass'] = mg.TotalBaseWaterVolume * 8.3  # reporting in lbs
        mg['total_mass'] = mg.carrier_mass/(mg.PercentHFJob/100)
        return mg
    
    def _calc_all_record_masses(self,totaldf):
        """apply 'M' to all filtered records that have mass and 'A' to all filtered
        records that have pres/abs.
        """
        # first limit to within reasonable range of 'carrier_mass'
        totaldf['tot_wi_range'] = (totaldf.PercentHFJob>50) & (totaldf.PercentHFJob<=100)
        self.df = pd.merge(self.df,totaldf[['UploadKey','total_mass','tot_wi_range']],
                           on='UploadKey',how='left',validate='m:1')
        self.df['bgMass'] = np.where(self.df.tot_wi_range,
                                    (self.df.PercentHFJob/100)*self.df.total_mass,
                                    np.NaN)
        
        self.df.DQ_code = np.where((self.df.tot_wi_range)&(self.df.PercentHFJob>0),
                                   self.df.DQ_code.str[:]+'-M',
                                   self.df.DQ_code)
        self.df.DQ_code = np.where(self.df.tot_wi_range,
                           self.df.DQ_code.str[:]+'-A',
                           self.df.DQ_code)

    def run(self):
        self._total_percent_within_range()
        self._get_carrier_names()
        totaldf = self._get_total_event_mass()
        self._calc_all_record_masses(totaldf)
        return self.df
    
    
