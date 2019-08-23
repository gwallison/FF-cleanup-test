# -*- coding: utf-8 -*-
"""
Created on June 18 2019

@author: GAllison

These routines are used to facilitate manual review of the 
CAS/ingredient names in the FracFocus dataset

This review process happens in phases:
    Phase I - As the field_dictionary is searched, any unique CASIG combinations 
    (that is, specific combinations of CASNumber and IngredientName) that are
    not in the casig_xlate, are added as 'not_classified' (flag = 0)
    
    Phase II - Examine all not_classified to determine if the cleaned CASnumber
    is an exact match to an entry in the cas_ref dictionary. If so, save 
    that ref_cas as the bgCAS and flag=1.  Note that at this point, the bgCAS is
    considered (by FF standards) to be sufficient to identify the material.  For
    us, however, it is only the very basest identification because we know there
    are lots of errors that need additional processing.
    
    Phase III - Examine remaining not_classified to find CAS Numbers that are
    CLOSE to an entry in the reference dictionary AND the associated Ingredient
    Name is an exact match with one of the 'authorized' synonyms of the match.
    This phase finds those small typos in the CASNumber that are, nonetheless, 
    clearly matched with an known material.  This phase requires manual review, 
    though it does not require much chemical expertise.

This version uses the matches from an LSH procedure 
(currently LSH_test_2.ipynb)
"""

import pandas as pd
import numpy as np
nan = np.nan
import pickle, csv
import core.CAS_tools as ct
import core.Lsh_tools as lsh

outdir = './norm_col/casig/'
refdir = './CAS_ref/out/'
field_dic_fn = './out/currentData/_field_dic.pkl'
match_fn = './out/casig_matches_2019_06_10.pkl'
out_fn = outdir+'xlate_casig.csv'
potentials_fn = './out/casig_potentials.pkl'
proprietary_fn = './out/CAS_labels_for_proprietary.csv'


class Casig_curator():
    
    """ object to perform the various processing tasks on CAS/Ingredient pairs"""
    def __init__(self,target_cas='50-00-0'):
        self._get_field_dic() # creates self.casig_orig
        self.cas = target_cas
        self.match_df = pd.read_pickle(match_fn)
        self.res_dic = self._get_results_dic() # creates self.res_dic from xlate file
        self._get_ref_dic() #
        self._get_review_status()
        self._get_potentials(self.cas)
        self._get_proprietary_list()

    def _get_field_dic(self):
        """takes the precompiled field_dic and creates casig_orig df from it."""
        with open(field_dic_fn,'rb') as f:
            field_dic = pickle.load(f)
        self.casig_orig = field_dic['casig']
        self.casig_orig['cas_clean'] = self.casig_orig.CASNumber.str.replace(r'[^0-9-]','')
        self.casig_orig['zero_corrected'] = self.casig_orig.cas_clean.map(lambda x: ct.correct_zeros(x) )
        
        # now the Ingredients
        self.casig_orig['ig_clean'] = self.casig_orig.IngredientName.str.strip().str.lower()
        self.casig_orig.drop_duplicates(inplace=True)
        
        self.casig_orig.to_csv(outdir+'temp.csv',
                               quotechar='$', quoting=csv.QUOTE_ALL)

    def no_na(self,df):
        for col in df.columns:
            if len(df[df[col].isna()>0]):
                print(f'unexpected NaN in {col}')
                return False
        return True

    def _get_results_dic(self):
        ''' creates a dict object from {out_fn}. The keys for the dic object
        are tuples: (CASNumber,ig_clean)'''
        try: df = pd.read_csv(out_fn,quotechar='$', quoting=csv.QUOTE_ALL,
                              keep_default_na=False)
        except: df = pd.DataFrame()
        assert self.no_na(df)
        rdic = {}
        for row in df.itertuples():
            rdic[(row.CASNumber,row.ig_clean)] = (row.bgCAS,row.flag)
        return rdic

    def _add_to_results_dic(self,CASN,ig,bgCAS,flag):
        " add single entry to res.dic"
        self.res_dic[(CASN,ig)] = (bgCAS,flag)
        self.reviewed[(CASN,ig)] = True

    def _save_results_dic(self,ask_about_save=True):
        "transfer self.res_dic to the xlate file."
        CASNumber = []
        ig_clean = []
        bgCAS = []
        flag = []
        for tup in self.res_dic.keys():
            CASNumber.append(tup[0])
            ig_clean.append(tup[1])
            bgCAS.append(self.res_dic[tup][0])
            flag.append(self.res_dic[tup][1])
        df = pd.DataFrame({'CASNumber':CASNumber,
                           'ig_clean':ig_clean,
                           'bgCAS':bgCAS,
                           'flag':flag})        
        assert self.no_na(df)
        if ask_about_save:
            keyp = input(prompt='Replace results file? [y/N] > ')
            if keyp=='y': df.to_csv(out_fn,index=False,
                                    quotechar='$', quoting=csv.QUOTE_ALL)
        else:
            df.to_csv(out_fn,index=False,
                      quotechar='$', quoting=csv.QUOTE_ALL)
            
    def _add_new_to_res_dic(self):
        """add new casig pairs to res_dic as unclassified"""
        for CAS,ig in zip(list(self.casig_orig.CASNumber),
                   list(self.casig_orig.ig_clean)):
            if (CAS,ig) not in self.res_dic: 
                self._add_to_results_dic(CAS,ig,'not_classified',0)
    
    def apply_phase_1(self):
        """add new casig pairs to res_dic as unclassified"""
        self._add_new_to_res_dic()
        self._save_results_dic(ask_about_save=False)
        
    def apply_phase_2(self):
        clean = {}
        for CAS,ig,zc in zip(list(self.casig_orig.CASNumber),
                             list(self.casig_orig.ig_clean),
                             list(self.casig_orig.zero_corrected)):
            clean[(CAS,ig)] = zc
        for tup in self.res_dic.keys():
            if self.res_dic[tup][1]==0:  # if not-classified
                if clean[tup] in self.cas_ref_dict.keys(): # keep as original
                    self.res_dic[tup] = (clean[tup],1)
        self._save_results_dic()
                
    
    def _get_review_status(self):
        self.reviewed = {}
        for CAS,ig in zip(list(self.casig_orig.CASNumber),
                   list(self.casig_orig.ig_clean)):
            if (CAS,ig) in self.res_dic: self.reviewed[(CAS,ig)] = True
            else: self.reviewed[(CAS,ig)] = False
            
        
    def _get_ref_dic(self):
        # cas reference pickle created by 'CAS Reference dataset.ipynb'
        with open(refdir+'cas_ref_dic_pickle.pkl','rb') as f:
            self.cas_ref_dict = pickle.load(f)
        
        accum = 0
        for cas in self.cas_ref_dict.keys():
            accum += len(self.cas_ref_dict[cas])
        print(f'Number of unique CAS_RN in reference dictionary: {len(self.cas_ref_dict)}')
        print(f'Total number of synonyms in reference dictionary: {accum}')
        
    def _pkl_lsh(self,obj,name):
        with open('./out/'+name+'.pkl', 'wb') as f:
            pickle.dump(obj,f)
    def _unpkl_lsh(self,name):
        with open('./out/'+name+'.pkl', 'rb') as f:
            return pickle.load(f)

    def make_ref_lsh_sets(self):
        ig_ref_lst = []
        for cas in self.cas_ref_dict.keys():
            for ig in self.cas_ref_dict[cas]:
                ig_ref_lst.append(ig)
        print('  -- Reference CAS set')
        self._pkl_lsh(lsh.LSH_set(rawlist=list(self.cas_ref_dict.keys()),
                      thresholds=[1.0,0.9,0.6]),
                      'cas_ref_set')
        print('  -- Reference Ingredient names')
        self._pkl_lsh(lsh.LSH_set(rawlist=ig_ref_lst,
                                  thresholds=[1.0,0.8,0.4]),
                      'ig_ref_set')
            
    def make_orig_lsh_sets(self):

        print('Creating LSH sets:\n  -- Original CASNumber')
        self._pkl_lsh(lsh.LSH(rawlist=list(self.casig_orig.zero_corrected.unique())),
                      'cas_orig_lsh')
        print('  -- Original IngredientName')
        self._pkl_lsh(lsh.LSH(rawlist=list(self.casig_orig.ig_clean.unique())),
                      'ig_orig_lsh')


    def _get_potentials(self,cas='7732-18-5'):
        with open(potentials_fn,'rb') as f:
            pots = pickle.load(f)
        self.potentials = list(pots[cas])
        
        
    def _get_proprietary_list(self):
        df = pd.read_csv(proprietary_fn,header=None,names=['name','cnt','label'],
                         keep_default_na=False)
        df['lower'] = df.name.str.lower()
        self.proprietary_lst = list(df.lower.unique())
        
    def _get_match_list(self,CASN,ig):
        cond1 = self.match_df.CASNumber==CASN
        cond2 = self.match_df.ig_clean==ig
        mlst = self.match_df[cond1&cond2].matches
        #print(mlst)
        if len(mlst)>0:
            return list(mlst)[0]
        else: return []
        
    def _get_perfect_match(self,CASN,ig):
        matches = self._get_match_list(CASN,ig)
        for m in matches:
            if (m[0]==1)&(m[1]==1):
                return (m[2],m[3])
        return None
    
    def _save_perfect_matches(self):
        # ******* this is done once! ********
        for tup in self.reviewed.keys():
            m = self._get_perfect_match(tup[0],tup[1])
            if m:
                #print(tup[0],' ** ',tup[1],' ** ',m)
                self._add_to_results_dic(tup[0],tup[1],m[0],1) 
                print(f'Perfect match for {tup}')

    def printpair(self,CAS,ig,pre=' ',pos4=' ',pos5=' '):
        print('{:>4}: {:>12}  --  {:<50} {} {}'.format(pre,CAS,ig,pos4,pos5))
        
    ###  reviewing the 'perfect' (or close to perfect) matches                
    def showrecord_CAS_perfect(self,CASN,ig):
        self.printpair(CASN,ig,'orig')
        match = self._get_perfect_match(CASN,ig)
        self.printpair(match[0],match[1],'ref')
        
    def examine_perfect_set(self):
        # shows all that rated 'perfect' but are not identical
        for tup in self.res_dic.keys():
            if self.res_dic[tup][1]==True: # perfect code
                m = self._get_perfect_match(tup[0],tup[1])
                if (m[0]==tup[0])&(m[1]==tup[1]):
                    next
                else:
                    self.showrecord_CAS_perfect(tup[0],tup[1])
                    keyp = input(prompt='q for quit > ')
                    if keyp=='q': break

    ### reviewing the next best matches
    def showrecord_best_match(self,CASN,ig):
        maxnum = 10
        mlst = self._get_match_list(CASN,ig)
        mlst.sort(reverse=True)
        l = min(len(mlst),maxnum)
        lst = mlst[:l]
        if lst[0][0]==1: c1 = ' ' 
        else: c1='-'
        if lst[0][1]==1: c2 = ' ' 
        else: c2='-'
        print('\n'*3+c1*22+'  '+c2*50)
        self.printpair(CASN,ig,'orig')
        print(c1*22+'  '+c2*50)
        for i,rec in enumerate(lst):
            self.printpair(rec[2],rec[3],str(i),rec[0],rec[1])
        for i in range(maxnum-len(lst)):
            print('---')
        return lst

    def _show_ig_list(self,cas):
        if cas in self.cas_ref_dict.keys():
            print('\n'*3+f'Ref ingredients for {cas}\n')
            for ig in self.cas_ref_dict[cas]:
                print('  -> '+ig)
                
        else:
            print(f'\n\n{cas} is not in the reference dictionary')
            
    def examine_best_match(self):
        rlst = list(self.cas_ref_dict.keys())
        try: rlst.remove('Nope') # not sure why this is in the reference!
        except: pass
        for i,ref in enumerate(rlst):
            keyp = ''
            self._get_potentials(ref)
            print('\n'+'*'*80)
            print('*'*10+f' {i}: Starting {ref}: {self.cas_ref_dict[ref][0]} '+'*'*10 )
            print('*'*80+'\n')
            cntr = 0            
            while cntr < len(self.potentials):
                tup = self.potentials[cntr]
                if self.reviewed[(tup[0],tup[1])]==False:
                    self._show_ig_list(tup[0])
                    lst = self.showrecord_best_match(tup[0],tup[1])
                    print('n for next, so for show orig, p for proprietary, h for hidden')
                    print('q for quit, s for save, # to assign, #a to flag as ambig')
                    keyp = input(prompt='m/ma for manual, t/ta to take from orig > ')
                    if keyp=='q': break
                    if keyp=='n': cntr+=1
                    if keyp=='so': self._show_ig_list(tup[0])
                    if keyp=='s': self._save_results_dic()
                    if keyp=='m': 
                        tmp = input(prompt='Enter CAS to use: ')
                        self._add_to_results_dic(tup[0],tup[1],tmp,4)
                        cntr += 1
                    if keyp=='ma': 
                        tmp = input(prompt='Enter CAS to use (mark as ambig): ')
                        self._add_to_results_dic(tup[0],tup[1],tmp,3)
                        cntr += 1
                    if keyp=='p':
                        self._add_to_results_dic(tup[0],tup[1],tup[0],7)
                        cntr += 1                        
                    if keyp=='h':
                        self._add_to_results_dic(tup[0],tup[1],tup[0],8)
                        cntr += 1                        
                    if keyp=='t': 
                        self._add_to_results_dic(tup[0],tup[1],tup[0],5)
                        cntr += 1
                    if keyp=='ta': 
                        self._add_to_results_dic(tup[0],tup[1],tup[0],6)
                        cntr += 1
                    nums = [str(x) for x in range(len(lst))]
                    if keyp in nums:
                        keyi = int(keyp)
                        self._add_to_results_dic(tup[0],tup[1],lst[keyi][2],2)
                        cntr += 1
                    # use #a to flag as ambiguous, though keeping cas
                    num_plus = [str(x)+'a' for x in range(len(lst))]
                    if keyp in num_plus:
                        keyi = int(keyp[0])
                        self._add_to_results_dic(tup[0],tup[1],lst[keyi][2],3)
                        cntr += 1
                else:
                    cntr += 1
   
            if keyp=='q': break 
        self._save_results_dic()

    def examine_proprietary(self):
        print('FIRST, THE CAS NUMBER FIELD')
        cntr = 1
        for tup in self.reviewed:
            if self.reviewed[tup]==False:
                if (tup[0] in self.proprietary_lst):
                    print('\n\n',tup,cntr,'\n\n')
                    keyp = input(prompt='q to quit, x to skip, s to save, Enter to take > ')
                    if keyp=='q':
                        break
                    if keyp=='':
                        self._add_to_results_dic(tup[0],tup[1],'proprietary',7)
                    if keyp=='s':
                        self._save_results_dic()
                    if keyp=='x':
                        pass
                    cntr+=1
        print('\n----------'*10)
        print('SECOND, THE INGREDIENT FIELD')
        cntr = 0
        for tup in self.reviewed:
            if self.reviewed[tup]==False:
                if (tup[1] in self.proprietary_lst):
                    print('\n\n',tup,cntr,'\n\n')
                    keyp = input(prompt='q to quit, x to skip, s to save, Enter to take > ')
                    if keyp=='q':
                        break
                    if keyp=='':
                        self._add_to_results_dic(tup[0],tup[1],'proprietary',7)
                    if keyp=='s':
                        self._save_results_dic()
                    if keyp=='x':
                        pass
                    cntr+=1

    def examine_cas_alone(self):
        # used after above methods to catch reasonable pairs wehn cas is a perfect match
        for tup in self.reviewed:
            if self.reviewed[tup]==False:
                if tup[0] in self.cas_ref_dict:
                    self.printpair(tup[0],tup[1])
                    keyp = input(prompt='q to quit > ' )
                    if keyp=='q': break
    
    def examine_not_valid(self):
        # !!!! DONT USE THIS YET
        cntr = 1
        for tup in self.reviewed:
            if self.reviewed[tup]==False:
                if not ct.is_valid_CAS_code(tup[0]):
                    print('\n\n',tup,cntr,'\n\n')
                    cntr+=1
                    self._add_to_results_dic(tup[0],tup[1],'non_valid_cas',8)
# =============================================================================
#                     keyp = input(prompt='q to quit, x to skip, s to save, Enter to take > ')
#                     if keyp=='q':
#                         break
#                     if keyp=='':
#                         self._add_to_results_dic(tup[0],tup[1],'proprietary',7)
#                     if keyp=='s':
#                         self._save_results_dic()
#                     if keyp=='x':
#                         pass
#                     cntr+=1
# =============================================================================
    def _show_percent_summaries(self,df,goal=0.99):
        print(f'Fraction of records curated:      {round(df[df.flag>9].cnt.sum()/df.cnt.sum(),2)}')
        cond1 = (df.flag>0)&(df.flag<10)
        print(f'Fraction of records identified:   {round(df[cond1].cnt.sum()/df.cnt.sum(),2)}')
        print(f'Fraction of records unclassified: {round(df[df.flag<1].cnt.sum()/df.cnt.sum(),2)}')
        
                
    def get_work_curve(self,goal=0.99):
        out = pd.read_csv(out_fn,quotechar='$',keep_default_na=False)
        out = pd.merge(out,self.casig_orig,on=['CASNumber','ig_clean'],how='outer',
                       validate='1:1',indicator=True)
        out = out.sort_values(by=['flag','cnt'],ascending=False)
        #out = out.sort_values(by='cnt',ascending=False)
        out.reset_index(inplace=True)
        out['frac_db'] = out.cnt.cumsum()/out.cnt.sum()
        out['frac_item'] = out.index/len(out)   
        out['num_items'] = out.index
        out['color'] = np.where(out.flag==0,'gray','orange')
        out.color = np.where(out.flag>9,'green',out.color)
        self._show_percent_summaries(out)
        return out
