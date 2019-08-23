# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 18:24:18 2019

@author: BMansfield

This routine is a review helper for getting through the review of 
CAS/ingredient names in the FracFocus dataset

This version uses the matches from an LSH procedure 
(currently LSH_test_2.ipynb)
"""

import pandas as pd
import numpy as np
nan = np.nan
import pickle
import core.CAS_tools as ct
#import core.FF_Container as ffc

outdir = './out/lsh_reviews/'
refdir = './CAS_ref/out/'

match_fn = './out/casig_matches_2019_06_10.pkl'
out_fn = outdir+'casig_hand_review.csv'
potentials_fn = './out/casig_potentials.pkl'
proprietary_fn = './out/CAS_labels_for_proprietary.csv'
casig_orig_fn = './out/casig_orig.csv'

class CAS_review():
    def __init__(self,target_cas='50-00-0'):
        self.cas = target_cas
        self._get_casig_orig_tuples()
        self.match_df = pd.read_pickle(match_fn)
        self._get_results_dic()
        self._get_ref_dic()
        self._get_review_status()
        self._get_potentials(self.cas)
        self._get_proprietary_list()
        
    def _get_results_dic(self):
        try: df = pd.read_csv(out_fn)
        except: df = pd.DataFrame()
        self.res_dic = {}
        for row in df.itertuples():
            self.res_dic[(row.CASNumber,row.ig_clean)] = (row.bgCAS,row.flag)

    def _add_to_results_dic(self,CASN,ig,bgCAS,flag):
        self.res_dic[(CASN,ig)] = (bgCAS,flag)
        self.reviewed[(CASN,ig)] = True

    def _save_results_dic(self):
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
        keyp = input(prompt='Replace results file? [y/N] > ')
        if keyp=='y': df.to_csv(out_fn,index=False)
    
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
        
    def _get_potentials(self,cas='7732-18-5'):
        with open(potentials_fn,'rb') as f:
            pots = pickle.load(f)
        self.potentials = list(pots[cas])
        
    def _get_casig_orig_tuples(self):
        self.casig_orig = pd.read_csv(casig_orig_fn,usecols=['CASNumber','ig_clean'])
        self.casig_orig.drop_duplicates(inplace=True)
        
    def _get_proprietary_list(self):
        df = pd.read_csv(proprietary_fn,header=None,names=['name','cnt','label'])
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
