# -*- coding: utf-8 -*-
"""
Created on Thu May 23 15:29:09 2019

@author: GAllison

curator is a class that has all the tools to 'curate' the categories in 
a field.  
 - The first phase will often be to 'lump'.  That is to take the most common
     values in a field, potentially add them to the ref_name file, and find
     all the other values in the field that may be the same, then add those to
     the xlate file.
"""
import pickle, csv, re
import pandas as pd
import numpy as np
import core.Lsh_tools as lsh_tools

class Curator():
    def __init__(self,col_name='Supplier',
                 field_dic_name='./out/currentData/_field_dic.pkl'):
        self.col_name = col_name
        self.field_dic_name = field_dic_name
        self._get_field_dic() 
        self.field_df = self.field_dic[self.col_name].copy()
        self.field_df.set_index('clean',inplace=True,verify_integrity=True)
        self.workdir = './norm_col/'+self.col_name+'/'
        self._load_ref_names()
        self._load_xlate()
        self.orig_lst = list(self.field_dic[self.col_name].sort_values(by='cnt',
                             ascending=False)['clean'])
        self.orig_lsh_fn = self.workdir+'orig_lsh.pkl'
        self.ref_lsh_fn = self.workdir+'ref_lsh.pkl'
        self.min_doc_length = 2
        self.total_unassigned = 0
        self.totalcnt = self._get_total_cnt()
        
    def _get_field_dic(self):
        with open(self.field_dic_name,'rb') as f:
            self.field_dic = pickle.load(f)
    def _clear_field_dic(self):
        self.field_dic = None
    def _get_total_cnt(self):
        return self.field_dic[self.col_name].cnt.sum()
    def _get_target_cnt(self,target):
        try:
            return self.field_df.cnt.at[target]
        except:
            print('Target not found')
            return 0
        
    def _load_xlate(self):
        try: self.xlate = pd.read_csv(self.workdir+'xlate.csv',quotechar='$',
                                      keep_default_na=False)
        except: print(f'xlate.csv has problems in {self.workdir}'); exit
        
    def _save_xlate(self):
        self.xlate.to_csv(self.workdir+'xlate.csv',index=False,
                             quoting=csv.QUOTE_ALL,quotechar='$')

    def _add_to_xlate(self,ref,original):
        self.xlate = self.xlate.append(pd.DataFrame({'primary':ref,'original':original},
                                       index=[len(self.xlate)]))
        
    def _load_ref_names(self):
        try: self.ref_name = pd.read_csv(self.workdir+'ref_name.csv',quotechar='$',
                                         keep_default_na=False)
        except: print(f'ref_name.csv has problems in {self.workdir}'); exit
            
    def _save_ref_names(self):
        self.ref_name.to_csv(self.workdir+'ref_name.csv',index=False,
                             quoting=csv.QUOTE_ALL,quotechar='$')

    def _add_to_ref_names(self,name):
       self.ref_name = self.ref_name.append(pd.DataFrame({'name':name},index=[name]))

    def _create_ref_name_from_xlate(self):
        print(f'\n{"*"*50}')
        print('   This will clobber the existing ref_names file!!!')
        print(f'{"*"*50}\n')
        keyp = input(prompt='enter "y" to proceed, just Enter to cancel > ')        
        if keyp == 'y':
            self.ref_name = pd.DataFrame({'name': list(self.xlate.primary.unique())})
            self._save_ref_names()

    def create_lsh_single(self,lst,threshold=0.3):
        return lsh_tools.LSH(lst,threshold=threshold)
            
    def create_lsh_set(self,lst,fn=None,threslst=[0.3]):#,0.7,0.4]):
        lsh_set = lsh_tools.LSH_set(lst,thresholds=threslst)
        with open(fn,'wb') as f:
            pickle.dump(lsh_set,f)
            
    def get_lsh_set(self,fn):
        with open(fn,'rb') as f:
            return pickle.load(f)

    def make_not_assigned_lst(self):
        #print(self.xlate.head())
        self.not_assigned = []
        self.total_unassigned = 0
        for w in self.orig_lst:
            if w not in list(self.xlate.original):
                self.not_assigned.append(w)
                self.total_unassigned += self._get_target_cnt(w)

    def showrecord(self,lst):
        print('Take: *******************************************************')
        for i,res in enumerate(lst):
            if i <16:
                print('({:2})  >{:>50}<    [{}]'.format(i,res,
                                                   self._get_target_cnt(res)))
    
    def lumper(self,new_lsh=False,thres=[0.9,0.6]):
        """good for early in the process to accumulate the major categories into
        the ref_name file.  This one uses an lsh of the unassigned categories to
        help find those that may be related and therefore collected at the same
        time.  For the later prat of the process, us mop_up that helps connect
        the remaining unassigned to the already found groups."""
        # start review display
        self.make_not_assigned_lst()
        if not new_lsh:
            try:
                lsh_set= self.get_lsh_set(self.orig_lsh_fn)
            except:
                self.create_lsh_set(self.orig_lst,threslst=thres,fn=self.orig_lsh_fn)
                lsh_set= self.get_lsh_set(self.orig_lsh_fn)
        else:
            self.create_lsh_set(self.orig_lst,threslst=thres,fn=self.orig_lsh_fn)
            lsh_set= self.get_lsh_set(self.orig_lsh_fn)
            
        #lsh_set = lsh_tools.LSH_set(self.orig_lst)
        currIndex = 0
        print(f'\n*******  STARTING {self.col_name} LUMPING  *********\n')
        ignore = []
        while currIndex < len(self.orig_lst):
            target = self.orig_lst[currIndex]
            if len(target) < self.min_doc_length:
                currIndex += 1
                print(f'Not processing name: {target}.  Too short')
                continue
            if (target in list(self.xlate.original))&(target not in list(self.ref_name.name)):
                currIndex +=1
                continue
            if target in list(self.ref_name.name): currGroup = target
            else: currGroup = '' 
            perc = round(100-(self.total_unassigned/self.totalcnt* 100),2)
            print(f'Percent assigned: {perc}')
            print(f'\n currIndex = {currIndex} target count: {self._get_target_cnt(target)}\n')
            print(' ***********************************************************')
            if currGroup == '':
                print('        -------- GROUP NOT SELECTED --------')
            else:
                print('{}  {:>49} '.format('GROUP:',currGroup))
            print('{}  >{:>50}< '.format('ref:',target))
            lst = lsh_set.get_short_list(target_text=target,
                                         not_assigned=self.not_assigned,
                                         ignore=ignore)
            self.showrecord(lst)
            print('\n')
            print('"s" to save                    "g" to take group name ')
            print('"q" to quit                    "n/b" next/back ')
            print('"e" to manually enter group    "j" jump to next unassigned')
            print('"ix" to ignore top x entries; if x=i, ignore all ')
            keyp = input(prompt='Press code then Enter > ')
            if keyp == '':
                print('No code entered...')
                continue
            if currGroup != '':
                nums = [str(x) for x in range(12)]
                if keyp in nums:
                    keyi = int(keyp)
                    self._add_to_xlate(currGroup,lst[keyi])
                    self.make_not_assigned_lst()
            if keyp == 'e': 
                currGroup = input(prompt = 'Type name then Enter >')
                self._add_to_xlate(currGroup,target)
                if currGroup not in self.ref_name:
                    self._add_to_ref_names(currGroup)
            if keyp == 'q': break
            if keyp == 'n': currIndex+=1; ignore = []
            if keyp == 'b': currIndex-=1; ignore = []
            if keyp == 'g': #take target as group
                currGroup = target
                self._add_to_ref_names(currGroup)
                self._add_to_xlate(currGroup,currGroup)
                self.make_not_assigned_lst()
            if keyp == 's': self._save_xlate(); self._save_ref_names()
            if keyp == 'j':
                currIndex +=1
                while self.orig_lst[currIndex] not in self.not_assigned:
                    currIndex +=1
            if keyp[0] == 'i':
                if len(keyp)==1: # just "i" -> just take top
                    ignore.append(lst[0])
                else:
                    if keyp[1]=='i':
                        for m in range(len(lst)):
                            ignore.append(lst[m])
                    else:
                        for m in range(int(keyp[1])):
                            ignore.append(lst[m])

    def showgrouprecord(self,lst):
        print('Take: *******************************************************')
        for i,res in enumerate(lst):
            if i <16:
                print('({:2})     > {:<50}<    [{}]'.format(i,res,
                                                   self._get_target_cnt(res)))
                            
    def mop_up(self,thresh=0.9):
        """good for later in the process to connect
        the remaining unassigned to the already found groups.
        This tool uses two lsh dictionaries: one for the unassigned and 
        one for the already-selected groups;
        USES single lsh list, not a set, no pickles!"""
        # start review display
        self.make_not_assigned_lst()
        reflst = list(self.ref_name.name.unique())

        ref_lsh = self.create_lsh_single(reflst,threshold=thresh)
        orig_lsh = self.create_lsh_single(self.orig_lst,threshold=thresh)
           
        currIndex = 0
        print(f'\n*******  STARTING {self.col_name} MOP_UP *********\n')
        ignore = []
        keyp = ''
        while currIndex < len(self.orig_lst):
            target = self.orig_lst[currIndex]
            if len(target) < self.min_doc_length:
                currIndex += 1
                print(f'Not processing name: {target}.  Too short')
                continue
            if (target in list(self.xlate.original))&(target not in list(self.ref_name.name)):
                currIndex +=1
                continue
            if target in list(self.ref_name.name): currGroup = target
            else: currGroup = '' 
            lst = ref_lsh.get_bucket_from_other(target=target,other_lsh=orig_lsh)
            # skip when there are no matches
            if (keyp=='j')&(len(lst)==0): 
                currIndex +=1
                continue
            perc = round(100-(self.total_unassigned/self.totalcnt* 100),2)
            print(f'Percent assigned: {perc}')
            print(f'\n currIndex = {currIndex} target count: {self._get_target_cnt(target)}\n')
            print(' ***********************************************************')
            if currGroup == '':
                print('        -------- GROUP NOT SELECTED --------')
            else:
                print('{}     {:<49} '.format('GROUP:',currGroup))
            print('{}  > {:<47}< '.format('target:',target))

            self.showgrouprecord(lst)
            print('\n')
            print('"s" to save                    "g" to take group name ')
            print('"q" to quit                    "n/b" next/back ')
            print('"e" to manually enter group    "j" jump to next unassigned')
            #print('"ix" to ignore top x entries; if x=i, ignore all ')
            keyp = input(prompt='Press code then Enter > ')
            if keyp == '':
                print('No code entered...')
                continue
            nums = [str(x) for x in range(15)]
            if keyp in nums:
                keyi = int(keyp)
                currGroup = lst[keyi]
                self._add_to_xlate(lst[keyi],target)
                self.make_not_assigned_lst()
            if keyp == 'e': 
                currGroup = input(prompt = 'Type name then Enter >')
                self._add_to_xlate(currGroup,target)
                if currGroup not in self.ref_name:
                    self._add_to_ref_names(currGroup)
            if keyp == 'q': break
            if keyp == 'n': currIndex+=1 #; ignore = []
            if keyp == 'b': currIndex-=1 #; ignore = []
            if keyp == 'g': #take target as group
                currGroup = target
                self._add_to_ref_names(currGroup)
                self._add_to_xlate(currGroup,currGroup)
                self.make_not_assigned_lst()
            if keyp == 's': self._save_xlate(); self._save_ref_names()
            if keyp == 'j':
                currIndex +=1
                while self.orig_lst[currIndex] not in self.not_assigned:
                    currIndex +=1
# =============================================================================
#             if keyp[0] == 'i':
#                 if len(keyp)==1: # just "i" -> just take top
#                     ignore.append(lst[0])
#                 else:
#                     if keyp[1]=='i':
#                         for m in range(len(lst)):
#                             ignore.append(lst[m])
#                     else:
#                         for m in range(int(keyp[1])):
#                             ignore.append(lst[m])
#                             
# =============================================================================
    def _get_regex_list(self,re_str,names):
        lst = []
        for name in names:
            # use re.search for whole string search, re.match for beginning
            if re.match(re_str,name):
                lst.append(name)
        return lst
    def test_regex(self,re_str):
        reflst = list(self.ref_name.name.unique())
        lst = self._get_regex_list(re_str,reflst)
        print(lst)
        
    def regex_match(self,len_match=4):
        """uses a simple regex match of target with existing ref names.
        Does not use lsh.  Allows finding of matches that are close in 
        some places (like the beginning) but diverge in other places so
        that lsh matches are less likely."""
        # start review display
        self.make_not_assigned_lst()
           
        currIndex = 0
        print(f'\n*******  STARTING {self.col_name} REGEX_MATCH *********\n')
        ignore = []
        keyp = ''
        while currIndex < len(self.orig_lst):
            target = self.orig_lst[currIndex]
            reflst = list(self.ref_name.name.unique())
            if len(target) < self.min_doc_length:
                currIndex += 1
                print(f'Not processing name: {target}.  Too short')
                continue
            if target in list(self.xlate.original):
                currIndex +=1
                continue
            if target in list(self.ref_name.name): currGroup = target
            else: currGroup = '' 

            # skip when there are no matches
#            if (keyp=='j')&(len(lst)==0): 
#                currIndex +=1
#                continue
            perc = round(100-(self.total_unassigned/self.totalcnt* 100),2)
            print(f'Percent assigned: {perc}')
            print(f'\n currIndex = {currIndex} target count: {self._get_target_cnt(target)}\n')
            print(' ***********************************************************')
            if currGroup == '':
                print('        -------- GROUP NOT SELECTED --------')
            else:
                print('{}     {:<49} '.format('GROUP:',currGroup))
            print('{}  > {:<47}< '.format('target:',target))

            re_str = target[:len_match]
            lst = self._get_regex_list(re_str,reflst)
            self.showgrouprecord(lst)
            print('\n')
            print('"s" to save                    "g" to take group name ')
            print('"q" to quit                    "n/b" next/back ')
            print('"e" to manually enter group    "j" jump to next unassigned')
            print('"a" to use ambiguous as group ')
            #print('"ix" to ignore top x entries; if x=i, ignore all ')
            keyp = input(prompt='Press code then Enter > ')
            if keyp == '':
                print('No code entered...')
                continue
            nums = [str(x) for x in range(20)]
            if keyp in nums:
                keyi = int(keyp)
                currGroup = lst[keyi]
                self._add_to_xlate(lst[keyi],target)
                self.make_not_assigned_lst()
            if keyp == 'a':
                currGroup = 'ambiguous'
                self._add_to_xlate(currGroup,target)
                if currGroup not in self.ref_name:
                    self._add_to_ref_names(currGroup)
            if keyp == 'e': 
                currGroup = input(prompt = 'Type name then Enter >')
                self._add_to_xlate(currGroup,target)
                if currGroup not in self.ref_name:
                    self._add_to_ref_names(currGroup)
            if keyp == 'q': break
            if keyp == 'n': currIndex+=1; ignore = []
            if keyp == 'b': currIndex-=1; ignore = []
            if keyp == 'g': #take target as group
                currGroup = target
                self._add_to_ref_names(currGroup)
                self._add_to_xlate(currGroup,currGroup)
                self.make_not_assigned_lst()
            if keyp == 's': self._save_xlate(); self._save_ref_names()
            if keyp == 'j':
                currIndex +=1
                while self.orig_lst[currIndex] not in self.not_assigned:
                    currIndex +=1
            if keyp[0] == 'i':
                if len(keyp)==1: # just "i" -> just take top
                    ignore.append(lst[0])
                else:
                    if keyp[1]=='i':
                        for m in range(len(lst)):
                            ignore.append(lst[m])
                    else:
                        for m in range(int(keyp[1])):
                            ignore.append(lst[m])
                            
    def get_goal_number(self,goal=0.99):
        self.make_not_assigned_lst()
        out = self.field_df.copy()
        out = out.sort_values(by='cnt',ascending=False)
        out.reset_index(inplace=True)
        out['frac_db'] = out.cnt.cumsum()/self.totalcnt
        return out[out.frac_db>goal].index[0]
                
    def get_work_curve(self,goal=0.99):
        self.make_not_assigned_lst()
        out = self.field_df.copy()
        out = out.sort_values(by='cnt',ascending=False)
        out.reset_index(inplace=True)
        out['frac_db'] = out.cnt.cumsum()/self.totalcnt
        out['frac_item'] = out.index/len(out)   
        out['num_items'] = out.index
        out['color'] = np.where(out.clean.isin(self.not_assigned),
                                       'gray','orange')
        return out

