# -*- coding: utf-8 -*-
"""
Created on Sun Jan 20 11:14:52 2019

@author: GWAllison

collection of tools for working with CAS RNs
"""
import re
import pandas as pd
from fuzzywuzzy import process

refdir = './out/'

def is_valid_CAS_code(cas):
    # checks if number follows strictest format of CAS registry numbers
    #  three sections separated by '-', section 1 is 2-7 digits with no 
    #  leading zeros, section 2 is two digits (no dropping leading sero),
    #  check digit is just one digit that satisfies algorithm.
    #  No extraneous characters.
    try:
        #print(cas)
        for c in cas:
            #print(f"testchar = '{c}'")
            err = False
            if c not in ['0','1','2','3','4','5','6','7','8','9','-']: 
                err = True
                break
        if err: return False
        lst = cas.split('-')
        if len(lst)!=3 : return False
        if len(lst[2])!=1 : return False # check digit must be a single digit
        if lst[0][0] == '0': return False # leading zeros not allowed
        s1int = int(lst[0])
        if s1int > 9999999: return False
        if s1int < 10: return False
        s2int = int(lst[1])
        if s2int > 99: return False
        if len(lst[1])!=2: return False # must be two digits, even if <10

        # validate test digit
        teststr = lst[0]+lst[1]
        teststr = teststr[::-1] # reverse for easy calculation
        accum = 0
        for i,digit in enumerate(teststr):
            accum += (i+1)*int(digit)
        if accum%10 != int(lst[2]):
            return False
        return True
    except:
        return False

def is_valid_without_junk(cas):
    # first removes all non-digit and '-' characters.  Then returns value
    #  from is_valid_cas_code.
    try:
        cas = re.sub(r'[^0-9-]','',cas)
        #print(cas)
        return is_valid_CAS_code(cas)
    except:
        return False

def correct_zeros(cas):
    if is_valid_CAS_code(cas): # don't need to do anything
        return cas
    lst = cas.split('-')
    if len(lst) != 3: return cas
    if len(lst[2])!= 1: return cas # can't do anything here with malformed checkdigit
    if len(lst[1])!=2:
        if len(lst[1])==1:
            lst[1] = '0'+lst[1]
        else:
            return cas # wrong number of digits in chunk2 to fix here
    lst[0] = lst[0].lstrip('0')
    if (len(lst[0])<2 or len(lst[0])>7): return cas
   
    return f'{lst[0]}-{lst[1]}-{lst[2]}'
    

def best_guess_scores(name,complist,min_score=95):
    #uses fuzzy wuzzy to find matches within a comparison list
    if len(name)>4: # 5 characters is bare minimum for CAS_RN
        res = process.extract(name,complist)
        out = []
        for r in res:
            if r[1] >= min_score:
                out.append((r[0],r[1]))
        return out
    return []

def closest_CAS_match(cas,validlst,min_score=50):
    res = best_guess_scores(cas,validlst,min_score)
    if res==[] : return cas
    return res[0][0]

def best_guess_name(name,complist,min_score=95):
    #uses fuzzy wuzzy to find matches within a comparison list
    try:
        res = process.extract(name,complist)
        out = []
        for r in res:
            if r[1] >= min_score:
                out.append(r)
        return out
    except:
        return []

def closest_ig_match(ig,validlst,min_score=50):
    res = best_guess_name(ig,validlst,min_score)
    #if res==[] : return (False,ig)
    #return (True,res[0][0])
    return res

def get_proprietary_labels():
    # read file of lables used for proprietary and return list.
    fn = refdir+'CAS_labels_for_proprietary.csv'
    df = pd.read_csv(fn,names=['CAS_RN','counts','label'])
    return list(df.CAS_RN)

proprietary_list = get_proprietary_labels()

def get_nondata_labels():
    # read file of lables used for proprietary and return list.
    fn = refdir+'CAS_labels_nondata.csv'
    df = pd.read_csv(fn,names=['CAS_RN','counts','label'])
    return list(df.CAS_RN)
nondata_list = get_nondata_labels()


if __name__ == '__main__':
#    print(validate_CAS('3942238-9-3'))
#    lst = ['12345678','1234567','1234-567','123456']
#    print(best_guess_scores('1234567',lst,93))
    print(is_valid_CAS_code('10049-04-4'))