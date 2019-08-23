# -*- coding: utf-8 -*-
"""
Created on Wed Feb  6 19:41:04 2019

@author: GAllison

This set of routines is used to translate the ref files created with SciFinder
(online) to a reference dictionary used to validate the CASNumbers and 
IngredientNames in the FracFocus dataset.

The ref files are created by searching for a given CAS number and then saving
the SciFinder results to a text file.  The routines below parse those 
text files for the infomation used later downstream 
"""

#import pandas as pd
import os, pickle

inputdir = './sources/CAS_ref_files/'
outputdir = './out/'

#print(os.listdir(inputdir))

cas_ref_dict = {}

def processRecord(rec):
    # return tuple of (cas#,[syn])
    cas = 'Nope'
    prime = ''
    lst = []
    fields = rec.split('FIELD ')
    #print(len(fields))
    for fld in fields:
        if 'Registry Number:' in fld:
            start = fld.find(':')+1
            end = fld.find('\n')
            cas = fld[start:end]
            #print(cas)
        if 'CA Index Name:' in fld:
            start = fld.find(':')+1
            end = fld.find('\n')
            prime = fld[start:end].lower()
            #print(prime)
        if 'Other Names:' in fld:
            start = fld.find(':')+1
            lst = fld[start:].split(';')
            #print(lst)
    olst = [prime]
    for syn in lst:
        syn = syn.strip().lower()
        if len(syn)>0: 
            if syn not in olst:
                olst.append(syn)
    #print(f'olst: {olst}')
    return (cas,olst)


#    return ('123',[])

def processFile(fn,ref_dict):
    with open(fn,'r') as f:
        whole = f.read()
    records = whole.split('END_RECORD')
    for rec in records:
        #print(rec)
        tup = processRecord(rec)
        ref_dict[tup[0]] = tup[1]   
    return ref_dict

def processAll(pklfn):
    cas_ref_dict = {}
    fnlst = os.listdir(inputdir)
    for fn in fnlst:
        cas_ref_dict = processFile(inputdir+fn,cas_ref_dict)
    print(f'Number of CAS references collected: {len(cas_ref_dict)}')
    with open(pklfn,'wb') as f:
        pickle.dump(cas_ref_dict,f)
    return(cas_ref_dict)

def make_syn_dict(cas_ref_dict,pklfn):
    syndict = {}
    for cas in cas_ref_dict.keys():
        for syn in cas_ref_dict[cas]:
            syndict.setdefault(syn.lower(),[]).append(cas)
    with open(pklfn,'wb') as f:
        pickle.dump(syndict,f)
    return syndict

    

if __name__ == '__main__':  
    dic = processAll(outputdir+'cas_ref_dic_pickle.pkl')
    sdict = make_syn_dict(dic,outputdir+'syn_dic_pickle.pkl')
    accum = 0
    for i in sdict.keys():
        if len(sdict[i])>1 : 
            accum +=1
            #print(f'{i}: {sdict[i]}')
    print(f'Number of synonyms with more than 1 CAS number: {accum}')