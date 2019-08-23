# -*- coding: utf-8 -*-
"""
Created on Sun May 19 08:55:48 2019

@author: GAllison
"""

from datasketch import MinHash, MinHashLSH


class LSH():
    def __init__(self,rawlist,shingle_length=2,threshold=0.8):
        self.indoc = rawlist
        self.make_lsh(shingle_length=shingle_length,threshold=threshold)
        
    def make_shingles(self,doc,length=2):
        s = []
        for i in range(len(doc)-(length-1)):
            s.append(doc[i:i+length])
        return s

    def make_shingle_sets(self,doclst=None,length=2):
        if doclst == None: doclst=self.indoc
        sets = {}
        for d in doclst:
            sets[d] = self.make_shingles(d,length)    
        return sets
          
    def make_lsh(self,shingle_length=2,threshold=0.8):
        print(f'Making LSH with threshold of {threshold}, shingle length of {shingle_length}')
        sets = self.make_shingle_sets(self.indoc,shingle_length)
        self.minhashes = {}
        self.lsh = MinHashLSH(threshold=threshold, num_perm=128)
        for k in sets.keys():
            m = MinHash(num_perm=128)
            for item in sets[k]:
                m.update(item.encode('utf8'))
                self.minhashes[k] = m
            self.lsh.insert(k,m)
    def get_minhash(self,doc):
        return self.minhashes[doc]
    def get_bucket(self,target_mh):
        return self.lsh.query(target_mh)
    def get_bucket_from_other(self,target,other_lsh):
        mh = other_lsh.get_minhash(target)
        return self.get_bucket(mh)
    
class LSH_set():
    def __init__(self,rawlist,thresholds=[.9,.7,.4],
                 shingle_lengths = [2]):
        self.indoc = rawlist
        self.thresholds = thresholds
        self.shingle_lengths = shingle_lengths
        self.lshlst = self.make_list_of_lsh()

    def make_list_of_lsh(self):
        lst = []
        for t in self.thresholds:
            for s in self.shingle_lengths:
                l = LSH(rawlist = self.indoc,shingle_length=s,threshold=t)
                lst.append(l)
        return lst                
        
    def get_short_list(self,target_text,maxlen=12,
                       not_assigned=None,ignore=None):
        # returns list of, at most, maxlen of items in not_assigned 
        currindex = 0
        while currindex < len(self.lshlst):
            closest = []
            minhash = self.lshlst[currindex].get_minhash(target_text)
            q = self.lshlst[currindex].lsh.query(minhash)
            #print(f'q = {q}')
            for w in q:
                if (w in not_assigned)&(w not in ignore): closest.append(w)
            if (len(closest)>0) & (len(closest)<=maxlen):
                return closest
            currindex +=1
        return closest[:maxlen]
        
    def get_bucket_dic_with_max_threshold(self,target_mh):
        # returns a dict {match:max_thres, ...}
        # assumes threshold list is in descending threshold order
        allhits = []
        out = {}
        for i,thres in enumerate(self.thresholds):
            q = self.lshlst[i].lsh.query(target_mh)
            for item in q:
                if item not in allhits:
                    out[item] = thres
                    allhits.append(item)
        return out