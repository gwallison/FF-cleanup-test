# -*- coding: utf-8 -*-
"""
Created on Sun Mar  3 19:32:35 2019

@author: BMansfield

This is used to correct names such as suplier, tradename, etc...

"""
import pandas as pd
from fuzzywuzzy import process
import core.FF_Container as ffc
import pickle

class Column_corrector():
    def __init__(self,ver_name='CurrentData',colName='sup_clean',
                 outdir='./out/'):
        self.colName = colName
        self.FFver = ver_name
        self.outdir = outdir
        self.outfn = self.outdir+'review_'+self.colName+'.csv'
        self.WORDS = self.getWORDS(self.get_input_list())
        self.wlst = self.get_sorted_words()
        self.res_dic = self.get_results_dic()
        self.groups = {}
        self.assemble_groups()
        self.guessdir = self.outdir+'guess/'
        self.guessdicfn = self.guessdir+self.colName+'guessdic.pkl'
        try:
            with open(self.guessdicfn,'rb') as f:
                self.guessdic = pickle.load(f)
            
        except:
            self.guessdic = {}
        
    def get_input_list(self):
        c = ffc.FF_Container(self.FFver)
        c.loadTables(which=['ing_ev'])
        return c.ing_ev[self.colName].copy()
        
    def getWORDS(self,series):
        series = series.astype('str')
        ser = series.value_counts()
        return ser.to_dict()
    
    def get_sorted_words(self):
        lst = []
        for w in self.WORDS:
            lst.append((self.WORDS[w],w))
        lst = sorted(lst,reverse=True)
        lst2 = []
        for i in lst: 
            lst2.append(i[1])
        return lst2
    
    def get_results_dic(self):
        try: df = pd.read_csv(self.outfn)
        except: df = pd.DataFrame()
        dic = {}
        for row in df.itertuples(index=False):
            dic[row.original] = row.best_guess
        return dic
    
    def add_word_to_groups(self,group,word):
        group = str(group)
        word = str(word)
        self.groups.setdefault(group,[]).append(word)
        
    def assemble_groups(self):
        for k in self.res_dic:
            self.add_word_to_groups(self.res_dic[k],k)
    
    def save_results_dic(self):
        varnamelst = []
        best_guess = []
        for name in self.res_dic.keys():
            varnamelst.append(name)
            best_guess.append(self.res_dic[name])
        df = pd.DataFrame({'original':varnamelst,
                           'best_guess':best_guess})
        df.to_csv(self.outfn,index=False)
        
    def make_not_assigned_lst(self):
        self.not_assigned = []
        for w in self.wlst:
            if w not in self.res_dic:
                self.not_assigned.append(w)
     
    def is_not_assigned(self,w):
        if w in self.not_assigned:
            return True
        return False

    def get_frac_not_assigned(self):
        tot = 0
        notassign = 0
        for w in self.WORDS:
            cnt = self.WORDS[w]
            if w != 'nan':
                tot += cnt
                if self.is_not_assigned(w):
                    notassign += cnt
        return notassign/tot
    
    def best_guess_scores(self,name,min_score=20):
        #uses fuzzy wuzzy to find matches within a comparison list
        if name in self.guessdic:
            res = self.guessdic[name]
            print('in self.guessdic')
        else:
            print('NOT in self.guessdic')
            complst = []
            for w in self.WORDS:
                if self.is_not_assigned(w):
                    complst.append(w)
            res = process.extract(name,complst,
                                  limit=30)
        #print(f'({name}):, {res}')
        out = []
        for r in res:
            if r[1] >= min_score:
                if self.is_not_assigned(r[0]):
                    out.append((r[0],r[1],self.WORDS[r[0]]))
        return out

    def best_guess_scores_full(self,name,min_score=20):
        #uses fuzzy wuzzy to find matches within a comparison list
        # this version for preloading - collect_guesses.
        res = process.extract(name,self.WORDS.keys(),
                              limit=200)
        #print(res)
        out = []
        for r in res:
            if r[1] >= min_score:
                out.append((r[0],r[1],self.WORDS[r[0]]))
        return out
    
       
    def showrecord(self,currindex):
        tname = self.wlst[currindex]
        bgl = [('NO GOOD MATCH',0)]
        if tname in self.WORDS.keys():
            bgl = self.best_guess_scores(tname)
        print('Original:{:>50}'.format(tname))
        print('Take: -----------------------------------------')
        for i,res in enumerate(bgl):
            if i <16:
                print('({:2})  >{:>50}<    score: {:>3};  in FF {:>8}'.format(i,
                      res[0],res[1],res[2]))
        return bgl 

    def collect_guesses(self):
        #because the it takes so long collecting the guesses with big sets, like the
        # trade names, I am going to allow assembing these guesses BEFORE I am actually
        # manually doing the sorting.
        cnt = -1
        for term in self.wlst:
            cnt += 1
            # some values were initially misrecorded. This corrects them
            if term in self.guessdic: 
                if isinstance(self.guessdic[term],list) == True: continue
            if len(term)<3 : continue
            if term=='---' : continue
            if term[0]=='/': continue
            print(f'{cnt}. Working on guesses for {term}')
            res = self.best_guess_scores_full(term,min_score=20)
            print(f'Found {len(res)} matches, lowest score = {res[-1][1]}')
            #pname = self.guessdir+self.colName+'_'+str(i)+'.pkl'
            self.guessdic[term] = res
            #with open(pname, 'wb') as f:
            #    pickle.dump(res,f)
            with open(self.guessdicfn,'wb') as f:
                pickle.dump(self.guessdic,f)
    
    def FormGroups(self):
        # start review display
        currIndex = 0
        currGroup = ''
        print(f'\n******* STARTING {self.colName} REVIEW  *********\n')
        while currIndex < len(self.wlst):
            self.make_not_assigned_lst()
            print(f'Number of unique words processed: {currIndex}/{len(self.wlst)}')
            print(f'Percent of records still unassigned: {round(self.get_frac_not_assigned()*100,2)}%\n')
            tname = self.wlst[currIndex]
            print(f'Current name: {tname}')
            if tname in self.res_dic.keys():
                best_guess = self.res_dic[tname]
                currGroup = best_guess
                print(f'Assigned group: {best_guess}; number members: {len(self.groups[best_guess])}')
            else:
                print('No assigned group')
            bgl = self.showrecord(currIndex)
            print(f'\n************* Current sys Group: <{currGroup}> ************\n')
            
            if currGroup != '':
                print('Use 0 to 30  to add selected name to group')
                print('Use "[0,1,2,5 " etc. to take a list of items')
                #print('"h" to hand enter a name      ')
            print('"n/b" next/back                "txx"   take original as group name and add "0"')
            print('"j"   jump to next unrecorded  "g"   enter group manually')
            print('"d"   dump group list to text  "ALL" take all in list')
            print('"m"   use "multiple types"     "a"   use "ambiguous name" as group')
            print('"q" to quit                    "s" to save ')
            keyp = input(prompt='Press code then Enter > ')
            if currGroup != '':
                nums = [str(x) for x in range(16)]
                if keyp in nums:
                    keyi = int(keyp)
                    self.res_dic[bgl[keyi][0]] = currGroup
                    self.add_word_to_groups(currGroup,bgl[keyi][0])
                    
                try: 
                    if keyp[0] == '[':
                        nlst = keyp[1:].split(',')
                        for n in nlst:
                            keyi = int(n)
                            self.res_dic[bgl[keyi][0]] = currGroup
                            self.add_word_to_groups(currGroup,bgl[keyi][0])
                except:
                    print('Ill-formed list!!')
                if keyp == 'ALL':
                    for num in nums:
                        keyi = int(num)
                        self.res_dic[bgl[keyi][0]] = currGroup
                        self.add_word_to_groups(currGroup,bgl[keyi][0])
                        
            if keyp == 'q':
                break
            if keyp[0] == 't':
                if len(keyp)==1:
                    currGroup = tname
                else: # which item to take?
                    v = int(keyp[1:])
                    currGroup = bgl[v][0]
                    
            if keyp == 'a':
                currGroup = 'ambiguous name'
            if keyp == 'm':
                currGroup = 'multiple types'
            if keyp == 'w':
                currGroup = 'water source details'
            if keyp == 'g':
                currGroup = input(prompt = 'Type name then Enter >')
            if keyp == 'j':
                currIndex +=1
                currGroup = ''
                while currIndex < len(self.wlst):
                    if self.wlst[currIndex] in self.res_dic.keys():
                        currIndex += 1
                    else:
                        break

            if keyp == 's':
                self.save_results_dic()
            if keyp == 'n':
                currIndex+=1
                currGroup = ''
            if keyp == 'b':
                currIndex-=1
                currGroup = ''
            if keyp == 'd':
                l = list(self.groups.keys())
                l.sort()
                with open(self.outdir+'groups.txt','w') as f:
                    for k in l:
                        f.write(f'{k} : {self.groups[k]}\n')
                            
        self.save_results_dic()
        print('*********  EXIT  ************')

    def mark_long_lists_as_multiple(self):
        self.make_not_assigned_lst()
        for w in self.wlst:
            if self.is_not_assigned(w):
                try:
                    lst = w.split(',')
                    if len(lst)>3:
                        self.res_dic[w] = 'multiple types'
                        self.add_word_to_groups('multiple types',w)
                except:
                    pass
                
    def classify_remaining(self,label='unclassified'):
        # use this carefully!!! 
        self.make_not_assigned_lst()
        for w in self.wlst:
            if self.is_not_assigned(w):
                self.res_dic[w] = label
                self.add_word_to_groups(label,w)
        

if __name__ == '__main__':
    cc = Column_corrector()
    