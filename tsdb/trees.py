import numpy as np

# Adapted from http://interactivepython.org/runestone/static/pythonds/Trees/SearchTreeImplementation.html
# to allow nodes to contain multiple values, 
# to delete one value at a time rather than the whole node,
# and to perform traversals that collect the values for keys greater than or less than a given key
class BinarySearchTree:

    def __init__(self):
        self.root = None
        self.size = 0

    def length(self):
        return self.size

    def __len__(self):
        return self.size

    def __iter__(self):
        return self.root.__iter__()

    def put(self,key,val):
        if self.root:
            self._put(key,val,self.root)
        else:
            self.root = TreeNode(key,val)
        self.size = self.size + 1

    def _put(self,key,val,currentNode):
        if key == currentNode.key:
            currentNode.payload.append(val)
        elif key < currentNode.key:
            if currentNode.hasLeftChild():
                   self._put(key,val,currentNode.leftChild)
            else:
                   currentNode.leftChild = TreeNode(key,val,parent=currentNode)
        else:
            if currentNode.hasRightChild():
                   self._put(key,val,currentNode.rightChild)
            else:
                   currentNode.rightChild = TreeNode(key,val,parent=currentNode)

    def __setitem__(self,k,v):
        self.put(k,v)

    def get(self,key):
        if self.root:
            res = self._get(key,self.root)
            if res:
                return res.payload
            else:
                return None
        else:
            return None

    def _get(self,key,currentNode):
        if not currentNode:
            return None
        elif currentNode.key == key:
            return currentNode
        elif key < currentNode.key:
            return self._get(key,currentNode.leftChild)
        else:
            return self._get(key,currentNode.rightChild)

    def __getitem__(self,key):
        return self.get(key)

    def __contains__(self,key):
        if self._get(key,self.root):
            return True
        else:
            return False

    def collect(self, key, op):
        pklist = []
        if self.root:
            pklist += self._collect(key, op, self.root)

        return pklist

    def _collect(self, key, op, currentNode):
        pklist = []
        if op(currentNode.key, key):
            pklist += currentNode.payload
        if currentNode.hasLeftChild():
            pklist += self._collect(key, op, currentNode.leftChild)
        if currentNode.hasRightChild():
            pklist += self._collect(key, op, currentNode.rightChild)

        return pklist

    def delete(self,key,val=None):
        if self.size > 1:
            nodeToRemove = self._get(key,self.root)
            if nodeToRemove:
                if val is None:
                    self.remove(nodeToRemove)
                    self.size = self.size-1
                else:
                    try:
                        nodeToRemove.payload.remove(val)
                    except:
                        raise KeyError('Error, key-value pair not in tree')
                    if len(nodeToRemove.payload) == 0:
                        self.remove(nodeToRemove)
                        self.size = self.size-1
            else:
                raise KeyError('Error, key not in tree')
        elif self.size == 1 and self.root.key == key:
            if val is None:
                self.root = None
                self.size = self.size-1
            else:
                try:
                    self.root.payload.remove(val)
                except:
                    raise KeyError('Error, key-value pair not in tree')
                if len(self.root.payload) == 0:
                    self.root = None
                    self.size = self.size-1
        else:
            raise KeyError('Error, key not in tree')

    def __delitem__(self,key):
        self.delete(key)

    def remove(self,currentNode):
        if currentNode.isLeaf(): #leaf
            if currentNode == currentNode.parent.leftChild:
                currentNode.parent.leftChild = None
            else:
                currentNode.parent.rightChild = None
        elif currentNode.hasBothChildren(): #interior
            succ = currentNode.findSuccessor()
            succ.spliceOut()
            currentNode.key = succ.key
            currentNode.payload = succ.payload
        else: # this node has one child
            if currentNode.hasLeftChild():
                if currentNode.isLeftChild():
                    currentNode.leftChild.parent = currentNode.parent
                    currentNode.parent.leftChild = currentNode.leftChild
                elif currentNode.isRightChild():
                    currentNode.leftChild.parent = currentNode.parent
                    currentNode.parent.rightChild = currentNode.leftChild
                else:
                    currentNode.replaceNodeData(currentNode.leftChild.key,
                                                currentNode.leftChild.payload,
                                                currentNode.leftChild.leftChild,
                                                currentNode.leftChild.rightChild)
            else:
                if currentNode.isLeftChild():
                    currentNode.rightChild.parent = currentNode.parent
                    currentNode.parent.leftChild = currentNode.rightChild
                elif currentNode.isRightChild():
                    currentNode.rightChild.parent = currentNode.parent
                    currentNode.parent.rightChild = currentNode.rightChild
                else:
                    currentNode.replaceNodeData(currentNode.rightChild.key,
                                                currentNode.rightChild.payload,
                                                currentNode.rightChild.leftChild,
                                                currentNode.rightChild.rightChild)

class TreeNode:
    def __init__(self,key,val,left=None,right=None,parent=None):
        self.key = key
        self.payload = [val]
        self.leftChild = left
        self.rightChild = right
        self.parent = parent

    def hasLeftChild(self):
        return self.leftChild

    def hasRightChild(self):
        return self.rightChild

    def isLeftChild(self):
        return self.parent and self.parent.leftChild == self

    def isRightChild(self):
        return self.parent and self.parent.rightChild == self

    def isRoot(self):
        return not self.parent

    def isLeaf(self):
        return not (self.rightChild or self.leftChild)

    def hasAnyChildren(self):
        return self.rightChild or self.leftChild

    def hasBothChildren(self):
        return self.rightChild and self.leftChild

    def replaceNodeData(self,key,value,lc,rc):
        self.key = key
        self.payload = value
        self.leftChild = lc
        self.rightChild = rc
        if self.hasLeftChild():
            self.leftChild.parent = self
        if self.hasRightChild():
            self.rightChild.parent = self

    def spliceOut(self):
        if self.isLeaf():
            if self.isLeftChild():
                self.parent.leftChild = None
            else:
                self.parent.rightChild = None
        elif self.hasAnyChildren():
            if self.hasLeftChild():
                if self.isLeftChild():
                    self.parent.leftChild = self.leftChild
                else:
                    self.parent.rightChild = self.leftChild
                self.leftChild.parent = self.parent
            else:
                if self.isLeftChild():
                    self.parent.leftChild = self.rightChild
                else:
                    self.parent.rightChild = self.rightChild
                self.rightChild.parent = self.parent

    def findSuccessor(self):
        succ = None
        if self.hasRightChild():
            succ = self.rightChild.findMin()
        else:
            if self.parent:
                if self.isLeftChild():
                    succ = self.parent
                else:
                    self.parent.rightChild = None
                    succ = self.parent.findSuccessor()
                    self.parent.rightChild = self
        return succ

    def findMin(self):
        current = self
        while current.hasLeftChild():
            current = current.leftChild
        return current


##################

Breakpoints = {}
Breakpoints[2] = np.array([0.]) 
Breakpoints[4] = np.array([-0.67449,0,0.67449]) 
Breakpoints[8] = np.array([-1.1503,-0.67449,-0.31864,0,0.31864,0.67449,1.1503]) 
Breakpoints[16] = np.array([-1.5341,-1.1503,-0.88715,-0.67449,-0.48878,-0.31864,-0.15731,0,0.15731,0.31864,0.48878,0.67449,0.88715,1.1503,1.5341]) 
Breakpoints[32] = np.array([-1.8627,-1.5341,-1.318,-1.1503,-1.01,-0.88715,-0.77642,-0.67449,-0.57913,-0.48878,-0.40225,-0.31864,-0.2372,-0.15731,-0.078412,0,0.078412,0.15731,0.2372,0.31864,0.40225,0.48878,0.57913,0.67449,0.77642,0.88715,1.01,1.1503,1.318,1.5341,1.8627]) 
Breakpoints[64] = np.array([-2.1539,-1.8627,-1.6759,-1.5341,-1.4178,-1.318,-1.2299,-1.1503,-1.0775,-1.01,-0.94678,-0.88715,-0.83051,-0.77642,-0.72451,-0.67449,-0.6261,-0.57913,-0.53341,-0.48878,-0.4451,-0.40225,-0.36013,-0.31864,-0.27769,-0.2372,-0.1971,-0.15731,-0.11777,-0.078412,-0.039176,0,0.039176,0.078412,0.11777,0.15731,0.1971,0.2372,0.27769,0.31864,0.36013,0.40225,0.4451,0.48878,0.53341,0.57913,0.6261,0.67449,0.72451,0.77642,0.83051,0.88715,0.94678,1.01,1.0775,1.1503,1.2299,1.318,1.4178,1.5341,1.6759,1.8627,2.1539]) 

Breakpoints[128] = np.array([-2.4176,-2.1539,-1.9874,-1.8627,-1.7617,-1.6759,-1.601,-1.5341,-1.4735,-1.4178,-1.3662,-1.318,-1.2727,-1.2299,-1.1892,-1.1503,-1.1132,-1.0775,-1.0432,-1.01,-0.9779,-0.94678,-0.91656,-0.88715,-0.85848,-0.83051,-0.80317,-0.77642,-0.75022,-0.72451,-0.69928,-0.67449,-0.6501,-0.6261,-0.60245,-0.57913,-0.55613,-0.53341,-0.51097,-0.48878,-0.46683,-0.4451,-0.42358,-0.40225,-0.38111,-0.36013,-0.33931,-0.31864,-0.2981,-0.27769,-0.25739,-0.2372,-0.21711,-0.1971,-0.17717,-0.15731,-0.13751,-0.11777,-0.098072,-0.078412,-0.058783,-0.039176,-0.019584,0,0.019584,0.039176,0.058783,0.078412,0.098072,0.11777,0.13751,0.15731,0.17717,0.1971,0.21711,0.2372,0.25739,0.27769,0.2981,0.31864,0.33931,0.36013,0.38111,0.40225,0.42358,0.4451,0.46683,0.48878,0.51097,0.53341,0.55613,0.57913,0.60245,0.6261,0.6501,0.67449,0.69928,0.72451,0.75022,0.77642,0.80317,0.83051,0.85848,0.88715,0.91656,0.94678,0.9779,1.01,1.0432,1.0775,1.1132,1.1503,1.1892,1.2299,1.2727,1.318,1.3662,1.4178,1.4735,1.5341,1.601,1.6759,1.7617,1.8627,1.9874,2.1539,2.4176])

class BinaryTree:
    def __init__(self, rep=None, parent=None,threshold = 10,wordlength = 16):
        self.parent = parent
        self.SAX = rep
        self.ts = []
        self.ts_SAX = []
        self.children = []
        self.th = threshold
        self.count = 0
        self.splitting_index = None 
        self.word_length = wordlength
        self.online_mean = np.zeros(self.word_length)
        self.online_stdev = np.zeros(self.word_length)
        self.dev_accum = np.zeros(self.word_length)
        self.left = None
        self.right = None    
            
    def addLeftChild(self, rep,threshold,wordlength): 
        n = self.__class__(rep=rep, parent=self,threshold=threshold, wordlength=wordlength)
        self.left = n
        return n
        
    def addRightChild(self, rep,threshold,wordlength):
        n = self.__class__(rep=rep, parent=self,threshold=threshold, wordlength=wordlength)
        self.right = n
        return n
        
    def addChild(self, rep,threshold,wordlength):
        n = self.__class__(rep=rep, parent=self,threshold=threshold, wordlength=wordlength)
        self.children += [n]
        return n
    
    def hasLeftChild(self):
        return self.left is not None

    def hasRightChild(self):
        return self.right is not None

    def hasAnyChild(self):
        return self.hasRightChild() or self.hasLeftChild()

    def hasBothChildren(self):
        return self.hasRightChild() and self.hasLeftChild()
    
    def hasNoChildren(self):
        return not self.hasRightChild() and not self.hasLeftChild()
    
    def isLeftChild(self):
        return self.parent and self.parent.left == self

    def isRightChild(self):
        return self.parent and self.parent.right == self

    def isRoot(self):
        return not self.parent

    def isLeaf(self):
        return not (self.right or self.left)    
                
class SAXTree(BinaryTree):
        
    def __init__(self, rep=None, parent=None, threshold = 10, wordlength = 16):
        super().__init__(rep, parent,threshold,wordlength)
        
    def _insert_hook(self):
        pass
            
    def insert(self, pk,rep):
        if self.parent == None:
            index = 0
            for i,symbol in enumerate(rep):
                if symbol[0] == '1':
                    index += 2**(self.word_length-i-1)
            self.children[index].insert(pk,rep)
        elif self.right == None and self.left == None and self.count < self.th:
            self.ts += [pk]
            self.ts_SAX += [rep]
            self.mean_std_calculator(rep)
        elif self.right == None and self.left == None:
            self.split()
            l = len(self.SAX[self.splitting_index])
            if rep[self.splitting_index][l] == '1':
                self.right.insert(pk,rep)
            else:
                self.left.insert(pk,rep)
        else:
            l = len(self.SAX[self.splitting_index])
            if rep[self.splitting_index][l] == '1':
                self.right.insert(pk,rep)
            else:
                self.left.insert(pk,rep)
            
    def search(self, rep, pk = None):
        if pk is not None:
            if self.parent == None:
                index = 0
                for i,symbol in enumerate(rep):
                    if symbol[0] == '1':
                        index += 2**(self.word_length-i-1)
                return self.children[index].search(rep,pk)
            elif self.right == None and self.left == None:
                if pk in self.ts:
                    return self
                else:
                    raise ValueError('"{}" not found'.format(pk))
            else:
                l = len(self.SAX[self.splitting_index])
                if rep[self.splitting_index][l] == '1':
                    return self.right.search(rep,pk)
                else:
                    return self.left.search(rep,pk)
        else:
            if self.parent == None:
                index = 0
                for i,symbol in enumerate(rep):
                    if symbol[0] == '1':
                        index += 2**(self.word_length-i-1)
                return self.children[index].search(rep)
            elif self.right == None and self.left == None:
                return self
            else:
                l = len(self.SAX[self.splitting_index])
                if rep[self.splitting_index][l] == '1':
                    return self.right.search(rep)
                else:
                    return self.left.search(rep)
        
    def delete(self, rep, pk):        
        n = self.search(rep,pk)
        index = n.ts.index(pk)
        n.ts.remove(pk)
        n.ts_SAX = n.ts_SAX[:index]+n.ts_SAX[index+1:]
        n.online_mean = np.zeros(self.word_length)
        n.online_stdev = np.zeros(self.word_length)
        n.dev_accum = np.zeros(self.word_length)
        n.count = 0
        for word in n.ts_SAX:
            n.mean_std_calculator(word)
        
       
    def word2number(self,word):
        number = []
        for j in word:
            l = len(j)
            length = 2**l
            for t in sorted(Breakpoints.keys()):
                if length < t:
                    key = t
                    break
            num = int(j,2)
            ind = 2*num
            number += [Breakpoints[key][ind]]
        return np.array(number)
    
    def mean_std_calculator(self,word):
        self.count += 1
        mu_1 = self.online_mean
        value = self.word2number(word)
        delta = value - self.online_mean
        self.online_mean = self.online_mean + delta/self.count
        prod = (value-self.online_mean)*(value-mu_1)
        self.dev_accum = self.dev_accum + prod
        if self.count > 1:
            self.online_stdev = np.sqrt(self.dev_accum/(self.count-1))
    
    def getBreakPoint(self,s):
        l = len(s)
        length = 2**l
        for t in sorted(Breakpoints.keys()):
            if length < t:
                key = t
                break
        num = int(s,2)
        ind = 2*num
        return Breakpoints[key][ind]
    
    def split(self):
        segmentToSplit = None
        if self.SAX is not None:
            diff = None
            for i,s in enumerate(self.SAX):
                b = self.getBreakPoint(s) 
                if b <= self.online_mean[i] + 3*self.online_stdev[i] and b >= self.online_mean[i] - 3*self.online_stdev[i]:
                    if diff is None or np.abs(self.online_mean[i] - b) < diff:
                        segmentToSplit = i
                        diff = np.abs(self.online_mean[i] - b)
            
            if segmentToSplit == None:
                diff = None
                for i,s in enumerate(self.SAX):
                    b = self.getBreakPoint(s) 
                    if diff is None or np.abs(self.online_mean[i] - b) < diff:
                        segmentToSplit = i
                        diff = np.abs(self.online_mean[i] - b)
                
            self.IncreaseCardinality(segmentToSplit)
    
    def IncreaseCardinality(self, segment):
        if self.SAX is None:
            raise ValueError('Cannot increase cardinality of root node')
        newSAXupper = list(self.SAX)
        newSAXupper[segment] = newSAXupper[segment]+'1'
        newSAXlower = list(self.SAX)
        newSAXlower[segment] = newSAXlower[segment]+'0'
        newtsupper = []
        newtslower = []
        newts_SAXupper = []
        newts_SAXlower = []
        l = len(newSAXupper[segment])
        for i,word in enumerate(self.ts_SAX):
            if len(word[segment])<l:
                for ts in self.ts:
                    lengths = len([i for i, j in enumerate(self.ts) if j == ts])
                    if lengths > 1:
                        raise ValueError("Inserted same time series twice")
                raise ValueError("Overflow error, consider increasing threshold or cardinality")    
            if word[segment][l-1] == '1':
                newts_SAXupper += [word]
                newtsupper += [self.ts[i]]
            else:
                newts_SAXlower += [word]
                newtslower += [self.ts[i]]
        
        self.addLeftChild(rep=newSAXlower,threshold=self.th, wordlength=self.word_length)
        self.addRightChild(rep=newSAXupper,threshold=self.th, wordlength=self.word_length)
        for word in newts_SAXupper:
            self.right.mean_std_calculator(word)
        self.right.ts = list(newtsupper)
        self.right.ts_SAX = list(newts_SAXupper)
        for word in newts_SAXlower:
            self.left.mean_std_calculator(word)
        self.left.ts = list(newtslower)
        self.left.ts_SAX = list(newts_SAXlower)
        self.ts = []
        self.ts_SAX = []
        self.count = 0
        self.online_mean = None
        self.online_stdev = None
        self.dev_accum = None
        self.splitting_index = segment
        
        
    def __iter__(self):
        if self is not None:
            if self.hasLeftChild():
                for node in self.left:
                    yield node
            for _ in range(self.count):
                yield self
            if self.hasRightChild():
                for node in self.right:
                    yield node
                    
    def __len__(self):#expensive O(n) version
        start=0
        for node in self:
            start += 1
        return start
    
    def __getitem__(self, i):
        return self.ithorder(i+1)
    
    def __contains__(self, data):
        return self.search(data) is not None

class Tree_Initializer():
    def __init__(self, threshold = 10, wordlength = 16):
        self.tree = SAXTree(threshold=threshold, wordlength=wordlength)
        words = [list('{0:b}'.format(i).zfill(int(np.log(2**wordlength-1)/np.log(2))+1)) for i in range(2**wordlength)]
        for i in range(2**wordlength):
            self.tree.addChild(words[i],threshold,wordlength)
        