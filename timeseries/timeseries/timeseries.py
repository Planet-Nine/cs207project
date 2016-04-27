import numpy as np
from pytest import raises
import pype
import numbers

def f(a):
    return a

class LazyOperation():
    """
    An class that takes a function and an arbitrary number of positional arguments 
    or keyword arguments as input
    
    Parameters
    ----------
    function : an arbitrary function
    args : arbitrary positional arguments
    kwargs : arbitrary keyword arguments
   
    Returns
    -------
    eval(LazyOperation): value
        a value representing the result of evaluating function with arguments args and kwargs
    __str__ / __repr__:
        when printing LazyOperation, the class name is printed followed by the function name,
        the positional arguments and the keyword arguments 
    
    Examples
    --------
    >>> a = TimeSeries([0,5,10], [1,2,3])
    >>> b = TimeSeries([1,2,3], [5,8,9])
    >>> thunk = check_length(a,b)
    >>> thunk.eval()
    True
    >>> assert isinstance( lazy_add(1,2), LazyOperation ) == True
    >>> thunk = lazy_mul( lazy_add(1,2), 4)
    >>> thunk.eval()
    12
    >>> print(thunk)
    LazyOperation( lazy_mul, args = (3, 4), kwargs = {} )
    """
      
    def __init__(self,function,*args,**kwargs):
        self.function = function
        self.args = args
        self.kwargs = kwargs
    def __str__(self):
        class_name = type(self).__name__
        function_name = self.function.__name__
        str_return = "{}( {}, args = {}, kwargs = {} )".format(class_name, function_name, self.args, self.kwargs)
        return str_return
    def eval(self):
        l = []
        for arg in self.args:
            if isinstance(arg,LazyOperation):
                l += [arg.eval()]
            else:
                l += [arg]
        self.args = tuple(l)
        for kwarg in self.kwargs:
            if isinstance(self.kwargs[kwarg],LazyOperation):
                self.kwargs[kwarg] = self.kwargs[kwarg].eval()
        return self.function(*self.args,**self.kwargs)


class TimeSeries(): 
    """
    An class that takes a sequence of integers or floats as input
    
    Parameters
    ----------
    data : any finite numeric sequence
    time : any finite, monotonically increasing numeric sequence
   
    Returns
    -------
    len(TimeSeries): int
        an integer representing the length of the time series
    Timeseries[position:int]: number
        returns the value of the TimeSeries at position
    Timeseries[position:int] = value:int/float
        set value of TimeSeries at position to be value
    __str__ / __repr__:
        when printing TimeSeries, if the total length of the Timeseries is greater than 10
        the result shows the first ten elements and its total length, else it prints the 
        whole Timeseries
        
    Examples
    --------
    >>> a = TimeSeries([0,5,10], [1,2,3])
    >>> threes = TimeSeries(range(100),range(100))
    >>> len(a)
    3
    >>> a[10]
    3
    >>> a[10]=10
    >>> a[10]
    10
    >>> print(a)
    [(0, 1), (5, 2), (10, 10)]
    >>> print(threes)
    [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), ...], length=100
    >>> [v for v in TimeSeries([0,1,2],[1,3,5])]
    [1, 3, 5]
    >>> a = TimeSeries([0,5,10], [1,2,3])
    >>> b = TimeSeries([2.5,7.5], [100, -100])
    >>> c = TimeSeries([0,5,10], [1,2,3])
    >>> a == c
    True
    >>> a == b
    False
    >>> print(a.interpolate([1])) 
    [(1, 1.2)]
    >>> print(a.interpolate(b.times()))
    [(2.5, 1.5), (7.5, 2.5)]
    >>> print(a.interpolate([-100,100]))
    [(-100, 1.0), (100, 3.0)]
    >>> b.mean()
    0.0
    >>> a.mean()
    2.0
    >>> a = TimeSeries([],[])
    >>> a.mean()
    Traceback (most recent call last):
        ...
    ValueError: Cannot perform operation on empty list
    >>> a = TimeSeries([1,2],[1,'a'])
    >>> a.mean()
    Traceback (most recent call last):
        ...
    TypeError: cannot perform reduce with flexible type
    >>> a = TimeSeries([0,5,10], [1,2,3])
    >>> lazy_series = a.lazy
    >>> assert isinstance(lazy_series.eval(),TimeSeries)==True
    >>> a = TimeSeries([0,5], [1,2,3])
    Traceback (most recent call last):
        ...
    ValueError: Not the same length
    >>> a = TimeSeries([0,5,10], [1,2,3])
    >>> a[1]
    Traceback (most recent call last):
        ...
    IndexError: Time does not exist
    >>> a[1] = 5
    Traceback (most recent call last):
        ...
    IndexError: Time does not exist
    >>> assert (0 in a) == True
    >>> a
    [(0, 1), (5, 2), (10, 3)]
    >>> a = TimeSeries(range(11),range(1,12))
    >>> a
    [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8), (8, 9), (9, 10), ...], length=11
    >>> a.values()
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    >>> a.items()
    [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8), (8, 9), (9, 10), (10, 11)]
    >>> a.std()
    3.1622776601683795
    >>> b = TimeSeries([1,2],[3,4])
    >>> a._check_times_helper(b)
    Traceback (most recent call last):
        ...
    ValueError: [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8), (8, 9), (9, 10), ...], length=11 and [(1, 3), (2, 4)] must have the same times
    >>> a == b
    False
    >>> a == 'hi'
    False
    >>> 2+b
    [(1, 5), (2, 6)]
    >>> TimeSeries([1,2],[5,6])+b
    [(1, 8), (2, 10)]
    >>> 2*b
    [(1, 6), (2, 8)]
    >>> TimeSeries([1,2],[5,6])*b
    [(1, 15), (2, 24)]
    >>> 2-b
    [(1, -1), (2, -2)]
    >>> TimeSeries([1,2],[5,6])-b
    [(1, 2), (2, 2)]
    
    Notes
    -----
    PRE: `data` is numeric
    
    """
    def __init__(self,time,data):
        if len(time)!=len(data):
            raise ValueError("Not the same length")
        self.time=np.array(time)
        self.data=np.array(data)
        self.index=0
        self.len=len(time)
    @pype.component
    def __len__(self):
        return len(self.data)
    def __getitem__(self, time):
        if time in self.time:
            return int(self.data[np.where(self.time==time)])
        raise IndexError("Time does not exist")
    def __setitem__(self,time,value):
        if time not in self.time:
             raise IndexError("Time does not exist")
        self.data[np.where(self.time==time)]=value
    def __contains__(self, time):
        return time in self.time
    def __next__(self): 
        try:
            word = self.data[self.index] 
        except IndexError:
            raise StopIteration() 
        self.index += 1
        return word 
    def __iter__(self):
        return self    
    def itertimes(self):
        return iter(self.time)
    def itervalues(self):
        return iter(self.data)
    def iteritems(self):
        return iter(list(zip(self.time,self.data)))
    def __str__(self):
        if self.len>10:
            return '[{}, ...], length={}'.format(str(list(zip(self.time,self.data))[0:10])[1:-1], self.len)
        return '{}'.format(list(zip(self.time,self.data)))
    def __repr__(self):
        if self.len>10:
            return '[{}, ...], length={}'.format(str(list(zip(self.time,self.data))[0:10])[1:-1], self.len)
        return '{}'.format(list(zip(self.time,self.data)))
    def values(self):
        return list(self.data)
    def times(self):
        return list(self.time)
    def items(self):
        return list(zip(self.time,self.data))
    def interpolate(self,newtime):
        newvalue=np.interp(newtime,self.time,self.data)
        return TimeSeries(newtime,newvalue)
    def to_json(self):
        # json_dict={}
        # json_dict['time']=self.time.tolist()
        # json_dict['data']=self.data.tolist()
        json_dict=(self.time.tolist(),self.data.tolist())
        return json_dict
    @property
    def lazy(self):
        lazy_fun = LazyOperation(f,self)
        return lazy_fun
    @pype.component
    def mean(self):
        if self.len == 0: raise ValueError("Cannot perform operation on empty list")
        return np.mean(self.data)
    @pype.component
    def std(self):
        if self.len == 0: raise ValueError("Cannot perform operation on empty list")
        return np.std(self.data)
    def median(self):
        if self.len == 0: raise ValueError("Cannot perform operation on empty list")
        return np.median(self.data)
    @pype.component
    def _check_times_helper(self,rhs):
        if not self.times() == rhs.times():
            raise ValueError(str(self)+' and '+str(rhs)+' must have the same times')
    
    def __eq__(self, other):
        if isinstance(other, TimeSeries):
            return ((len(self) == len(other)) and all(self.time==other.time) and all(self.data==other.data ))
        else:
            return NotImplemented
    @pype.component        
    def __add__(self, rhs):
        try:
            if isinstance(rhs, numbers.Real):
                return TimeSeries(self.time,self.data+rhs) 
            else: 
                self._check_times_helper(rhs)
                pairs = zip(self, rhs)
                return TimeSeries(self.time,self.data+rhs.data)
        except TypeError:
            raise NotImplemented
    
    def __radd__(self, other): # other + self delegates to __add__
        return self + other
    @pype.component    
    def __mul__(self, rhs):
        try:
            if isinstance(rhs, numbers.Real):
                return TimeSeries(self.time,self.data*rhs) 
            else: #
                self._check_times_helper(rhs)
                pairs = zip(self, rhs)
                return TimeSeries(self.time,self.data*rhs.data)
        except TypeError: 
            raise NotImplemented
    @pype.component    
    def __rmul__(self, other): # other + self delegates to __mul__
        return self*other
    @pype.component
    def __truediv__(self, rhs):
        try:
            if isinstance(rhs, numbers.Real):
                return TimeSeries(self.time,self.data/rhs) 
            else: #
                self._check_times_helper(rhs)
                pairs = zip(self, rhs)
                return TimeSeries(self.time,self.data/rhs.data)
        except TypeError:
            raise NotImplemented 

    @pype.component    
    def __sub__(self, rhs):
        try:
            if isinstance(rhs, numbers.Real):
                return TimeSeries(self.time,self.data-rhs) 
            else: #
                self._check_times_helper(rhs)
                pairs = zip(self, rhs)
                return TimeSeries(self.time,self.data-rhs.data)
        except TypeError:
            raise NotImplemented
    @pype.component    
    def __rsub__(self, other): # other + self delegates to __sub__
        return -self + other
    @pype.component    
    def __pos__(self):
        if self.len!=0:
            return TimeSeries(self.time,self.data)
        else:
            raise ValueError
    @pype.component    
    def __neg__(self):
        if self.len!=0:
            return TimeSeries(self.time, -1*self.data)
        else:
            raise ValueError

    def __abs__(self):
        if self.len!=0:
            return np.sqrt(np.sum(self.data*self.data))
        else:
            raise ValueError
    
    def __bool__(self):
        if self.len!=0:
            return bool(abs(self))
        else:
            raise ValueError

def lazy(f):
    def inner(*args,**kwargs):
        inner.__name__ = f.__name__
        lazy_fun = LazyOperation(f,*args,**kwargs)
        return lazy_fun
    return inner

@lazy
def check_length(a,b):
    return len(a)==len(b)

@lazy
def lazy_add(a,b):
    return a+b

@lazy
def lazy_mul(a,b):
    return a*b
