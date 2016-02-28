import numpy as np

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
            if type(arg).__name__ == type(self).__name__:
                l += [arg.eval()]
            else:
                l += [arg]
        self.args = tuple(l)
        for kwarg in self.kwargs:
            if type(self.kwargs[kwarg]).__name__ == type(self).__name__:
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
    
    Notes
    -----
    PRE: `data` is numeric
    
    """
    def __init__(self,time,data):
        if len(time)!=len(data):
            raise "Not the same length"
        self.time=np.array(time)
        self.data=np.array(data)
        self.index=0
        self.len=len(time)
        
    def __len__(self):
        return len(self.data)
    def __getitem__(self, time):
        if time in self.time:
            return int(self.data[np.where(self.time==time)])
        raise "Time does not exist"
    def __setitem__(self,time,value):
        if time not in self.time:
             raise "Time does not exist"
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
    @property
    def lazy(self):
        lazy_fun = LazyOperation(f,self)
        return lazy_fun
    def mean(self):
        if self.len == 0: raise ValueError("Cannot perform operation on empty list")
        return np.mean(self.data)
    def median(self):
        if self.len == 0: raise ValueError("Cannot perform operation on empty list")
        return np.median(self.data)

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