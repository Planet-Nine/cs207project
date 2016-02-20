import numpy as np

class TimeSeries():
    """ 
    TimeSeries class
    ----------------
    Instances of this class behave as sequences and the value of the only attribute of the 
    class is a list. Inputs to this class should be lists or arrays that can be cast as lists.
    
    Constructor
    -----------
    Takes as argument a type that can be cast as a list and sets the _data attribute of the 
    TimeSeries class instance to that list. 
    
    __str___
    --------
    Returns a string for the output of the print statement acting on an instance of the 
    TimeSeries class that abbreviates the output by only printing the first 10 elements 
    followed by an ellipsis and the length of the list if the list used as input for the 
    class has more than 10 elements. Otherwise, the output of the print statement uses 
    the whole list.
    
    Examples
    --------
    >>> threes = TimeSeries(range(0,10000,3))
    >>> print(threes)
    TimeSeries: [0, 3, 6, 9, 12, 15, 18, 21, 24, 27, ...], length=3334
    """
    def __init__(self, data):
        self.data = list(data)
        
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, position):
        return (self.data[position])
        
    def __setitem__(self, position, value):
        if position < len(self) and position >= 0:
            self.data[position] = value
        else:
            raise IndexError("Out of bounds.")  
    
    def __str__(self):
        class_name = type(self).__name__
        if len(self) > 10:
            components = self.data[:10]
            string = '{}: ['.format(class_name)
            string += ('{}, '*10).format(*components)
            string += '...], length = {}'.format(len(self))
            return string
        else:
            components = self.data
            return '{}: {}'.format(class_name, components)
    
    
    def __repr__(self):
        class_name = type(self).__name__
        if len(self) > 10:
            components = self.data[:10]
            string = '{}(['.format(class_name)
            string += ('{}, '*10).format(*components)
            string += '...], length = {})'.format(len(self))
            return string
        else:
            components = self.data
            return '{}({})'.format(class_name, components)

class ArrayTimeSeries(TimeSeries):
    def __init__(self,data):
        self.data=np.array(data)
    