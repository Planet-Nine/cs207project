
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
    TimeSeries class that abbreviates the output by only printing the first 20 elements 
    followed by an ellipsis if the list used as input for the class has more than 20 elements.
    Otherwise, the output of the print statement uses the whole list.
    """
    def __init__(self, data):
        self.data = list(data)
        
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, position):
        if position < len(self) and position >= 0:
            return (self.data[position])
        else:
            return -1
    
    def __setitem__(self, position, value):
        if position < len(self) and position >= 0:
            self.data[position] = value
        else:
            raise IndexError("Out of bounds.")  
    
    def __str__(self):
        class_name = type(self).__name__
        if len(self) > 20:
            components = self.data[:20]
            string = '{}: ['.format(class_name)
            for j in range(20):
                string += '{}, '.format(components[j])
            string += '...]'
            return string
        else:
            components = self.data
            return '{}: {}'.format(class_name, components)
    
    
    def __repr__(self):
        class_name = type(self).__name__
        if len(self) > 20:
            components = self.data[:20]
            string = '{}(['.format(class_name)
            for j in range(20):
                string += '{}, '.format(components[j])
            string += '...])'
            return string
        else:
            components = self.data
            return '{}({})'.format(class_name, components)
    