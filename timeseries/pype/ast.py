class ASTVisitor():
  """
    A class that takes no arguments and is generally used with the walk method of an AST
    
    Parameters
    ----------
   
    Returns
    -------
    visit(node): None
        read only function which looks at a single AST node
    return_value: None
        returns nothing if the walker exits correctly
    
    Examples
    --------
    >>> a = ASTVisitor()
    >>> b = ASTNode()
    >>> a.visit(b)
    >>> a.return_value()
    >>> assert isinstance( a, ASTVisitor ) == True
  """
  def visit(self, astnode):
    'A read-only function which looks at a single AST node.'
    pass
  def return_value(self):
    return None

class ASTModVisitor(ASTVisitor):
  '''A visitor class that can also construct a new, modified AST.
  Two methods are offered: the normal visit() method, which focuses on analyzing
  and/or modifying a single node; and the post_visit() method, which allows you
  to modify the child list of a node.
  The default implementation does nothing; it simply builds up itself, unmodified.'''
  def visit(self, astnode):
    # Note that this overrides the super's implementation, because we need a
    # non-None return value.
    return astnode
  def post_visit(self, visit_value, child_values):
    '''A function which constructs a return value out of its children.
    This can be used to modify an AST by returning a different or modified
    ASTNode than the original. The top-level return value will then be the
    new AST.'''
    return visit_value

class ASTNode(object):
  """
    A class that takes no arguments and defines a node in an abstract syntax tree
    
    Parameters
    ----------
   
    Returns
    -------
    @property
    children: self._children
        returns children of a given node
    @children.setter
    children(child_list): None
        returns nothing, but sets the children of a given node to child_list
    pprint(indent): printed
        returns a string representing the human-readable structure of the AST
    walk(visitor): visitor.return_value()
        returns the value of the return_value method of a visitor that does a depth-first, pre-order traversal of the AST
    
    Examples
    --------
    >>> a,b,c = ASTNode(),ASTNode(),ASTNode()
    >>> a.parent,b.parent,c.parent
    (None, None, None)
    >>> a.children = b
    Traceback (most recent call last):
        ...
    TypeError: 'ASTNode' object is not iterable
    >>> a.children = [b,c]
    >>> assert [id(a)]*2==[id(b.parent),id(c.parent)]
    >>> [child.__class__.__name__ for child in a.children]
    ['ASTNode', 'ASTNode']
    >>> print(a.pprint())
    ASTNode
      ASTNode
      ASTNode
    >>> d = ASTVisitor()
    >>> a.walk(d)
  """
  def __init__(self):
    self.parent = None
    self._children = []

  @property
  def children(self):
    return self._children
  @children.setter
  def children(self, children):
    self._children = children
    for child in children:
      child.parent = self

  def pprint(self,indent=''):
    '''Recursively prints a formatted string representation of the AST.'''
    pfx = indent
    if isinstance(self, ASTNode):
        printed = ''.join([pfx,self.__class__.__name__])

        if any(isinstance(child, ASTNode) for child in self.children):
            for i, child in enumerate(self.children):
                printed += '\n'
                printed += child.pprint(indent+'  ')
            return printed
        else:
            # None of the children as nodes, simply join their repr on a single
            # line.
            printed += ', '.join(repr(child) for child in self.children)
            return printed


    else:
        pass
 
  def walk(self, visitor):
    '''Traverses an AST, calling visitor.visit() on every node.
    This is a depth-first, pre-order traversal. Parents will be visited before
    any children, children will be visited in order, and (by extension) a node's
    children will all be visited before its siblings.
    The visitor may modify attributes, but may not add or delete nodes.'''
    # TODO
    visitor.visit(self)
    for child in self.children:
        child.walk(visitor)
    return visitor.return_value()

  def mod_walk(self, mod_visitor):
    '''Traverses an AST, building up a return value from visitor methods.
    Similar to walk(), but constructs a return value from the result of
    postvisit() calls. This can be used to modify an AST by building up the
    desired new AST with return values.'''

    selfval = mod_visitor.visit(self)
    child_values = [child.mod_walk(mod_visitor) for child in self.children]
    retval = mod_visitor.post_visit(self, selfval, child_values)
    return retval


class ASTProgram(ASTNode):
  """
    A class that takes a list of statements as its argument and defines the overarching program node 
    in an abstract syntax tree
    
    Parameters
    ----------
    statements: a list of statements, which are AST nodes
    
    Returns
    -------
    __init__(statements): None
        returns nothing, but sets the children of the ASTProgram node to the statement list
    Examples
    --------
    >>> b,c = ASTNode(),ASTNode()
    >>> b.parent,c.parent
    (None, None)
    >>> statements = [b,c]
    >>> a = ASTProgram(statements)
    >>> assert [id(a)]*2==[id(b.parent),id(c.parent)]
    >>> print(a.pprint())
    ASTProgram
      ASTNode
      ASTNode
    >>> d = ASTVisitor()
    >>> a.walk(d)
  """
  def __init__(self, statements):
    super().__init__()
    self.children = statements

class ASTImport(ASTNode):
  """
    A class that takes a string which is the name of a module as its argument and defines an import 
    node in an abstract syntax tree
    
    Parameters
    ----------
    mod: a string representing a module
    
    Returns
    -------
    __init__(mod): None
        returns nothing, but sets the mod attribute of the ASTImport node to mod
    module: self.mod
        returns the string representing the module to be imported
    
    Examples
    --------
    >>> b,c = ASTImport('mod'),ASTNode()
    >>> statements = [b,c]
    >>> a = ASTProgram(statements)
    >>> assert [id(a)]*2==[id(b.parent),id(c.parent)]
    >>> print(a.pprint())
    ASTProgram
      ASTImport
      ASTNode
    >>> b.module
    'mod'
  """
  def __init__(self, mod):
    super().__init__()
    self.mod = mod
  @property
  def module(self):
    return self.mod

class ASTComponent(ASTNode): 
  """
    A class that takes a string which serves as the name of the component and expressions as its argument 
    and defines a component node in an abstract syntax tree
    
    Parameters
    ----------
    name: a string representing the name of the component
    expressions: a list of expressions, which are AST nodes
    
    Returns
    -------
    __init__(name,expressions): None
        returns nothing, but sets the children of ASTComponent to ASTID(name) and the expressions
    name: self.children[0].name
        returns a string representing the name of the component
    expressions: self.children[1:]
        returns a list of AST nodes representing the remaining children of the component node
    
    Examples
    --------
    >>> name,expressions = 'comp1',[ASTNode()]
    >>> a = ASTComponent(name,expressions)
    >>> statements = [a]
    >>> b = ASTProgram(statements)
    >>> a.name.name
    'comp1'
    >>> [child.__class__.__name__ for child in a.expressions]
    ['ASTNode']
    >>> print(b.pprint())
    ASTProgram
      ASTComponent
        ASTID
        ASTNode
  """
  def __init__(self,name,expressions):
    super().__init__()
    self.children = [ASTID(name),*expressions]
    
  @property
  def name(self): # Return an element of self.children
    return self.children[0]
  @property
  def expressions(self): # Return one or more children
    return self.children[1:]

class ASTInputExpr(ASTNode): 
  """
    A class that takes a list of declarations as its argument 
    and defines an input expression node in an abstract syntax tree
    
    Parameters
    ----------
    declaration_list: a list of declarations, which are AST nodes
    
    Returns
    -------
    __init__(declaration_list): None
        returns nothing, but sets the children of ASTInputExpr to declaration_list
    
    Examples
    --------
    >>> declaration_list = [ASTID('name'),ASTNode()]
    >>> a = ASTInputExpr(declaration_list)
    >>> [child.__class__.__name__ for child in a.children]
    ['ASTID', 'ASTNode']
    >>> print(a.pprint())
    ASTInputExpr
      ASTID
      ASTNode
  """
  def __init__(self,declaration_list=None):
    super().__init__()
    if declaration_list:
        self.children = declaration_list

class ASTOutputExpr(ASTNode):
  """
    A class that takes a list of declarations as its argument 
    and defines an output expression node in an abstract syntax tree
    
    Parameters
    ----------
    declaration_list: a list of declarations, which are AST nodes
    
    Returns
    -------
    __init__(declaration_list): None
        returns nothing, but sets the children of ASTOutputExpr to declaration_list
    
    Examples
    --------
    >>> a = ASTOutputExpr()
    >>> [child.__class__.__name__ for child in a.children]
    []
    >>> declaration_list = [ASTID('name'),ASTNode()]
    >>> a = ASTOutputExpr(declaration_list)
    >>> [child.parent.__class__.__name__ for child in a.children]
    ['ASTOutputExpr', 'ASTOutputExpr']
  """
  def __init__(self,declaration_list=None):
    super().__init__()
    if declaration_list:
        self.children = declaration_list

class ASTAssignmentExpr(ASTNode): 
  """
    A class that takes an ID to assign to and an expression to be assigned as its arguments 
    and defines an assignment expression node in an abstract syntax tree
    
    Parameters
    ----------
    ID: string
    expression: an AST node
    
    Returns
    -------
    __init__(ID,expression): None
        returns nothing, but sets the children of ASTAssignmentExpr to ASTID(ID) and expression
    binding: self.children[0]
        returns the ASTID node representing the variable to be assigned to
    value: self.children[1]
        returns the AST node representing the value of the variable
    
    Examples
    --------
    >>> a = ASTAssignmentExpr('var1',ASTNode())
    >>> a.binding.__class__.__name__
    'ASTID'
    >>> a.value.__class__.__name__
    'ASTNode'
  """
  def __init__(self,ID,expression):
    super().__init__()
    self.children = [ASTID(ID), expression]
  @property
  def binding(self): 
    return self.children[0]
  @property
  def value(self): 
    return self.children[1]

class ASTEvalExpr(ASTNode): 
  """
    A class that takes an operator and its arguments as its arguments 
    and defines an expression evaluation node in an abstract syntax tree
    
    Parameters
    ----------
    oper: some function represented as a string
    argms: a list of AST nodes
    
    Returns
    -------
    __init__(oper,argms): None
        returns nothing, but sets the children of ASTEvalExpr to ASTID(oper) and argms
    op: self.children[0]
        returns the ASTID node representing the operator/function
    args: self.children[1:]
        returns the list of AST nodes representing the arguments of the operator/function
    
    Examples
    --------
    >>> a = ASTEvalExpr(ASTID('op'),[ASTNode(),ASTNode()])
    >>> a.op.__class__.__name__
    'ASTID'
    >>> [child.__class__.__name__ for child in a.args]
    ['ASTNode', 'ASTNode']
    >>> a = ASTEvalExpr(ASTID('op2'),[])
    >>> [child.__class__.__name__ for child in a.children]
    ['ASTID']
  """
  def __init__(self,oper,argms):
    super().__init__()
    if len(argms) > 0:
      self.children = [oper,*argms]
    else:
      self.children = [oper]
  @property
  def op(self): 
    return self.children[0]
  @property
  def args(self): 
    return self.children[1:]

class ASTID(ASTNode):
  """
    A class that takes a name and a declaration of its type as its arguments 
    and defines an ID node in an abstract syntax tree
    
    Parameters
    ----------
    name: some string
    typedecl: some string
    
    Returns
    -------
    __init__(name,typedecl): None
        returns nothing, but sets the name attribute of the node to name and the type attribute of the node to typedecl 
    
    Examples
    --------
    >>> a = ASTID('Joe','component')
    >>> a.__class__.__name__
    'ASTID'
    >>> a.name
    'Joe'
    >>> a.type
    'component'
  """
  def __init__(self, name, typedecl=None):
    super().__init__()
    self.name = name
    self.type = typedecl

class ASTLiteral(ASTNode):
  """
    A class that takes a value as its argument
    and defines a literal node in an abstract syntax tree
    
    Parameters
    ----------
    value: some scalar
    
    Returns
    -------
    __init__(value): None
        returns nothing, but sets the value attribute of the node to value and the type attribute of the node to 'Scalar'
    
    Examples
    --------
    >>> a = ASTLiteral(5)
    >>> a.__class__.__name__
    'ASTLiteral'
    >>> a.value
    5
    >>> a.type
    'Scalar'
  """
  def __init__(self, value):
    super().__init__()
    self.value = value
    self.type = 'Scalar'

