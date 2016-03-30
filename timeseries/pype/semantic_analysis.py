from .ast import *

class PrettyPrint(ASTVisitor):
  """
    A class that takes no arguments and defines a printing visitor for an abstract syntax tree
    
    Parameters
    ----------
   
    Returns
    -------
    visit(node): 
        returns nothing, but prints out the class name of a given node
    
    Examples
    --------
    >>> a,b,c = ASTNode(),ASTNode(),ASTNode()
    >>> a.children = [b,c]
    >>> d = PrettyPrint()
    >>> a.walk(d)
    ASTNode
      ASTNode
      ASTNode
  """
  def __init__(self):
    pass
  def visit(self, node):
    u = node
    s = ''
    while u.parent:
        s += '  '
        u = u.parent
    print(''.join([s,node.__class__.__name__]))

class CheckSingleAssignment(ASTVisitor):
  """
    A class that takes no arguments and defines a checking single assignment visitor for an abstract syntax tree
    
    Parameters
    ----------
   
    Returns
    -------
    __init__: 
        returns nothing, but sets the pairs attribute of the visitor to [], the component attribute of the visitor to None
        and the components attribute of the visitor to []
    
    Examples
    --------
    >>> e,f = ASTNode(),ASTNode()
    >>> c,b = ASTComponent('comp1',[e]),ASTComponent('comp1',[f])
    >>> a = ASTProgram([c,b])
    >>> d = CheckSingleAssignment()
    >>> a.walk(d)
    Traceback (most recent call last):
        ...
    SyntaxError: Multiple assignment of component 'comp1' is not supported
    >>> e,f = ASTAssignmentExpr('a',ASTNode()),ASTInputExpr([ASTID('a','string')])
    >>> c = ASTComponent('comp1',[e,f])
    >>> d = CheckSingleAssignment()
    >>> c.walk(d)
    Traceback (most recent call last):
        ...
    SyntaxError: Multiple assignment of 'a' in component 'comp1' is not supported
  """
  def __init__(self):
    self.pairs = []
    self.component = None
    self.components = []

  def visit(self, node):
    if isinstance(node,ASTComponent):
        self.component = node.name
        if self.component in self.components:
            raise SyntaxError("Multiple assignment of component '%s' is not supported"%self.component)
        else:
            self.components += [self.component] 
        self.pairs = [(self.component,self.component)]
    if isinstance(node,ASTAssignmentExpr):
        pair = (node.binding.name,self.component)
        if pair in self.pairs:
            raise SyntaxError("Multiple assignment of '%s' in component '%s' is not supported"%pair)
        else:
            self.pairs += [pair]     
    if isinstance(node,ASTInputExpr):
        for child in node.children:
            pair = (child.name,self.component)
            if pair in self.pairs:
                raise SyntaxError("Multiple assignment of '%s' in component '%s' is not supported"%pair)
            else:
                self.pairs += [pair] 
class CheckSingleIOExpression(ASTVisitor):
  def __init__(self):
    self.component = None
    self.component_has_input = False
    self.component_has_output = False

  def visit(self, node):
    if isinstance(node, ASTComponent):
      self.component = node.name.name
      self.component_has_input = False
      self.component_has_output = False
    elif isinstance(node, ASTInputExpr):
      if self.component_has_input:
        raise PypeSyntaxError('Component '+str(self.component)+' has multiple input expressions')
      self.component_has_input = True
    elif isinstance(node, ASTOutputExpr):
      if self.component_has_output:
        raise PypeSyntaxError('Component '+str(self.component)+' has multiple output expressions')
      self.component_has_output = True

class CheckUndefinedVariables(ASTVisitor):
  def __init__(self, symtab):
    self.symtab = symtab
    self.scope=None

  def visit(self, node):
    if isinstance(node, ASTComponent):
      self.scope = node.name.name
    elif isinstance(node, ASTID):
      if self.symtab.lookupsym(node.name, scope=self.scope) is None:
        raise PypeSyntaxError('Undefined variable: '+str(node.name))

