class ASTVisitor():
  def visit(self, astnode):
    'A read-only function which looks at a single AST node.'
    pass
  def return_value(self):
    return None

class ASTNode(object):
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
    # TODO
    pfx = indent
    if isinstance(self, ASTNode):
        printed = ''.join([pfx,self.__class__.__name__,'('])
        print(printed)

        if any(isinstance(child, ASTNode) for child in self.children):
            for i, child in enumerate(self.children):
                if i != 0:
                    printed += ','
                printed += '\n'
                printed += child.pprint(indent+'  ')
            printed += ')'
            return printed
        else:
            # None of the children as nodes, simply join their repr on a single
            # line.
            printed += ', '.join(repr(child) for child in self.children)+')'
            return printed


    else:
        printed = pfx + repr(self)
        return printed

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

class ASTProgram(ASTNode):
  def __init__(self, statements):
    super().__init__()
    self.children = statements

class ASTImport(ASTNode):
  def __init__(self, mod):
    super().__init__()
    self.mod = mod
  @property
  def module(self):
    return self.mod

class ASTComponent(ASTNode): # TODO
  def __init__(self,name,expressions):
    super().__init__()
    self.children = [ASTID(name),*expressions]
    
  @property
  def name(self): # TODO return an element of self.children
    return self.children[0]
  @property
  def expressions(self): # TODO return one or more children
    return self.children[1:]

class ASTInputExpr(ASTNode): # TODO
  def __init__(self,declaration_list=None):
    super().__init__()
    self.children = declaration_list

class ASTOutputExpr(ASTNode): # TODO
  def __init__(self,declaration_list=None):
    super().__init__()
    self.children = declaration_list

# class ASTAssignmentExpr(ASTNode): # TODO
#   def __init__(self,ID,expression):
#     super().__init__()
#     self.children = [ID, expression]
#   @property
#   def binding(self): # TODO
#     return self.children[0]
#   @property
#   def value(self): # TODO
#     return self.children[1]

class ASTAssignmentExpr(ASTNode): # TODO
  def __init__(self,ID,expression):
    super().__init__()
    self.children = [ASTID(ID), expression]
  @property
  def binding(self): # TODO
    return self.children[0]
  @property
  def value(self): # TODO
    return self.children[1]

# class ASTEvalExpr(ASTNode): # TODO
#   def __init__(self,oper,argms):
#     super().__init__()
#     self.children = [oper,*argms]
#   @property
#   def op(self): # TODO
#     return self.children[0]
#   @property
#   def args(self): # TODO
#     return self.children[1:]
 
class ASTEvalExpr(ASTNode): # TODO
  def __init__(self,oper,argms):
    super().__init__()
    self.children = [ASTID(oper),*argms]
  @property
  def op(self): # TODO
    return self.children[0]
  @property
  def args(self): # TODO
    return self.children[1:]

# These are already complete.
class ASTID(ASTNode):
  def __init__(self, name, typedecl=None):
    super().__init__()
    self.name = name
    self.type = typedecl

class ASTLiteral(ASTNode):
  def __init__(self, value):
    super().__init__()
    self.value = value
    self.type = 'Scalar'

