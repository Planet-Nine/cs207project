from .ast import *

class PrettyPrint(ASTVisitor):
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
