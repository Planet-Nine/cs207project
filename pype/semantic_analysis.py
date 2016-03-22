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
    # TODO

class CheckSingleAssignment(ASTVisitor):
  def __init__(self):
    self.pairs = []
    self.component = None
    pass # TODO
  def visit(self, node):
    if isinstance(node,ASTComponent):
        self.component = node.name.name
        self.pairs = []
    if isinstance(node,ASTAssignmentExpr):
        pair = (node.binding.name,self.component)
        if pair in self.pairs:
            raise SyntaxError("Multiple assignment of '%s' in component '%s' is not supported"%(pair[0],pair[1]))
        else:
            self.pairs += [pair]            
    # TODO