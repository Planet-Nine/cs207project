from .ast import *
from .symtab import *
from .lib_import import LibraryImporter

class SymbolTableVisitor(ASTVisitor):
  """
  A class defining a visitor which constructs a symbol table for an abstract syntax tree 
    
  Parameters
  ----------
  None
  
  Attributes
  ----------
  symbol_table : SymbolTable
     The symbol table corresponding to the visited AST

  Examples
  --------
  >>> e,f = ASTNode(),ASTNode()
  >>> c,b = ASTComponent('comp1',[e]),ASTComponent('comp2',[f])
  >>> a = ASTProgram([c,b])
  >>> d = SymbolTableVisitor()
  >>> st = a.walk(d)
  >>> len(st.scopes())
  3
 
  """
  def __init__(self):
    self.symbol_table = SymbolTable()
    self._component = None

  def return_value(self):
    return self.symbol_table

  def visit(self, node):
    if isinstance(node, ASTImport):
      # Import statements make library functions available to PyPE
      imp = LibraryImporter(node.module)
      imp.add_symbols(self.symbol_table)

    if isinstance(node, ASTComponent):
      self.symbol_table.addsym(Symbol(node.name, SymbolType.component, None))
      self.symbol_table.addscope(node.name)
      self._component = node.name

    if isinstance(node, ASTAssignmentExpr):
        self.symbol_table.addsym(Symbol(node.binding.name, SymbolType.var, None), self._component)

    if isinstance(node, ASTInputExpr):
      if len(node.children) > 0:
        for child in node.children:
          self.symbol_table.addsym(Symbol(child.name, SymbolType.input, None), self._component)

 
