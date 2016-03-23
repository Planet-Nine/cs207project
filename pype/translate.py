from .ast import *
from .symtab import *
from .lib_import import LibraryImporter

class SymbolTableVisitor(ASTVisitor):
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
      try:   # If it is already in the table, it's an output expression
        checkoutput = self.symbol_table[self._component][node.binding.name]
      except:
        self.symbol_table.addsym(Symbol(node.binding.name, SymbolType.var, None), self._component)

    if isinstance(node, ASTInputExpr):
      if len(node.children) > 0:
        for child in node.children:
          self.symbol_table.addsym(Symbol(child.name, SymbolType.input, None), self._component)

    if isinstance(node, ASTOutputExpr):  # overwrite any assignment expressions
      if len(node.children) > 0:
        for child in node.children:
          self.symbol_table.addsym(Symbol(child.name, SymbolType.output, None), self._component)
