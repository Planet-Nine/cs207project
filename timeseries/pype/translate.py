from .ast import *
from .symtab import *
from .lib_import import LibraryImporter
from .fgir import FGNodeType, FGNode, Flowgraph, FGIR
from .error import *


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
      self._component = node.name.name

    if isinstance(node, ASTAssignmentExpr):
        self.symbol_table.addsym(Symbol(node.binding.name, SymbolType.var, None), self._component)

    if isinstance(node, ASTInputExpr):
      if len(node.children) > 0:
        for child in node.children:
          self.symbol_table.addsym(Symbol(child.name, SymbolType.input, None), self._component)
class LoweringVisitor(ASTModVisitor):
  'Produces FGIR from an AST.'
  def __init__(self,symtab):
    self.symtab = symtab
    self.ir = FGIR()
    self.current_component = None

  def visit(self, astnode):
    if isinstance(astnode, ASTComponent):
      name = astnode.name.name
      self.ir[name] = Flowgraph(name=name)
      self.current_component = name
    return astnode

  def post_visit(self, node, visit_value, child_values):
    if isinstance(node, ASTProgram):
      return self.ir

    elif isinstance(node, ASTInputExpr):
      fg = self.ir[self.current_component]
      for child_v in child_values:
        varname = child_v.name
        var_nodeid = fg.get_var(varname)
        if var_nodeid is None: # No use yet, declare it.
          var_nodeid = fg.new_node(FGNodeType.input).nodeid
        else: # use before declaration
          fg.nodes[var_nodeid].type = FGNodeType.input
        fg.set_var(varname,var_nodeid)
        fg.add_input(var_nodeid)
      return None

    elif isinstance(node, ASTOutputExpr):
      fg = self.ir[self.current_component]
      for child_v in child_values:
        n = fg.new_node(FGNodeType.output)
        varname = child_v.name
        var_nodeid = fg.get_var(varname)
        if var_nodeid is None: # Use before declaration
          # The "unknown" type will be replaced later
          var_nodeid = fg.new_node(FGNodeType.unknown).nodeid
          fg.set_var(varname, var_nodeid)
        # Already declared in an assignment or input expression
        n.inputs.append(var_nodeid)
        fg.add_output(n.nodeid)
      return None

    elif isinstance(node, ASTAssignmentExpr):
      fg = self.ir[self.current_component]
      # If a variable use precedes its declaration, a stub will be in this table
      stub_nodeid = fg.get_var(node.binding.name)
      if stub_nodeid is not None: # Modify the existing stub
        n = fg.nodes[stub_nodeid]
        n.type = FGNodeType.assignment
      else: # Create a new node
        n = fg.new_node(FGNodeType.assignment)
      child_v = child_values[1]
      if isinstance(child_v, FGNode): # subexpressions or literals
        n.inputs.append(child_v.nodeid)
      elif isinstance(child_v, ASTID): # variable lookup
        varname = child_v.name
        var_nodeid = fg.get_var(varname)
        if var_nodeid is None: # Use before declaration
          # The "unknown" type will be replaced later
          var_nodeid = fg.new_node(FGNodeType.unknown).nodeid
          fg.set_var(varname, var_nodeid)
        # Already declared in an assignment or input expression
        n.inputs.append(var_nodeid)
      fg.set_var(node.binding.name, n.nodeid)
      return None

    elif isinstance(node, ASTEvalExpr):
      fg = self.ir[self.current_component]
      op = self.symtab.lookupsym(node.op.name, scope=self.current_component)
      if op is None:
        raise PypeSyntaxError('Undefined operator: '+str(node.op.name))
      if op.type==SymbolType.component:
        n = fg.new_node(FGNodeType.component)
      elif op.type==SymbolType.libraryfunction:
        n = fg.new_node(FGNodeType.libraryfunction, ref=op.ref)
      elif op.type==SymbolType.librarymethod:
        n = fg.new_node(FGNodeType.librarymethod, ref=op.ref)
      else:
        raise PypeSyntaxError('Invalid operator of type "'+str(SymbolType)+'" in expression: '+str(node.op.name))

      n.inputs = []
      for child_v in child_values[1:]:
        if isinstance(child_v, FGNode): # subexpressions or literals
          n.inputs.append(child_v.nodeid)
        elif isinstance(child_v, ASTID): # variable lookup
          varname = child_v.name
          var_nodeid = fg.get_var(varname)
          if var_nodeid is None: # Use before declaration
            # The "unknown" type will be replaced later
            var_nodeid = fg.new_node(FGNodeType.unknown).nodeid
            fg.set_var(varname, var_nodeid)
          # Already declared in an assignment or input expression
          n.inputs.append(var_nodeid)
      return n

    elif isinstance(node, ASTLiteral):
      fg = self.ir[self.current_component]
      n = fg.new_node(FGNodeType.literal, ref=node.value)
      return n

    else:
      return visit_value
 
