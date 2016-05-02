import collections
import enum

SymbolType = enum.Enum('SymbolType', 'component var input output libraryfunction librarymethod')
Symbol = collections.namedtuple('Symbol','name type ref')

class SymbolTable(object):
  """
  A container which keeps track of the named objects in an AST and their scopes
    
  Parameters
  ----------
  None
  
  Attributes
  ----------
  T : dict
     Key = scope, Value = dict mapping names to Symbols within that scope

  Examples
  --------
  >>> st = SymbolTable()
  >>> list(st.scopes())[0]
  'global'
  >>> st.addsym(Symbol('x', SymbolType.var, None))
  >>> st['global']
  {'x': Symbol(name='x', type=<SymbolType.var: 2>, ref=None)}
  >>> st.addscope('newfunc')
  >>> st.addsym(Symbol('y', SymbolType.input, None), scope='newfunc')
  >>> st['newfunc']
  {'y': Symbol(name='y', type=<SymbolType.input: 3>, ref=None)}
  """
  def __init__(self):
    self.T = {} # {scope: {name:str => {type:SymbolType => ref:object} }}
    self.T['global'] = {}

  def __getitem__(self, component):
    """
    Fetches the mapping for the given scope

    Parameters
    ----------
    component : str
      Scope name, e.g. 'global'

    Returns
    -------
    dict
      Key = name (string), Value = Symbol
    """
    return self.T[component]

  def scopes(self):
    """
    Lists the scopes in the AST

    Returns
    -------
    dict_keys
      List of scope names
    """
    return self.T.keys()

  def __repr__(self):
    return str(self.T)

  def pprint(self):
    """
    Prints a human-readable version of the symbol table
    """
    print('---SYMBOL TABLE---')
    for (scope,table) in self.T.items():
      print(scope)
      for (name,symbol) in table.items():
        print(' ',name,'=>',symbol)

  def addsym(self, sym, scope='global'):
    """
    Adds a symbol to the table at the given scope

    Parameters
    ----------
    sym : Symbol
      The Symbol object to be added
    scope : string (optional)
      The scope to which the symbol belongs, global by default
    """
    try:
      self.T[scope][sym.name] = sym
    except:
      self.addscope(scope)
      self.T[scope][sym.name] = sym

  def addscope(self, scope):
    """
    Creates a new scope in the symbol table.

    Parameters
    ----------
    scope : string 
      The name of the new scope
    """
    self.T[scope] = {}
  def lookupsym(self, sym, scope=None):
    if scope is not None:
      if sym in self.T[scope]:
        return self.T[scope][sym]
    if sym in self.T['global']:
      return self.T['global'][sym]
    return None 
