import importlib
import inspect
import functools

from .symtab import *

ATTRIB_COMPONENT = '_pype_component'

def component(func):
  """
  Decorator which marks a function as compatible for exposing as a component in PyPE.

  Parameters
  ----------
  func : function
    The function intended for use as a PyPE component

  Returns
  -------
  function
    The marked function, now suitable for PyPE use

  Examples
  --------
  >>> def f(x): return x + 1
  >>> def g(x): return x + 2
  >>> f = component(f)
  >>> is_component(f)
  True
  >>> is_component(g)
  False
  """
  func._attributes = {}
  func._attributes['_pype_component'] = True
  return func

def is_component(func):
  """
  Checks whether the @component decorator was applied to a function.

  Parameters
  ----------
  func : function
    The function to be examined

  Returns
  -------
  bool
    True if the decorator was applied, False if not

  Examples
  --------
  See component().
  """
  try:
    if func._attributes['_pype_component']: return True
    else: return False
  except:
    return False

class LibraryImporter(object):
  """
  An object which imports library functions into a PyPE symbol table.

  Parameters
  ----------
  modname : string (optional)
    The name of the module from which to import (e.g. numpy)
  
  Attributes
  ----------
  mod : module
     The current module being imported

  Examples
  --------
  >>> li = LibraryImporter()
  >>> li.import_module('timeseries')
  >>> st = SymbolTable()
  >>> st = li.add_symbols(st)
  >>> 'mean' in st['global']
  True
  """
  def __init__(self, modname=None):
    self.mod = None
    if modname is not None:
      self.import_module(modname)

  def import_module(self, modname):
    """
    Set the next module to be imported

    Parameters
    ----------
    modname : string
      The name of the module from which to import (e.g. numpy)
    """
    self.mod = importlib.import_module(modname)

  def add_symbols(self, symtab):
    """
    Adds any component-decorated functions in the current module to a symbol table

    Parameters
    ----------
    symtab : SymbolTable
      The table to which the functions should be added

    Returns
    -------
    SymbolTable
      The same table with the module functions included
    """
    assert self.mod is not None, 'No module specified or loaded'

    for (name,obj) in inspect.getmembers(self.mod):

      if inspect.isroutine(obj) and is_component(obj):
        symtab.addsym( Symbol(name, SymbolType.libraryfunction, obj) )

      elif inspect.isclass(obj):
        for (methodname,method) in inspect.getmembers(obj):
          if inspect.isroutine(method) and is_component(method):
            symtab.addsym( Symbol(methodname, SymbolType.librarymethod, method) )

    return symtab
