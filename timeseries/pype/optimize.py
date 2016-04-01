from .fgir import *
from .error import *

# Optimization pass interfaces

class Optimization(object):
  def visit(self, obj): pass

class FlowgraphOptimization(Optimization):
  '''Called on each flowgraph in a FGIR.

  May modify the flowgraph by adding or removing nodes (return a new Flowgraph).
  If you modify nodes, make sure inputs, outputs, and variables are all updated.
  May NOT add or remove flowgraphs.'''
  pass

class NodeOptimization(Optimization):
  '''Called on each node in a FGIR.

  May modify the node (return a new Node object, and it will be assigned).
  May NOT remove or add nodes (use a component pass).'''
  pass

class TopologicalNodeOptimization(NodeOptimization): pass

# Optimization pass implementations

class PrintIR(TopologicalNodeOptimization):
  'A simple "optimization" pass which can be used to debug topological sorting'
  def visit(self, node):
    print(str(node))

class AssignmentEllision(FlowgraphOptimization):
  '''Eliminates all assignment nodes.

  Assignment nodes are useful for the programmer to reuse the output of an
  expression multiple times, and the lowering transformation generates explicit
  flowgraph nodes for these expressions. However, they are not necessary for
  execution, as they simply forward their value. This removes them and connects
  their pre- and post-dependencies.'''

  def visit(self, flowgraph):
    nodes = flowgraph.nodes
    nodestoremove = []
    for n in nodes:
      if nodes[n].type == FGNodeType.assignment:
        inputs = nodes[n].inputs[0]
        for varname, varnode in flowgraph.variables.items():
          if varnode == n:
            flowgraph.variables[varname] = inputs
        for nf in nodes:
          if (len(nodes[nf].inputs) > 0) and (n in nodes[nf].inputs):
            nodes[nf].inputs.remove(n)
            nodes[nf].inputs.append(inputs)
        nodestoremove.append(n)
    for n in nodestoremove:
      del nodes[n]
    return flowgraph

class DeadCodeElimination(FlowgraphOptimization):
  '''Eliminates unreachable expression statements.

  Statements which never affect any output are effectively useless, and we call
  these "dead code" blocks. This optimization removes any expressions which can
  be shown not to affect the output.
  NOTE: input statements *cannot* safely be removed, since doing so would change
  the call signature of the component. For example, it might seem that the input
  x could be removed:
    { component1 (input x y) (output y) }
  but imagine this component1 was in a file alongside this one:
    { component2 (input a b) (:= c (component1 a b)) (output c) }
  By removing x from component1, it could no longer accept two arguments. So in
  this instance, component1 will end up unmodified after DCE.'''

  def visit(self, flowgraph):
    # TODO: implement this
    outputs = flowgraph.outputs
    inputs = flowgraph.inputs
    keep_vars = []
    keep_ids = []
    lose_vars = []
    lose_ids = []
    for inpt in inputs:
        keep_ids.append(inpt)
    for output in outputs:
        keep_ids.append(output)
        self.search(flowgraph,output,keep_ids)
    for iden in keep_ids:
        var = [key for key, value in flowgraph.variables.items() if value == iden]
        if var:
            var = var[0]
        keep_vars.append(var)
    for var in flowgraph.variables:
        if var not in keep_vars:
            lose_vars.append(var)
    for var in lose_vars:
        del flowgraph.variables[var]
    for iden in flowgraph.nodes:
        if iden not in keep_ids:
            lose_ids.append(iden)
    for iden in lose_ids:
        del flowgraph.nodes[iden]
    return flowgraph
  
  def search(self,flowgraph,nodeid,keep_ids):
    node = flowgraph.nodes[nodeid]
    for nodeid2 in node.inputs:
        keep_ids.append(nodeid2)
        self.search(flowgraph,nodeid2,keep_ids)
