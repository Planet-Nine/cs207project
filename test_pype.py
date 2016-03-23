import pype
import timeseries

data = open("example1.ppl").read()
ast = pype.parser.parser.parse(data, lexer=pype.lexer.lexer)
printer = pype.semantic_analysis.PrettyPrint()
ast.walk(printer)
checker = pype.semantic_analysis.CheckSingleAssignment()
ast.walk(checker)
tabler = pype.translate.SymbolTableVisitor()
ast.walk(tabler)
tabler.symbol_table.pprint()

@pype.component
def sillyfunc(a):
    print(a)

print(sillyfunc._attributes['_pype_component'])   # True
print(pype.is_component(sillyfunc))  # True

def sillyfunc2(b):
    print(b)

print(pype.is_component(sillyfunc2))  # False
