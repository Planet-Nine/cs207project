import unittest
from timeseries import pype
# import .timeseries
import io
import sys
from contextlib import redirect_stdout

class MyTest(unittest.TestCase):

    def test_astlexpar(self):
        data = open("./samples/example1.ppl").read()
        ast = pype.parser.parser.parse(data, lexer=pype.lexer.lexer)
        printer = pype.semantic_analysis.PrettyPrint()
        f = io.StringIO()
        with redirect_stdout(f):
             ast.walk(printer)
        compare = open("./samples/example1.ast").read()
        self.assertEqual(f.getvalue(), compare)

    def test_singleassignment(self):
        data = '''(import timeseries)

        { standardize
        (:= new_t (/ (- t mu) sig))
        (:= mu (mean t))
        (:= sig (std t))
        
        (input (TimeSeries t))
        (output new_t)
        }'''
        ast = pype.parser.parser.parse(data, lexer=pype.lexer.lexer)
        checker = pype.semantic_analysis.CheckSingleAssignment()
        try:
            ast.walk(checker)
        except:
            self.fail("Single Assignment erroneously flagged")

        data = '''(import timeseries)

        { standardize
        (:= new_t (/ (- t mu) sig))
        (:= mu (mean t))
        (:= mu (std t))
        
        (input (TimeSeries t))
        (output new_t)
        }'''
        ast = pype.parser.parser.parse(data, lexer=pype.lexer.lexer)
        checker = pype.semantic_analysis.CheckSingleAssignment()
        with self.assertRaises(SyntaxError):
            ast.walk(checker)

        data = '''(import timeseries)

        { standardize
        (:= new_t (/ (- t mu) sig))
        (:= mu (mean t))
        (:= sig (std t))
        
        (input (TimeSeries t))
        (output new_t)
        }

        { standardize2
        (:= new_t (/ (- t mu) sig))
        (:= mu (mean t))
        (:= sig (std t))
        
        (input (TimeSeries t))
        (output new_t)
        }'''
        ast = pype.parser.parser.parse(data, lexer=pype.lexer.lexer)
        checker = pype.semantic_analysis.CheckSingleAssignment()
        try:
            ast.walk(checker)
        except:
            self.fail("Single Assignment erroneously flagged")

    def test_symtablevisitor(self):
        data = open("./samples/example1.ppl").read()
        ast = pype.parser.parser.parse(data, lexer=pype.lexer.lexer)
        tabler = pype.translate.SymbolTableVisitor()
        ast.walk(tabler)
        symtab = tabler.symbol_table
        self.assertListEqual(sorted(list(symtab.scopes())), sorted(['global', 'standardize']))
        self.assertEqual(len(symtab['global']), 11)
        self.assertEqual(len(symtab['standardize']), 4)

    def test_component(self):
        @pype.component
        def sillyfunc(a):
            print(a)
        self.assertEqual(sillyfunc._attributes['_pype_component'], True)
        self.assertEqual(pype.is_component(sillyfunc), True)
        def sillyfunc2(b):
            print(b)
        self.assertEqual(pype.is_component(sillyfunc2), False)
    
    def test_deadcodeelimination(self):
        data = """
        (import timeseries)

        { component2
          # sum of squares
          (input x y)
          (:= z (+ (* x x) (* y y)))
          (output z)
        }

        { six
          # Produces the number 6 through convoluted means
          (input x y)
          (:= a (+ x (* 2 y)))
          (:= b (+ (/ y x) (* x x)))
          (:= c 6)
          (:= d (component2 x y)) 
          (:= e (+ (* a a) (+ (* b b) d)))
          (output c)
        }
        """
        graph1 = 'digraph six {\n  "@N10" -> "@N11"\n  "@N11" -> "@N19"\n  "@N1" [ label = "y" ]\n  "@N0" [ label = "x" ]\n  "@N11" [ label = "c" ]\n  "@N0" [ color = "green" ]\n  "@N1" [ color = "green" ]\n  "@N19" [ color = "red" ]\n}\n'
        graph2 = 'digraph component2 {\n  "@N2" -> "@N4"\n  "@N3" -> "@N4"\n  "@N1" -> "@N3"\n  "@N1" -> "@N3"\n  "@N0" -> "@N2"\n  "@N0" -> "@N2"\n  "@N4" -> "@N5"\n  "@N5" -> "@N6"\n  "@N5" [ label = "z" ]\n  "@N0" [ label = "x" ]\n  "@N1" [ label = "y" ]\n  "@N0" [ color = "green" ]\n  "@N1" [ color = "green" ]\n  "@N6" [ color = "red" ]\n}\n'
        ast = pype.parser.parser.parse(data,pype.lexer.lexer)
        q = pype.translate.SymbolTableVisitor()
        ast.walk(q)
        IR = ast.mod_walk(pype.translate.LoweringVisitor(q.symbol_table))
        flowgraph = IR['six']
        flowgraph2 = IR['component2']
        eliminate = pype.optimize.DeadCodeElimination()
        flowgraph2 = eliminate.visit(flowgraph2)
        flowgraph = eliminate.visit(flowgraph)
        self.assertEqual(flowgraph.dotfile(),graph1)
        self.assertEqual(flowgraph2.dotfile(),graph2)

suite = unittest.TestLoader().loadTestsFromModule(MyTest())
unittest.TextTestRunner().run(suite)
