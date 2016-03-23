import unittest
import pype
import timeseries
import io
import sys
from contextlib import redirect_stdout

class MyTest(unittest.TestCase):

    def test_astlexpar(self):
        data = open("samples/example1.ppl").read()
        ast = pype.parser.parser.parse(data, lexer=pype.lexer.lexer)
        printer = pype.semantic_analysis.PrettyPrint()
        f = io.StringIO()
        with redirect_stdout(f):
             ast.walk(printer)
        compare = open("samples/example1.ast").read()
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
        data = open("samples/example1.ppl").read()
        ast = pype.parser.parser.parse(data, lexer=pype.lexer.lexer)
        tabler = pype.translate.SymbolTableVisitor()
        ast.walk(tabler)
        symtab = tabler.symbol_table
        self.assertListEqual(sorted(list(symtab.scopes())), sorted(['global', 'standardize']))
        self.assertEqual(len(symtab['global']), 3)
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

suite = unittest.TestLoader().loadTestsFromModule(MyTest())
unittest.TextTestRunner().run(suite)
