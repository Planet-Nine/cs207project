import ply.lex

reserved = { # pattern : token-name
  'input' : 'INPUT',
  'output' : 'OUTPUT',
  'import' : 'IMPORT',
}

tokens = [ 
  'LPAREN','RPAREN', # Individual parentheses
  'LBRACE','RBRACE', # Individual braces
  'OP_ADD','OP_SUB','OP_MUL','OP_DIV', # the four basic arithmetic symbols
  'STRING', # Anything enclosed by double quotes
  'ASSIGN', # The two characters :=
  'NUMBER', # An arbitrary number of digits
  'ID', # a sequence of letters, numbers, and underscores. Must not start with a number.
] + list(reserved.values())

# Token specifications here.
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_LBRACE = r'\{'
t_RBRACE = r'\}'
t_OP_ADD = r'\+'
t_OP_SUB = r'-'
t_OP_MUL = r'\*'
t_OP_DIV = r'/'
t_STRING = r'".*"'
t_ASSIGN = r':='
t_NUMBER = r'\d+'

# Ignore whitespace
t_ignore  = ' \t\r\f\v'

# A rule for IDs and reserved keywords
def t_ID(t):
    r'[a-zA-Z_][a-zA-Z_0-9]*'
    t.type = reserved.get(t.value,'ID')    # Check for reserved words
    return t

# Ignore comments
def t_COMMENT(t):
    r'\#.*'
    pass

# A rule for newlines that tracks line numbers. 
def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)

# Error-handling routine, which prints line and column numbers
def t_error(t):
    print("Illegal character '%s' at line %d, column %d" % (t.value[0], t.lexer.lineno, find_column(t.lexer.lexdata, t)))
    t.lexer.skip(1)

def find_column(input,token):
    last_cr = input.rfind('\n',0,token.lexpos)
    if last_cr < 0:
        last_cr = 0
    column = (token.lexpos - last_cr) 
    return column

# This actually builds the lexer.
lexer = ply.lex.lex()
