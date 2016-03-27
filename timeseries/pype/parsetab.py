
# parsetab.py
# This file is automatically generated. Do not edit.
_tabversion = '3.8'

_lr_method = 'LALR'

_lr_signature = 'A8243AB58ECAD8DA279AFAE7F345FE96'
    
_lr_action_items = {'$end':([2,3,4,5,8,9,18,28,],[-5,-1,0,-4,-3,-2,-7,-6,]),'RBRACE':([11,12,13,14,16,19,31,39,40,44,48,50,51,52,53,55,57,],[-28,-27,18,-9,-26,-8,-13,-11,-21,-12,-25,-24,-23,-10,-20,-22,-19,]),'LBRACE':([0,2,3,5,8,9,18,28,],[1,-5,1,-4,-3,-2,-7,-6,]),'RPAREN':([11,12,16,17,20,24,25,29,30,31,33,34,35,36,37,38,39,40,41,43,44,45,48,49,50,51,52,53,54,55,56,57,58,],[-28,-27,-26,28,31,39,40,44,-17,-13,-15,-30,48,50,51,52,-11,-21,53,55,-12,-14,-25,-29,-24,-23,-10,-20,57,-22,58,-19,-16,]),'OP_DIV':([15,],[21,]),'OP_MUL':([15,],[22,]),'NUMBER':([7,11,12,13,14,16,19,21,22,23,25,27,31,34,35,36,37,39,40,41,42,43,44,48,49,50,51,52,53,55,57,],[12,-28,-27,12,-9,-26,-8,12,12,12,12,12,-13,-30,12,12,12,-11,-21,12,12,12,-12,-25,-29,-24,-23,-10,-20,-22,-19,]),'STRING':([7,11,12,13,14,16,19,21,22,23,25,27,31,34,35,36,37,39,40,41,42,43,44,48,49,50,51,52,53,55,57,],[11,-28,-27,11,-9,-26,-8,11,11,11,11,11,-13,-30,11,11,11,-11,-21,11,11,11,-12,-25,-29,-24,-23,-10,-20,-22,-19,]),'OP_SUB':([15,],[23,]),'OP_ADD':([15,],[27,]),'INPUT':([15,],[24,]),'OUTPUT':([15,],[20,]),'ID':([1,7,10,11,12,13,14,15,16,19,20,21,22,23,24,25,26,27,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,55,57,58,],[7,16,17,-28,-27,16,-9,25,-26,-8,30,16,16,16,30,16,42,16,30,-17,-13,47,-15,-30,16,16,16,30,-11,-21,16,16,16,-12,-14,56,-18,-25,-29,-24,-23,-10,-20,-22,-19,-16,]),'IMPORT':([6,],[10,]),'ASSIGN':([15,],[26,]),'LPAREN':([0,2,3,5,7,8,9,11,12,13,14,16,18,19,20,21,22,23,24,25,27,28,29,30,31,33,34,35,36,37,38,39,40,41,42,43,44,45,48,49,50,51,52,53,55,57,58,],[6,-5,6,-4,15,-3,-2,-28,-27,15,-9,-26,-7,-8,32,15,15,15,32,15,15,-6,32,-17,-13,-15,-30,15,15,15,32,-11,-21,15,15,15,-12,-14,-25,-29,-24,-23,-10,-20,-22,-19,-16,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'expression_list':([7,],[13,]),'expression':([7,13,21,22,23,25,27,35,36,37,41,42,43,],[14,19,34,34,34,34,34,49,49,49,49,54,49,]),'parameter_list':([21,22,23,25,27,],[35,36,37,41,43,]),'component':([0,3,],[2,9,]),'statement_list':([0,],[3,]),'program':([0,],[4,]),'declaration_list':([20,24,],[29,38,]),'type':([32,],[46,]),'import_statement':([0,3,],[5,8,]),'declaration':([20,24,29,38,],[33,33,45,45,]),}

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
   for _x, _y in zip(_v[0], _v[1]):
       if not _x in _lr_goto: _lr_goto[_x] = {}
       _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
  ("S' -> program","S'",1,None,None,None),
  ('program -> statement_list','program',1,'p_program','parser.py',8),
  ('statement_list -> statement_list component','statement_list',2,'p_statement_list','parser.py',13),
  ('statement_list -> statement_list import_statement','statement_list',2,'p_statement_list','parser.py',14),
  ('statement_list -> import_statement','statement_list',1,'p_statement_list','parser.py',15),
  ('statement_list -> component','statement_list',1,'p_statement_list','parser.py',16),
  ('import_statement -> LPAREN IMPORT ID RPAREN','import_statement',4,'p_import_statement','parser.py',24),
  ('component -> LBRACE ID expression_list RBRACE','component',4,'p_component','parser.py',28),
  ('expression_list -> expression_list expression','expression_list',2,'p_expression_list','parser.py',32),
  ('expression_list -> expression','expression_list',1,'p_expression_list','parser.py',33),
  ('expression -> LPAREN INPUT declaration_list RPAREN','expression',4,'p_input','parser.py',41),
  ('expression -> LPAREN INPUT RPAREN','expression',3,'p_input','parser.py',42),
  ('expression -> LPAREN OUTPUT declaration_list RPAREN','expression',4,'p_output','parser.py',49),
  ('expression -> LPAREN OUTPUT RPAREN','expression',3,'p_output','parser.py',50),
  ('declaration_list -> declaration_list declaration','declaration_list',2,'p_declaration_list','parser.py',57),
  ('declaration_list -> declaration','declaration_list',1,'p_declaration_list','parser.py',58),
  ('declaration -> LPAREN type ID RPAREN','declaration',4,'p_declaration','parser.py',66),
  ('declaration -> ID','declaration',1,'p_declaration','parser.py',67),
  ('type -> ID','type',1,'p_type','parser.py',74),
  ('expression -> LPAREN ASSIGN ID expression RPAREN','expression',5,'p_assign','parser.py',78),
  ('expression -> LPAREN ID parameter_list RPAREN','expression',4,'p_funcexpr','parser.py',82),
  ('expression -> LPAREN ID RPAREN','expression',3,'p_funcexpr','parser.py',83),
  ('expression -> LPAREN OP_ADD parameter_list RPAREN','expression',4,'p_op','parser.py',90),
  ('expression -> LPAREN OP_SUB parameter_list RPAREN','expression',4,'p_op','parser.py',91),
  ('expression -> LPAREN OP_MUL parameter_list RPAREN','expression',4,'p_op','parser.py',92),
  ('expression -> LPAREN OP_DIV parameter_list RPAREN','expression',4,'p_op','parser.py',93),
  ('expression -> ID','expression',1,'p_exprid','parser.py',97),
  ('expression -> NUMBER','expression',1,'p_literal','parser.py',101),
  ('expression -> STRING','expression',1,'p_literal','parser.py',102),
  ('parameter_list -> parameter_list expression','parameter_list',2,'p_parameter_list','parser.py',106),
  ('parameter_list -> expression','parameter_list',1,'p_parameter_list','parser.py',107),
]
