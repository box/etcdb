
# parsetab.py
# This file is automatically generated. Do not edit.
# pylint: disable=W,C,R
_tabversion = '3.10'

_lr_method = 'LALR'

_lr_signature = 'leftANDORrightUNOTAFTER AND AS ASC AUTOCOMMIT AUTO_INCREMENT BOOL BY COMMIT COUNT CREATE DATABASE DATABASES DATETIME DEFAULT DELETE DESC DROP EXISTS FROM FULL GREATER_OR_EQ IF IN INSERT INT INTEGER INTO IS KEY LESS_OR_EQ LIMIT LOCK LONGTEXT NAMES NOT NULL NUMBER N_EQ OR ORDER PRIMARY SELECT SET SHOW SMALLINT STRING STRING_VALUE TABLE TABLES TINYINT UNIQUE UNSIGNED UPDATE USE VALUES VARCHAR VERSION WAIT WHEREstatement : select_statement\n        | show_tables_statement\n        | create_table_statement\n        | create_database_statement\n        | show_databases_statement\n        | use_database_statement\n        | commit_statement\n        | set_statement\n        | insert_statement\n        | delete_statement\n        | drop_database_statement\n        | drop_table_statement\n        | desc_table_statement\n        | update_table_statement\n        | wait_statementwait_statement : WAIT select_item_list FROM identifier opt_WHERE opt_AFTERopt_AFTER : opt_AFTER : AFTER NUMBERupdate_table_statement : UPDATE identifier SET col_expr_list opt_WHERE opt_USE_LOCKcol_expr_list : col_exprcol_expr_list : col_expr_list \',\' col_exprcol_expr :  identifier \'=\' exprdesc_table_statement : DESC identifierdrop_database_statement : DROP DATABASE identifierdrop_table_statement : DROP TABLE identifier opt_IF_EXISTSopt_IF_EXISTS : opt_IF_EXISTS : IF EXISTSinsert_statement : INSERT INTO identifier opt_fieldlist VALUES \'(\' values_list \')\'opt_fieldlist : opt_fieldlist : \'(\' fieldlist \')\'fieldlist : identifierfieldlist : fieldlist \',\' identifier  values_list : valuevalues_list : values_list \',\' valueset_statement : set_autocommit_statement\n        | set_names_statementset_names_statement : SET NAMES STRINGset_autocommit_statement : SET AUTOCOMMIT \'=\' NUMBERcommit_statement : COMMITcreate_table_statement : CREATE TABLE identifier \'(\' create_definition_list \')\'create_database_statement : CREATE DATABASE identifiershow_databases_statement : SHOW DATABASESuse_database_statement : USE identifieridentifier : STRINGidentifier : \'`\' STRING \'`\'create_definition_list : create_definitioncreate_definition_list : create_definition_list \',\' create_definitioncreate_definition : identifier column_definitioncolumn_definition : data_type opt_column_def_options_listdata_type : INTEGER opt_UNSIGNED\n        | VARCHAR \'(\' NUMBER \')\'\n        | DATETIME\n        | DATETIME \'(\' NUMBER \')\'\n        | INT opt_UNSIGNED\n        | LONGTEXT\n        | SMALLINT opt_UNSIGNED\n        | TINYINT\n        | BOOLopt_UNSIGNED :\n        | UNSIGNEDopt_column_def_options_list : opt_column_def_options_list : opt_column_def_options opt_column_def_options_listopt_column_def_options : DEFAULT valueopt_column_def_options : NULLopt_column_def_options : NOT NULLopt_column_def_options : AUTO_INCREMENTopt_column_def_options : PRIMARY KEYopt_column_def_options : UNIQUEvalue : q_STRING\n        | NUMBER\n        | STRING_VALUE q_STRING : "\'" STRING "\'" q_STRING :  select_statement : SELECT select_item_list opt_FROM opt_WHERE opt_ORDER_BY opt_LIMITopt_ORDER_BY : opt_ORDER_BY : ORDER BY identifier opt_ORDER_DIRECTIONopt_ORDER_BY : ORDER BY identifier \'.\' identifier opt_ORDER_DIRECTIONopt_ORDER_DIRECTION : opt_ORDER_DIRECTION : ASC\n        | DESC opt_LIMIT : opt_LIMIT : LIMIT NUMBERshow_tables_statement : SHOW opt_FULL TABLESopt_FROM : opt_FROM : FROM table_referencetable_reference : identifiertable_reference : identifier \'.\' identifieropt_FULL : opt_FULL : FULLselect_item_list : select_item select_item_list : select_item_list \',\' select_item select_item_list : \'*\'select_item : select_item2 select_aliasselect_item2 : table_wild\n        | expr select_alias : select_alias : AS identifiertable_wild : identifier \'.\' \'*\' opt_USE_LOCK : opt_USE_LOCK : USE LOCK STRING_VALUE opt_WHERE : opt_WHERE : WHERE exprexpr : expr OR exprexpr : expr AND exprexpr : NOT expr %prec UNOTexpr : boolean_primaryboolean_primary : boolean_primary IS NULLboolean_primary : boolean_primary IS NOT NULLboolean_primary : boolean_primary comparison_operator predicateboolean_primary : predicatecomparison_operator : \'=\'\n        | GREATER_OR_EQ\n        | \'>\'\n        | LESS_OR_EQ\n        | \'<\'\n        | N_EQpredicate : bit_expr predicate : bit_expr IN \'(\' list_expr \')\'list_expr : exprlist_expr : list_expr \',\' exprbit_expr : simple_exprsimple_expr : identifiersimple_expr : identifier \'.\' identifiersimple_expr : \'(\' expr \')\'simple_expr : variablevariable : \'@\' \'@\' STRINGsimple_expr : literalliteral : q_STRING\n        | NUMBER\n        | STRING_VALUEsimple_expr : function_callfunction_call : VERSION \'(\' \')\'function_call : COUNT \'(\' \'*\' \')\'delete_statement : DELETE FROM identifier opt_WHERE'
    
_lr_action_items = {'USE':([0,33,37,38,39,41,45,46,47,48,49,51,55,79,81,82,83,84,85,86,91,92,94,95,107,111,115,117,119,120,122,124,125,126,129,135,137,142,144,147,156,157,165,181,184,],[1,-44,-129,-131,-121,-130,-128,-106,-117,-110,-127,-125,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,-122,-105,-45,-126,-73,-72,-107,-109,-132,-104,-103,-124,-123,-101,-20,-133,-102,-108,182,-73,-118,-21,-22,]),'*':([6,17,73,96,],[44,44,110,128,]),'DEFAULT':([117,169,171,172,173,174,175,177,178,186,188,190,195,196,198,199,200,201,205,207,208,219,220,221,226,227,],[-72,-59,200,-55,-52,-57,-58,-59,-59,-71,-70,-69,-60,-56,-68,200,-73,-66,-64,-50,-54,-63,-67,-65,-51,-53,]),'AUTOCOMMIT':([7,],[58,]),'DATABASES':([19,],[66,]),'NUMBER':([6,17,54,55,76,79,81,82,83,84,85,86,91,92,97,115,121,157,160,163,166,179,197,200,206,212,],[37,37,37,37,37,-116,-112,-115,-114,-111,37,-113,37,37,130,37,37,37,188,191,37,209,217,188,222,188,]),'UNSIGNED':([169,177,178,],[195,195,195,]),'TINYINT':([33,107,152,],[-44,-45,174,]),'SMALLINT':([33,107,152,],[-44,-45,169,]),'LIMIT':([6,33,37,38,39,40,41,43,44,45,46,47,48,49,51,52,53,55,56,57,76,77,79,81,82,83,84,85,86,89,91,92,94,95,107,111,112,113,114,115,116,117,119,120,122,123,124,125,126,128,129,142,144,145,147,161,165,192,213,215,216,225,228,],[-73,-44,-129,-131,-121,-84,-130,-94,-92,-128,-106,-117,-110,-127,-125,-96,-95,-73,-90,-122,-73,-101,-116,-112,-115,-114,-111,-73,-113,-93,-73,-73,-122,-105,-45,-126,-86,-85,-91,-73,-75,-72,-107,-109,-132,-97,-104,-103,-124,-98,-123,-133,-102,163,-108,-87,-118,-78,-76,-79,-80,-78,-77,]),'IF':([33,101,107,],[-44,133,-45,]),'N_EQ':([6,17,33,37,38,39,41,45,46,47,48,49,51,54,55,57,76,79,81,82,83,84,85,86,91,92,94,107,111,115,117,119,120,121,122,126,129,142,147,157,165,166,],[-73,-73,-44,-129,-131,-121,-130,-128,79,-117,-110,-127,-125,-73,-73,-122,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,-122,-45,-126,-73,-72,-107,-109,-73,-132,-124,-123,-133,-108,-73,-118,-73,]),'ORDER':([6,33,37,38,39,40,41,43,44,45,46,47,48,49,51,52,53,55,56,57,76,77,79,81,82,83,84,85,86,89,91,92,94,95,107,111,112,113,114,115,116,117,119,120,122,123,124,125,126,128,129,142,144,147,161,165,],[-73,-44,-129,-131,-121,-84,-130,-94,-92,-128,-106,-117,-110,-127,-125,-96,-95,-73,-90,-122,-73,-101,-116,-112,-115,-114,-111,-73,-113,-93,-73,-73,-122,-105,-45,-126,-86,-85,-91,-73,146,-72,-107,-109,-132,-97,-104,-103,-124,-98,-123,-133,-102,-108,-87,-118,]),'SELECT':([0,],[6,]),'IS':([6,17,33,37,38,39,41,45,46,47,48,49,51,54,55,57,76,79,81,82,83,84,85,86,91,92,94,107,111,115,117,119,120,121,122,126,129,142,147,157,165,166,],[-73,-73,-44,-129,-131,-121,-130,-128,80,-117,-110,-127,-125,-73,-73,-122,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,-122,-45,-126,-73,-72,-107,-109,-73,-132,-124,-123,-133,-108,-73,-118,-73,]),'INSERT':([0,],[4,]),'TABLE':([10,11,],[61,62,]),'SET':([0,33,69,107,],[7,-44,105,-45,]),'VARCHAR':([33,107,152,],[-44,-45,170,]),'STRING_VALUE':([6,17,54,55,76,79,81,82,83,84,85,86,91,92,115,121,157,160,166,200,210,212,],[41,41,41,41,41,-116,-112,-115,-114,-111,41,-113,41,41,41,41,41,186,41,186,223,186,]),"'":([6,17,54,55,76,78,79,81,82,83,84,85,86,91,92,115,121,157,160,166,200,212,],[42,42,42,42,42,117,-116,-112,-115,-114,-111,42,-113,42,42,42,42,42,42,42,42,42,]),')':([33,37,38,39,41,45,46,47,48,49,51,54,55,79,81,82,83,84,85,86,88,91,92,93,94,95,107,110,111,117,119,120,121,122,124,125,126,129,139,140,142,147,148,149,150,151,160,165,166,169,171,172,173,174,175,176,177,178,185,186,187,188,189,190,193,194,195,196,198,199,200,201,203,205,207,208,212,217,218,219,220,221,222,224,226,227,],[-44,-129,-131,-121,-130,-128,-106,-117,-110,-127,-125,-73,-73,-116,-112,-115,-114,-111,-73,-113,122,-73,-73,126,-122,-105,-45,142,-126,-72,-107,-109,-73,-132,-104,-103,-124,-123,158,-31,-133,-108,165,-119,167,-46,-73,-118,-73,-59,-61,-55,-52,-57,-58,-48,-59,-59,-32,-71,-33,-70,211,-69,-120,-47,-60,-56,-68,-61,-73,-66,-49,-64,-50,-54,-73,226,-62,-63,-67,-65,227,-34,-51,-53,]),'(':([6,17,33,35,50,54,55,72,76,79,81,82,83,84,85,86,87,91,92,100,107,115,121,141,157,166,170,173,],[54,54,-44,73,88,54,54,108,54,-116,-112,-115,-114,-111,54,-113,121,54,54,131,-45,54,54,160,54,54,197,206,]),'CREATE':([0,],[10,]),'DROP':([0,],[11,]),'NULL':([80,117,118,169,171,172,173,174,175,177,178,186,188,190,195,196,198,199,200,201,204,205,207,208,219,220,221,226,227,],[119,-72,147,-59,205,-55,-52,-57,-58,-59,-59,-71,-70,-69,-60,-56,-68,205,-73,-66,221,-64,-50,-54,-63,-67,-65,-51,-53,]),',':([6,17,33,37,38,39,40,41,43,44,45,46,47,48,49,51,52,53,55,56,57,64,76,79,81,82,83,84,85,86,89,91,92,94,95,107,111,114,117,119,120,121,122,123,124,125,126,128,129,135,137,139,140,142,147,148,149,150,151,157,160,165,166,169,171,172,173,174,175,176,177,178,181,184,185,186,187,188,189,190,193,194,195,196,198,199,200,201,203,205,207,208,212,218,219,220,221,224,226,227,],[-73,-73,-44,-129,-131,-121,76,-130,-94,-92,-128,-106,-117,-110,-127,-125,-96,-95,-73,-90,-122,76,-73,-116,-112,-115,-114,-111,-73,-113,-93,-73,-73,-122,-105,-45,-126,-91,-72,-107,-109,-73,-132,-97,-104,-103,-124,-98,-123,155,-20,159,-31,-133,-108,166,-119,168,-46,-73,-73,-118,-73,-59,-61,-55,-52,-57,-58,-48,-59,-59,-21,-22,-32,-71,-33,-70,212,-69,-120,-47,-60,-56,-68,-61,-73,-66,-49,-64,-50,-54,-73,-62,-63,-67,-65,-34,-51,-53,]),'.':([33,57,94,107,112,192,],[-44,96,127,-45,143,214,]),'ASC':([33,107,192,225,],[-44,-45,215,215,]),'NAMES':([7,],[59,]),'COMMIT':([0,],[20,]),'WAIT':([0,],[17,]),'=':([6,17,33,37,38,39,41,45,46,47,48,49,51,54,55,57,58,76,79,81,82,83,84,85,86,91,92,94,107,111,115,117,119,120,121,122,126,129,136,142,147,157,165,166,],[-73,-73,-44,-129,-131,-121,-130,-128,84,-117,-110,-127,-125,-73,-73,-122,97,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,-122,-45,-126,-73,-72,-107,-109,-73,-132,-124,-123,157,-133,-108,-73,-118,-73,]),'<':([6,17,33,37,38,39,41,45,46,47,48,49,51,54,55,57,76,79,81,82,83,84,85,86,91,92,94,107,111,115,117,119,120,121,122,126,129,142,147,157,165,166,],[-73,-73,-44,-129,-131,-121,-130,-128,82,-117,-110,-127,-125,-73,-73,-122,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,-122,-45,-126,-73,-72,-107,-109,-73,-132,-124,-123,-133,-108,-73,-118,-73,]),'$end':([2,3,5,6,8,9,12,13,14,15,16,18,20,21,22,23,24,27,28,29,32,33,37,38,39,40,41,43,44,45,46,47,48,49,51,52,53,55,56,57,66,68,76,77,79,81,82,83,84,85,86,89,91,92,94,95,98,99,101,102,104,106,107,111,112,113,114,115,116,117,119,120,122,123,124,125,126,128,129,130,132,134,135,137,138,142,144,145,147,153,154,156,157,161,162,165,167,180,181,183,184,191,192,209,211,213,215,216,223,225,228,],[-7,-2,-36,-73,-11,-4,-14,-10,-5,0,-8,-13,-39,-15,-1,-12,-9,-35,-6,-3,-43,-44,-129,-131,-121,-84,-130,-94,-92,-128,-106,-117,-110,-127,-125,-96,-95,-73,-90,-122,-42,-23,-73,-101,-116,-112,-115,-114,-111,-73,-113,-93,-73,-73,-122,-105,-37,-41,-26,-24,-83,-101,-45,-126,-86,-85,-91,-73,-75,-72,-107,-109,-132,-97,-104,-103,-124,-98,-123,-38,-25,-101,-101,-20,-134,-133,-102,-81,-108,-27,-17,-99,-73,-87,-74,-118,-40,-16,-21,-19,-22,-82,-78,-18,-28,-76,-79,-80,-100,-78,-77,]),'COUNT':([6,17,54,55,76,79,81,82,83,84,85,86,91,92,115,121,157,166,],[35,35,35,35,35,-116,-112,-115,-114,-111,35,-113,35,35,35,35,35,35,]),'@':([6,17,36,54,55,76,79,81,82,83,84,85,86,91,92,115,121,157,166,],[36,36,74,36,36,36,-116,-112,-115,-114,-111,36,-113,36,36,36,36,36,36,]),'FULL':([19,],[67,]),'STRING':([1,6,17,25,26,31,34,42,54,55,59,60,61,62,63,70,74,75,76,79,81,82,83,84,85,86,90,91,92,96,103,105,108,115,121,127,131,143,155,157,159,164,166,168,214,],[33,33,33,33,33,71,33,78,33,33,98,33,33,33,33,33,111,33,33,-116,-112,-115,-114,-111,33,-113,33,33,33,33,33,33,33,33,33,33,33,33,33,33,33,33,33,33,33,]),'EXISTS':([133,],[153,]),'SHOW':([0,],[19,]),'LOCK':([182,],[210,]),'INTO':([4,],[34,]),'BY':([146,],[164,]),'UPDATE':([0,],[26,]),'DATETIME':([33,107,152,],[-44,-45,173,]),'AS':([6,17,33,37,38,39,41,43,45,46,47,48,49,51,52,53,55,57,76,79,81,82,83,84,85,86,91,92,94,95,107,111,117,119,120,122,124,125,126,128,129,142,147,165,],[-73,-73,-44,-129,-131,-121,-130,-94,-128,-106,-117,-110,-127,-125,90,-95,-73,-122,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,-122,-105,-45,-126,-72,-107,-109,-132,-104,-103,-124,-98,-123,-133,-108,-118,]),'VERSION':([6,17,54,55,76,79,81,82,83,84,85,86,91,92,115,121,157,166,],[50,50,50,50,50,-116,-112,-115,-114,-111,50,-113,50,50,50,50,50,50,]),'LESS_OR_EQ':([6,17,33,37,38,39,41,45,46,47,48,49,51,54,55,57,76,79,81,82,83,84,85,86,91,92,94,107,111,115,117,119,120,121,122,126,129,142,147,157,165,166,],[-73,-73,-44,-129,-131,-121,-130,-128,83,-117,-110,-127,-125,-73,-73,-122,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,-122,-45,-126,-73,-72,-107,-109,-73,-132,-124,-123,-133,-108,-73,-118,-73,]),'IN':([6,17,33,37,38,39,41,45,47,49,51,54,55,57,76,79,81,82,83,84,85,86,91,92,94,107,111,115,117,121,122,126,129,142,157,166,],[-73,-73,-44,-129,-131,-121,-130,-128,87,-127,-125,-73,-73,-122,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,-122,-45,-126,-73,-72,-73,-132,-124,-123,-133,-73,-73,]),'UNIQUE':([117,169,171,172,173,174,175,177,178,186,188,190,195,196,198,199,200,201,205,207,208,219,220,221,226,227,],[-72,-59,198,-55,-52,-57,-58,-59,-59,-71,-70,-69,-60,-56,-68,198,-73,-66,-64,-50,-54,-63,-67,-65,-51,-53,]),'WHERE':([6,33,37,38,39,40,41,43,44,45,46,47,48,49,51,52,53,55,56,57,76,77,79,81,82,83,84,85,86,89,91,92,94,95,106,107,111,112,113,114,117,119,120,122,123,124,125,126,128,129,134,135,137,142,147,157,161,165,181,184,],[-73,-44,-129,-131,-121,-84,-130,-94,-92,-128,-106,-117,-110,-127,-125,-96,-95,-73,-90,-122,-73,115,-116,-112,-115,-114,-111,-73,-113,-93,-73,-73,-122,-105,115,-45,-126,-86,-85,-91,-72,-107,-109,-132,-97,-104,-103,-124,-98,-123,115,115,-20,-133,-108,-73,-87,-118,-21,-22,]),'DESC':([0,33,107,192,225,],[25,-44,-45,216,216,]),'AND':([6,17,33,37,38,39,41,45,46,47,48,49,51,53,54,55,57,76,79,81,82,83,84,85,86,91,92,93,94,95,107,111,115,117,119,120,121,122,124,125,126,129,142,144,147,149,157,165,166,184,193,],[-73,-73,-44,-129,-131,-121,-130,-128,-106,-117,-110,-127,-125,91,-73,-73,-122,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,91,-122,-105,-45,-126,-73,-72,-107,-109,-73,-132,-104,-103,-124,-123,-133,91,-108,91,-73,-118,-73,91,91,]),'`':([1,6,17,25,26,34,54,55,60,61,62,63,70,71,75,76,79,81,82,83,84,85,86,90,91,92,96,103,105,108,115,121,127,131,143,155,157,159,164,166,168,214,],[31,31,31,31,31,31,31,31,31,31,31,31,31,107,31,31,-116,-112,-115,-114,-111,31,-113,31,31,31,31,31,31,31,31,31,31,31,31,31,31,31,31,31,31,31,]),'GREATER_OR_EQ':([6,17,33,37,38,39,41,45,46,47,48,49,51,54,55,57,76,79,81,82,83,84,85,86,91,92,94,107,111,115,117,119,120,121,122,126,129,142,147,157,165,166,],[-73,-73,-44,-129,-131,-121,-130,-128,81,-117,-110,-127,-125,-73,-73,-122,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,-122,-45,-126,-73,-72,-107,-109,-73,-132,-124,-123,-133,-108,-73,-118,-73,]),'FROM':([6,17,30,33,37,38,39,40,41,43,44,45,46,47,48,49,51,52,53,55,56,57,64,76,79,81,82,83,84,85,86,89,91,92,94,95,107,111,114,117,119,120,122,123,124,125,126,128,129,142,147,165,],[-73,-73,70,-44,-129,-131,-121,75,-130,-94,-92,-128,-106,-117,-110,-127,-125,-96,-95,-73,-90,-122,103,-73,-116,-112,-115,-114,-111,-73,-113,-93,-73,-73,-122,-105,-45,-126,-91,-72,-107,-109,-132,-97,-104,-103,-124,-98,-123,-133,-108,-118,]),'DATABASE':([10,11,],[60,63,]),'INT':([33,107,152,],[-44,-45,178,]),'INTEGER':([33,107,152,],[-44,-45,177,]),'TABLES':([19,65,67,],[-88,104,-89,]),'LONGTEXT':([33,107,152,],[-44,-45,172,]),'PRIMARY':([117,169,171,172,173,174,175,177,178,186,188,190,195,196,198,199,200,201,205,207,208,219,220,221,226,227,],[-72,-59,202,-55,-52,-57,-58,-59,-59,-71,-70,-69,-60,-56,-68,202,-73,-66,-64,-50,-54,-63,-67,-65,-51,-53,]),'AFTER':([33,37,38,39,41,45,46,47,48,49,51,55,79,81,82,83,84,85,86,91,92,94,95,107,111,115,117,119,120,122,124,125,126,129,134,142,144,147,154,165,],[-44,-129,-131,-121,-130,-128,-106,-117,-110,-127,-125,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,-122,-105,-45,-126,-73,-72,-107,-109,-132,-104,-103,-124,-123,-101,-133,-102,-108,179,-118,]),'BOOL':([33,107,152,],[-44,-45,175,]),'KEY':([202,],[220,]),'VALUES':([33,72,107,109,158,],[-44,-29,-45,141,-30,]),'AUTO_INCREMENT':([117,169,171,172,173,174,175,177,178,186,188,190,195,196,198,199,200,201,205,207,208,219,220,221,226,227,],[-72,-59,201,-55,-52,-57,-58,-59,-59,-71,-70,-69,-60,-56,-68,201,-73,-66,-64,-50,-54,-63,-67,-65,-51,-53,]),'NOT':([6,17,54,55,76,80,91,92,115,117,121,157,166,169,171,172,173,174,175,177,178,186,188,190,195,196,198,199,200,201,205,207,208,219,220,221,226,227,],[55,55,55,55,55,118,55,55,55,-72,55,55,55,-59,204,-55,-52,-57,-58,-59,-59,-71,-70,-69,-60,-56,-68,204,-73,-66,-64,-50,-54,-63,-67,-65,-51,-53,]),'>':([6,17,33,37,38,39,41,45,46,47,48,49,51,54,55,57,76,79,81,82,83,84,85,86,91,92,94,107,111,115,117,119,120,121,122,126,129,142,147,157,165,166,],[-73,-73,-44,-129,-131,-121,-130,-128,86,-117,-110,-127,-125,-73,-73,-122,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,-122,-45,-126,-73,-72,-107,-109,-73,-132,-124,-123,-133,-108,-73,-118,-73,]),'OR':([6,17,33,37,38,39,41,45,46,47,48,49,51,53,54,55,57,76,79,81,82,83,84,85,86,91,92,93,94,95,107,111,115,117,119,120,121,122,124,125,126,129,142,144,147,149,157,165,166,184,193,],[-73,-73,-44,-129,-131,-121,-130,-128,-106,-117,-110,-127,-125,92,-73,-73,-122,-73,-116,-112,-115,-114,-111,-73,-113,-73,-73,92,-122,-105,-45,-126,-73,-72,-107,-109,-73,-132,-104,-103,-124,-123,-133,92,-108,92,-73,-118,-73,92,92,]),'DELETE':([0,],[30,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'list_expr':([121,],[148,]),'commit_statement':([0,],[2,]),'show_tables_statement':([0,],[3,]),'function_call':([6,17,54,55,76,85,91,92,115,121,157,166,],[38,38,38,38,38,38,38,38,38,38,38,38,]),'variable':([6,17,54,55,76,85,91,92,115,121,157,166,],[51,51,51,51,51,51,51,51,51,51,51,51,]),'set_names_statement':([0,],[5,]),'opt_USE_LOCK':([156,],[183,]),'column_definition':([152,],[176,]),'opt_WHERE':([77,106,134,135,],[116,138,154,156,]),'simple_expr':([6,17,54,55,76,85,91,92,115,121,157,166,],[39,39,39,39,39,39,39,39,39,39,39,39,]),'show_databases_statement':([0,],[14,]),'opt_column_def_options':([171,199,],[199,199,]),'select_item_list':([6,17,],[40,64,]),'opt_ORDER_BY':([116,],[145,]),'drop_database_statement':([0,],[8,]),'create_database_statement':([0,],[9,]),'table_wild':([6,17,76,],[43,43,43,]),'opt_LIMIT':([145,],[162,]),'update_table_statement':([0,],[12,]),'literal':([6,17,54,55,76,85,91,92,115,121,157,166,],[49,49,49,49,49,49,49,49,49,49,49,49,]),'delete_statement':([0,],[13,]),'fieldlist':([108,],[139,]),'statement':([0,],[15,]),'set_statement':([0,],[16,]),'opt_FROM':([40,],[77,]),'desc_table_statement':([0,],[18,]),'q_STRING':([6,17,54,55,76,85,91,92,115,121,157,160,166,200,212,],[45,45,45,45,45,45,45,45,45,45,45,190,45,190,190,]),'boolean_primary':([6,17,54,55,76,91,92,115,121,157,166,],[46,46,46,46,46,46,46,46,46,46,46,]),'bit_expr':([6,17,54,55,76,85,91,92,115,121,157,166,],[47,47,47,47,47,47,47,47,47,47,47,47,]),'predicate':([6,17,54,55,76,85,91,92,115,121,157,166,],[48,48,48,48,48,120,48,48,48,48,48,48,]),'values_list':([160,],[189,]),'table_reference':([75,],[113,]),'data_type':([152,],[171,]),'opt_UNSIGNED':([169,177,178,],[196,207,208,]),'opt_column_def_options_list':([171,199,],[203,218,]),'create_definition_list':([131,],[150,]),'opt_ORDER_DIRECTION':([192,225,],[213,228,]),'wait_statement':([0,],[21,]),'opt_FULL':([19,],[65,]),'select_statement':([0,],[22,]),'drop_table_statement':([0,],[23,]),'col_expr_list':([105,],[135,]),'opt_IF_EXISTS':([101,],[132,]),'insert_statement':([0,],[24,]),'identifier':([1,6,17,25,26,34,54,55,60,61,62,63,70,75,76,85,90,91,92,96,103,105,108,115,121,127,131,143,155,157,159,164,166,168,214,],[32,57,57,68,69,72,94,94,99,100,101,102,106,112,57,94,123,94,94,129,134,136,140,94,94,129,152,161,136,94,185,192,94,152,225,]),'opt_AFTER':([154,],[180,]),'select_item2':([6,17,76,],[52,52,52,]),'expr':([6,17,54,55,76,91,92,115,121,157,166,],[53,53,93,95,53,124,125,144,149,184,193,]),'opt_fieldlist':([72,],[109,]),'create_definition':([131,168,],[151,194,]),'value':([160,200,212,],[187,219,224,]),'use_database_statement':([0,],[28,]),'select_alias':([52,],[89,]),'create_table_statement':([0,],[29,]),'select_item':([6,17,76,],[56,56,114,]),'set_autocommit_statement':([0,],[27,]),'col_expr':([105,155,],[137,181,]),'comparison_operator':([46,],[85,]),}

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
   for _x, _y in zip(_v[0], _v[1]):
       if not _x in _lr_goto: _lr_goto[_x] = {}
       _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
  ("S' -> statement","S'",1,None,None,None),
  ('statement -> select_statement','statement',1,'p_statement','parser.py',18),
  ('statement -> show_tables_statement','statement',1,'p_statement','parser.py',19),
  ('statement -> create_table_statement','statement',1,'p_statement','parser.py',20),
  ('statement -> create_database_statement','statement',1,'p_statement','parser.py',21),
  ('statement -> show_databases_statement','statement',1,'p_statement','parser.py',22),
  ('statement -> use_database_statement','statement',1,'p_statement','parser.py',23),
  ('statement -> commit_statement','statement',1,'p_statement','parser.py',24),
  ('statement -> set_statement','statement',1,'p_statement','parser.py',25),
  ('statement -> insert_statement','statement',1,'p_statement','parser.py',26),
  ('statement -> delete_statement','statement',1,'p_statement','parser.py',27),
  ('statement -> drop_database_statement','statement',1,'p_statement','parser.py',28),
  ('statement -> drop_table_statement','statement',1,'p_statement','parser.py',29),
  ('statement -> desc_table_statement','statement',1,'p_statement','parser.py',30),
  ('statement -> update_table_statement','statement',1,'p_statement','parser.py',31),
  ('statement -> wait_statement','statement',1,'p_statement','parser.py',32),
  ('wait_statement -> WAIT select_item_list FROM identifier opt_WHERE opt_AFTER','wait_statement',6,'p_wait_statement','parser.py',38),
  ('opt_AFTER -> <empty>','opt_AFTER',0,'p_opt_after_empty','parser.py',50),
  ('opt_AFTER -> AFTER NUMBER','opt_AFTER',2,'p_opt_after','parser.py',54),
  ('update_table_statement -> UPDATE identifier SET col_expr_list opt_WHERE opt_USE_LOCK','update_table_statement',6,'p_update_table_statement','parser.py',62),
  ('col_expr_list -> col_expr','col_expr_list',1,'p_col_expr_list_one','parser.py',73),
  ('col_expr_list -> col_expr_list , col_expr','col_expr_list',3,'p_col_expr_list','parser.py',78),
  ('col_expr -> identifier = expr','col_expr',3,'p_col_expr','parser.py',84),
  ('desc_table_statement -> DESC identifier','desc_table_statement',2,'p_desc_table_statement','parser.py',89),
  ('drop_database_statement -> DROP DATABASE identifier','drop_database_statement',3,'p_drop_database_statement','parser.py',97),
  ('drop_table_statement -> DROP TABLE identifier opt_IF_EXISTS','drop_table_statement',4,'p_drop_table_statement','parser.py',105),
  ('opt_IF_EXISTS -> <empty>','opt_IF_EXISTS',0,'p_opt_if_exists_empty','parser.py',114),
  ('opt_IF_EXISTS -> IF EXISTS','opt_IF_EXISTS',2,'p_opt_if_exists','parser.py',119),
  ('insert_statement -> INSERT INTO identifier opt_fieldlist VALUES ( values_list )','insert_statement',8,'p_insert_statement','parser.py',124),
  ('opt_fieldlist -> <empty>','opt_fieldlist',0,'p_opt_fieldlist_empty','parser.py',143),
  ('opt_fieldlist -> ( fieldlist )','opt_fieldlist',3,'p_opt_fieldlist','parser.py',148),
  ('fieldlist -> identifier','fieldlist',1,'p_fieldlist_one','parser.py',153),
  ('fieldlist -> fieldlist , identifier','fieldlist',3,'p_fieldlist_many','parser.py',158),
  ('values_list -> value','values_list',1,'p_values_list_one','parser.py',167),
  ('values_list -> values_list , value','values_list',3,'p_values_list_many','parser.py',172),
  ('set_statement -> set_autocommit_statement','set_statement',1,'p_set_statement','parser.py',181),
  ('set_statement -> set_names_statement','set_statement',1,'p_set_statement','parser.py',182),
  ('set_names_statement -> SET NAMES STRING','set_names_statement',3,'p_set_names_statement','parser.py',187),
  ('set_autocommit_statement -> SET AUTOCOMMIT = NUMBER','set_autocommit_statement',4,'p_set_statement_autocommit','parser.py',194),
  ('commit_statement -> COMMIT','commit_statement',1,'p_commit_statement','parser.py',202),
  ('create_table_statement -> CREATE TABLE identifier ( create_definition_list )','create_table_statement',6,'p_create_table_statement','parser.py',209),
  ('create_database_statement -> CREATE DATABASE identifier','create_database_statement',3,'p_create_database_statement','parser.py',218),
  ('show_databases_statement -> SHOW DATABASES','show_databases_statement',2,'p_show_databases_statement','parser.py',226),
  ('use_database_statement -> USE identifier','use_database_statement',2,'p_use_database_statement','parser.py',233),
  ('identifier -> STRING','identifier',1,'p_identifier','parser.py',241),
  ('identifier -> ` STRING `','identifier',3,'p_identifier_escaped','parser.py',246),
  ('create_definition_list -> create_definition','create_definition_list',1,'p_create_definition_list_one','parser.py',251),
  ('create_definition_list -> create_definition_list , create_definition','create_definition_list',3,'p_create_definition_list_many','parser.py',258),
  ('create_definition -> identifier column_definition','create_definition',2,'p_create_definition','parser.py',265),
  ('column_definition -> data_type opt_column_def_options_list','column_definition',2,'p_column_definition','parser.py',270),
  ('data_type -> INTEGER opt_UNSIGNED','data_type',2,'p_data_type','parser.py',278),
  ('data_type -> VARCHAR ( NUMBER )','data_type',4,'p_data_type','parser.py',279),
  ('data_type -> DATETIME','data_type',1,'p_data_type','parser.py',280),
  ('data_type -> DATETIME ( NUMBER )','data_type',4,'p_data_type','parser.py',281),
  ('data_type -> INT opt_UNSIGNED','data_type',2,'p_data_type','parser.py',282),
  ('data_type -> LONGTEXT','data_type',1,'p_data_type','parser.py',283),
  ('data_type -> SMALLINT opt_UNSIGNED','data_type',2,'p_data_type','parser.py',284),
  ('data_type -> TINYINT','data_type',1,'p_data_type','parser.py',285),
  ('data_type -> BOOL','data_type',1,'p_data_type','parser.py',286),
  ('opt_UNSIGNED -> <empty>','opt_UNSIGNED',0,'p_opt_UNSIGNED','parser.py',291),
  ('opt_UNSIGNED -> UNSIGNED','opt_UNSIGNED',1,'p_opt_UNSIGNED','parser.py',292),
  ('opt_column_def_options_list -> <empty>','opt_column_def_options_list',0,'p_opt_column_def_options_list_empty','parser.py',296),
  ('opt_column_def_options_list -> opt_column_def_options opt_column_def_options_list','opt_column_def_options_list',2,'p_opt_column_def_options_list','parser.py',303),
  ('opt_column_def_options -> DEFAULT value','opt_column_def_options',2,'p_DEFAULT_CLAUSE','parser.py',312),
  ('opt_column_def_options -> NULL','opt_column_def_options',1,'p_NULL','parser.py',319),
  ('opt_column_def_options -> NOT NULL','opt_column_def_options',2,'p_NOT_NULL','parser.py',326),
  ('opt_column_def_options -> AUTO_INCREMENT','opt_column_def_options',1,'p_AUTO_INCREMENT','parser.py',333),
  ('opt_column_def_options -> PRIMARY KEY','opt_column_def_options',2,'p_PRIMARY_KEY','parser.py',340),
  ('opt_column_def_options -> UNIQUE','opt_column_def_options',1,'p_UNIQUE','parser.py',347),
  ('value -> q_STRING','value',1,'p_value','parser.py',354),
  ('value -> NUMBER','value',1,'p_value','parser.py',355),
  ('value -> STRING_VALUE','value',1,'p_value','parser.py',356),
  ("q_STRING -> ' STRING '",'q_STRING',3,'p_q_STRING','parser.py',361),
  ('q_STRING -> <empty>','q_STRING',0,'p_q_STRING_EMPTY','parser.py',366),
  ('select_statement -> SELECT select_item_list opt_FROM opt_WHERE opt_ORDER_BY opt_LIMIT','select_statement',6,'p_select_statement','parser.py',371),
  ('opt_ORDER_BY -> <empty>','opt_ORDER_BY',0,'p_opt_ORDER_BY_empty','parser.py',389),
  ('opt_ORDER_BY -> ORDER BY identifier opt_ORDER_DIRECTION','opt_ORDER_BY',4,'p_opt_ORDER_BY_simple','parser.py',394),
  ('opt_ORDER_BY -> ORDER BY identifier . identifier opt_ORDER_DIRECTION','opt_ORDER_BY',6,'p_opt_ORDER_BY_extended','parser.py',403),
  ('opt_ORDER_DIRECTION -> <empty>','opt_ORDER_DIRECTION',0,'p_opt_ORDER_DIRECTION_empty','parser.py',412),
  ('opt_ORDER_DIRECTION -> ASC','opt_ORDER_DIRECTION',1,'p_opt_ORDER_DIRECTION','parser.py',417),
  ('opt_ORDER_DIRECTION -> DESC','opt_ORDER_DIRECTION',1,'p_opt_ORDER_DIRECTION','parser.py',418),
  ('opt_LIMIT -> <empty>','opt_LIMIT',0,'p_opt_LIMIT_empty','parser.py',423),
  ('opt_LIMIT -> LIMIT NUMBER','opt_LIMIT',2,'p_opt_LIMIT','parser.py',428),
  ('show_tables_statement -> SHOW opt_FULL TABLES','show_tables_statement',3,'p_show_tables_statement','parser.py',433),
  ('opt_FROM -> <empty>','opt_FROM',0,'p_opt_from_empty','parser.py',441),
  ('opt_FROM -> FROM table_reference','opt_FROM',2,'p_opt_from','parser.py',446),
  ('table_reference -> identifier','table_reference',1,'p_table_reference','parser.py',451),
  ('table_reference -> identifier . identifier','table_reference',3,'p_table_reference_w_database','parser.py',456),
  ('opt_FULL -> <empty>','opt_FULL',0,'p_opt_FULL_empty','parser.py',461),
  ('opt_FULL -> FULL','opt_FULL',1,'p_opt_FULL','parser.py',466),
  ('select_item_list -> select_item','select_item_list',1,'p_select_item_list_select_item','parser.py',471),
  ('select_item_list -> select_item_list , select_item','select_item_list',3,'p_select_item_list','parser.py',476),
  ('select_item_list -> *','select_item_list',1,'p_select_item_list_star','parser.py',483),
  ('select_item -> select_item2 select_alias','select_item',2,'p_select_item','parser.py',493),
  ('select_item2 -> table_wild','select_item2',1,'p_select_item2','parser.py',498),
  ('select_item2 -> expr','select_item2',1,'p_select_item2','parser.py',499),
  ('select_alias -> <empty>','select_alias',0,'p_select_alias_empty','parser.py',504),
  ('select_alias -> AS identifier','select_alias',2,'p_select_alias','parser.py',509),
  ('table_wild -> identifier . *','table_wild',3,'p_table_wild','parser.py',514),
  ('opt_USE_LOCK -> <empty>','opt_USE_LOCK',0,'p_opt_USE_LOCK_empty','parser.py',519),
  ('opt_USE_LOCK -> USE LOCK STRING_VALUE','opt_USE_LOCK',3,'p_opt_USE_LOCK','parser.py',524),
  ('opt_WHERE -> <empty>','opt_WHERE',0,'p_opt_WHERE_empty','parser.py',529),
  ('opt_WHERE -> WHERE expr','opt_WHERE',2,'p_opt_WHERE','parser.py',534),
  ('expr -> expr OR expr','expr',3,'p_expr_OR','parser.py',539),
  ('expr -> expr AND expr','expr',3,'p_expr_AND','parser.py',544),
  ('expr -> NOT expr','expr',2,'p_expr_NOT','parser.py',549),
  ('expr -> boolean_primary','expr',1,'p_expr_bool_primary','parser.py',554),
  ('boolean_primary -> boolean_primary IS NULL','boolean_primary',3,'p_boolean_primary_is_null','parser.py',559),
  ('boolean_primary -> boolean_primary IS NOT NULL','boolean_primary',4,'p_boolean_primary_is_not_null','parser.py',564),
  ('boolean_primary -> boolean_primary comparison_operator predicate','boolean_primary',3,'p_boolean_primary_comparison','parser.py',569),
  ('boolean_primary -> predicate','boolean_primary',1,'p_boolean_primary_predicate','parser.py',574),
  ('comparison_operator -> =','comparison_operator',1,'p_comparison_operator','parser.py',579),
  ('comparison_operator -> GREATER_OR_EQ','comparison_operator',1,'p_comparison_operator','parser.py',580),
  ('comparison_operator -> >','comparison_operator',1,'p_comparison_operator','parser.py',581),
  ('comparison_operator -> LESS_OR_EQ','comparison_operator',1,'p_comparison_operator','parser.py',582),
  ('comparison_operator -> <','comparison_operator',1,'p_comparison_operator','parser.py',583),
  ('comparison_operator -> N_EQ','comparison_operator',1,'p_comparison_operator','parser.py',584),
  ('predicate -> bit_expr','predicate',1,'p_predicate','parser.py',589),
  ('predicate -> bit_expr IN ( list_expr )','predicate',5,'p_predicate_in','parser.py',594),
  ('list_expr -> expr','list_expr',1,'p_list_expr_one','parser.py',603),
  ('list_expr -> list_expr , expr','list_expr',3,'p_list_expr','parser.py',608),
  ('bit_expr -> simple_expr','bit_expr',1,'p_bit_expr','parser.py',614),
  ('simple_expr -> identifier','simple_expr',1,'p_simple_expr_identifier','parser.py',619),
  ('simple_expr -> identifier . identifier','simple_expr',3,'p_simple_expr_identifier_full','parser.py',624),
  ('simple_expr -> ( expr )','simple_expr',3,'p_simple_expr_parent','parser.py',629),
  ('simple_expr -> variable','simple_expr',1,'p_simple_expr_variable','parser.py',634),
  ('variable -> @ @ STRING','variable',3,'p_variable','parser.py',639),
  ('simple_expr -> literal','simple_expr',1,'p_simple_expr_literal','parser.py',644),
  ('literal -> q_STRING','literal',1,'p_literal','parser.py',649),
  ('literal -> NUMBER','literal',1,'p_literal','parser.py',650),
  ('literal -> STRING_VALUE','literal',1,'p_literal','parser.py',651),
  ('simple_expr -> function_call','simple_expr',1,'p_simple_expr_function_call','parser.py',656),
  ('function_call -> VERSION ( )','function_call',3,'p_function_call_version','parser.py',661),
  ('function_call -> COUNT ( * )','function_call',4,'p_function_call_count_star','parser.py',666),
  ('delete_statement -> DELETE FROM identifier opt_WHERE','delete_statement',4,'p_delete_statement','parser.py',671),
]
