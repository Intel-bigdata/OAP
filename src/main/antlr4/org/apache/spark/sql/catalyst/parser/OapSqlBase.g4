grammar OapSqlBase;

@members {
  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is folllowed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }
}

singleStatement
    : statement EOF
    ;

statement
    : REFRESH SINDEX ON tableIdentifier partitionSpec?                #oapRefreshIndices
    | CREATE SINDEX (IF NOT EXISTS)? IDENTIFIER ON
        tableIdentifier indexCols (USING indexType)?
        partitionSpec?                                                #oapCreateIndex
    | DROP SINDEX (IF EXISTS)? IDENTIFIER ON tableIdentifier
        partitionSpec?                                                #oapDropIndex
    | DISABLE SINDEX IDENTIFIER                                       #oapDisableIndex
    | ENABLE SINDEX IDENTIFIER                                        #oapEnableIndex
    | SHOW SINDEX (FROM | IN) tableIdentifier                         #oapShowIndex
    | CHECK SINDEX ON tableIdentifier partitionSpec?                  #oapCheckIndex
    | .*?                                                             #passThrough
    ;

tableIdentifier
    : (db=identifier '.')? table=identifier
    ;

partitionSpec
    : PARTITION '(' partitionVal (',' partitionVal)* ')'
    ;

partitionVal
    : identifier (EQ constant)?
    ;

constant
    : NULL                                                                                     #nullLiteral
    | interval                                                                                 #intervalLiteral
    | identifier STRING                                                                        #typeConstructor
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | STRING+                                                                                  #stringLiteral
    ;

indexCols
    : '(' indexCol (',' indexCol)* ')'
    ;

indexCol
    : identifier (ASC | DESC)?
    ;

indexType
    : BTREE
    | BITMAP
    ;

nonReserved
    : CHECK
    | DISABLE | ENABLE | TRUE | FALSE | REFRESH | CREATE | IF | NOT | EXISTS
    | DROP | SHOW | FROM | IN | PARTITION | AS | ASC | DESC | INTERVAL | TO
    ;

interval
    : INTERVAL intervalField*
    ;

intervalField
    : value=intervalValue unit=identifier (TO to=identifier)?
    ;

intervalValue
    : (PLUS | MINUS)? (INTEGER_VALUE | DECIMAL_VALUE)
    | STRING
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    ;

number
    : MINUS? DECIMAL_VALUE            #decimalLiteral
    | MINUS? INTEGER_VALUE            #integerLiteral
    | MINUS? BIGINT_LITERAL           #bigIntLiteral
    | MINUS? SMALLINT_LITERAL         #smallIntLiteral
    | MINUS? TINYINT_LITERAL          #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL           #doubleLiteral
    | MINUS? BIGDECIMAL_LITERAL       #bigDecimalLiteral
    ;
booleanValue
    : TRUE | FALSE
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

DISABLE: 'DISABLE';
ENABLE: 'ENABLE';
CHECK: 'CHECK';
SINDEX: 'SINDEX' | 'OINDEX';
SINDICES: 'SINDICES' | 'OINDICES';
BTREE: 'BTREE';
BLOOM: 'BLOOM';
BITMAP: 'BITMAP';
NULL: 'NULL';
TRUE: 'TRUE';
FALSE: 'FALSE';
ON: 'ON';
REFRESH: 'REFRESH';
CREATE: 'CREATE';
IF: 'IF';
NOT: 'NOT' | '!';
EXISTS: 'EXISTS';
USING: 'USING';
DROP: 'DROP';
SHOW: 'SHOW';
FROM: 'FROM';
IN: 'IN';
PARTITION: 'PARTITION';
AS: 'AS';
ASC: 'ASC';
DESC: 'DESC';
INTERVAL: 'INTERVAL';
TO: 'TO';
PLUS: '+';
MINUS: '-';
EQ : '=' | '==';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT? {isValidDecimal()}?
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS  : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
