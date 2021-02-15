/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4
 */
grammar BQLBase;

query
    : SELECT select FROM stream
      (WHERE where=expression)?
      (GROUP BY groupBy)?
      (HAVING having=expression)?
      (ORDER BY orderBy)?
      (WINDOWING window)?
      (LIMIT limit=INTEGER_VALUE)?
      ';'?
      EOF
    ;

select
    : DISTINCT? selectItem (',' selectItem)*
    ;

selectItem
    : expression (AS? identifier)?
    | ASTERISK
    ;

stream
    : STREAM '(' (timeDuration=(INTEGER_VALUE | MAX) ',' TIME)? ')'
    ;

groupBy
    : expressions
    ;

orderBy
    : sortItem (',' sortItem)*
    ;

sortItem
    : expression ordering=(ASC | DESC)?
    ;

window
    : EVERY '(' emitEvery=INTEGER_VALUE ',' emitType=(TIME | RECORD) ',' include ')'
    | TUMBLING '(' emitEvery=INTEGER_VALUE ',' emitType=(TIME | RECORD) ')'
    ;

include
    : includeUnit=ALL
    | includeType=FIRST ',' INTEGER_VALUE ',' includeUnit=(TIME | RECORD)
    ;

expression
    : valueExpression                                                                                                   #value
    | fieldExpression                                                                                                   #field
    | listExpression                                                                                                    #list
    | expression IS NULL                                                                                                #nullPredicate
    | expression IS NOT NULL                                                                                            #nullPredicate
    | unaryExpression                                                                                                   #unary
    | functionExpression                                                                                                #function
    | left=expression modifier=NOT? op=IN right=expression                                                              #infix
    | left=expression op=RLIKE modifier=ANY? right=expression                                                           #infix
    | left=expression op=(ASTERISK | SLASH) right=expression                                                            #infix
    | left=expression op=(PLUS | MINUS) right=expression                                                                #infix
    | left=expression op=(LT | LTE | GT | GTE) modifier=(ANY | ALL)? right=expression                                   #infix
    | left=expression op=(EQ | NEQ) modifier=(ANY | ALL)? right=expression                                              #infix
    | left=expression op=AND right=expression                                                                           #infix
    | left=expression op=XOR right=expression                                                                           #infix
    | left=expression op=OR right=expression                                                                            #infix
    | '(' expression ')'                                                                                                #parentheses
    ;

expressions
    : expression (',' expression)*
    ;

valueExpression
    : NULL                                                                                                              #nullLiteral
    | number                                                                                                            #numericLiteral
    | booleanValue                                                                                                      #booleanLiteral
    | STRING                                                                                                            #stringLiteral
    ;

fieldExpression
    : field=identifier (':' fieldType)?
    | field=identifier '[' index=INTEGER_VALUE ']' (':' fieldType)?
    | field=identifier '[' index=INTEGER_VALUE ']' '.' subKey=identifier (':' fieldType)?
    | field=identifier '.' key=identifier (':' fieldType)?
    | field=identifier '.' key=identifier '.' subKey=identifier (':' fieldType)?
    ;

listExpression
    : '[' ']'
    | '[' expressions ']'
    ;

unaryExpression
    : op=(NOT | SIZEOF) parens='(' operand=expression ')'
    | op=(NOT | SIZEOF) operand=expression
    ;

functionExpression
    : op=(SIZEIS | CONTAINSKEY | CONTAINSVALUE | FILTER)
      '(' left=expression ',' right=expression ')'                                                                      #binary
    | op=IF '(' expressions ')'                                                                                         #nAry
    | aggregateExpression                                                                                               #aggregate
    | CAST '(' expression AS primitiveType ')'                                                                          #cast
    ;

aggregateExpression
    : op=COUNT '(' ASTERISK ')'                                                                                         #groupOperation
    | op=(SUM | AVG | MIN | MAX) '(' expression ')'                                                                     #groupOperation
    | COUNT '(' DISTINCT expressions ')'                                                                                #countDistinct
    | distributionType '(' expression ',' inputMode ')'                                                                 #distribution
    | TOP '(' size=INTEGER_VALUE (',' threshold=INTEGER_VALUE)? ',' expressions ')'                                     #topK
    ;

distributionType
    : type=(QUANTILE | FREQ | CUMFREQ)
    ;

inputMode
    : iMode=LINEAR ',' numberOfPoints=INTEGER_VALUE
    | iMode=REGION ',' start=number ',' end=number ',' increment=number
    | iMode=MANUAL ',' number (',' number)*
    ;

identifier
    : IDENTIFIER                                                                                                        #unquotedIdentifier
    | nonReserved                                                                                                       #unquotedIdentifier
    | QUOTED_IDENTIFIER                                                                                                 #quotedIdentifier
    | DIGIT_IDENTIFIER                                                                                                  #digitIdentifier
    ;

number
    : operator=(MINUS | PLUS)? value=(INTEGER_VALUE | LONG_VALUE | FLOAT_VALUE | DOUBLE_VALUE)
    ;

booleanValue
    : TRUE | FALSE
    ;

fieldType
    : primitiveType
    | outerType=LIST_TYPE '[' primitiveType ']'
    | outerType=MAP_TYPE '[' primitiveType ']'
    | complexOuterType=LIST_TYPE '[' MAP_TYPE '[' primitiveType ']' ']'
    | complexOuterType=MAP_TYPE '[' MAP_TYPE '[' primitiveType ']' ']'
    ;

primitiveType
    : INTEGER_TYPE | LONG_TYPE | FLOAT_TYPE | DOUBLE_TYPE | BOOLEAN_TYPE | STRING_TYPE
    ;

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See BQLParser.exitNonReserved
    : ALL | ASC | DESC
    | FIRST | LAST | LIMIT
    | EMPTY
    | STREAM
    | TIME | RECORD | MAX
    | WINDOWING | EVERY | TUMBLING
    | MANUAL | REGION | LINEAR
    | QUANTILE | FREQ | CUMFREQ
    | TOP
    | COUNT | SUM | AVG | MIN
    ;

ALL: 'ALL';
AND: 'AND';
ANY: 'ANY';
AS: 'AS';
ASC: 'ASC';
BETWEEN: 'BETWEEN';
BY: 'BY';
CAST: 'CAST';
DESC: 'DESC';
DISTINCT: 'DISTINCT';
FIRST: 'FIRST';
FROM: 'FROM';
GROUP: 'GROUP';
HAVING: 'HAVING';
IN: 'IN';
IS: 'IS';
LAST: 'LAST';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
NOT: 'NOT';
NULL: 'NULL';
OR: 'OR';
ORDER: 'ORDER';
SELECT: 'SELECT';
WHERE: 'WHERE';
STREAM: 'STREAM';
TIME: 'TIME';
TRUE: 'TRUE';
FALSE: 'FALSE';
QUANTILE: 'QUANTILE';
FREQ: 'FREQ';
CUMFREQ: 'CUMFREQ';
LINEAR: 'LINEAR';
REGION: 'REGION';
MANUAL: 'MANUAL';
TOP: 'TOP';
WINDOWING: 'WINDOWING';
RECORD: 'RECORD';
EVERY: 'EVERY';
EMPTY: 'EMPTY';
TUMBLING: 'TUMBLING';
MAX: 'MAX';
XOR: 'XOR';

COUNT: 'COUNT';
SUM: 'SUM';
AVG: 'AVG';
MIN: 'MIN';
RLIKE: 'RLIKE';
SIZEIS: 'SIZEIS';
FILTER: 'FILTER';
IF: 'IF';

INTEGER_TYPE: 'INTEGER';
LONG_TYPE: 'LONG';
FLOAT_TYPE: 'FLOAT';
DOUBLE_TYPE: 'DOUBLE';
BOOLEAN_TYPE: 'BOOLEAN';
STRING_TYPE: 'STRING';
LIST_TYPE: 'LIST';
MAP_TYPE: 'MAP';

EQ  : '=' | '==';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';
SIZEOF : 'SIZEOF';
CONTAINSKEY : 'CONTAINSKEY';
CONTAINSVALUE : 'CONTAINSVALUE';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

LONG_VALUE
    : INTEGER_VALUE 'L'
    ;

DOUBLE_VALUE
    : DIGIT+ '.' DIGIT*
    | DIGIT+ ('.' DIGIT*)? EXPONENT
    ;

FLOAT_VALUE
    : DOUBLE_VALUE 'F'
    ;

IDENTIFIER
    : (LETTER | '_') NAME_LETTER*
    ;

DIGIT_IDENTIFIER
    : DIGIT NAME_LETTER+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
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

fragment NAME_LETTER
    : LETTER | DIGIT | '_'
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text.
// when splitting statements with DelimiterLexer.
UNRECOGNIZED
    : .
    ;
