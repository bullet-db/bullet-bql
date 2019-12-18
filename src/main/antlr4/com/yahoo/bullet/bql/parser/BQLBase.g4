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
    : STREAM '(' (timeDuration=(INTEGER_VALUE | MAX) ',' TIME (',' recordDuration=(INTEGER_VALUE | MAX) ',' RECORD)?)? ')'
    ;

groupBy
    : expression (',' expression)*
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
    //| includeType=(FIRST | LAST) ',' INTEGER_VALUE ',' includeUnit=(TIME | RECORD)
    | includeType=FIRST ',' INTEGER_VALUE ',' includeUnit=(TIME | RECORD)
    ;

expression
    : valueExpression                                                                       #value
    | identifier                                                                            #field
    | listExpression                                                                        #list
    | expression IS NULL                                                                    #nullPredicate
    | expression IS NOT NULL                                                                #nullPredicate
    | unaryExpression                                                                       #unary
    | functionExpression                                                                    #function
    | left=expression op=(ASTERISK | SLASH) right=expression                                #infix
    | left=expression op=(PLUS | MINUS) right=expression                                    #infix
    | left=expression op=(LT | LTE | GT | GTE) right=expression                             #infix
    | left=expression op=(EQ | NEQ) right=expression                                        #infix
    | left=expression op=(AND | XOR) right=expression                                       #infix
    | left=expression op=OR right=expression                                                #infix
    | '(' expression ')'                                                                    #parentheses
    ;

valueExpression
    : NULL                                                                                  #nullLiteral
    | signedNumber                                                                          #numericLiteral
    | booleanValue                                                                          #booleanLiteral
    | string                                                                                #stringLiteral
    ;

listExpression
    : '[' ']'
    | '[' expression (',' expression)* ']'
    ;

unaryExpression
    : op=(NOT | SIZEOF) operand=expression
    ;
/*
binaryExpression
    : op=('RLIKE' | 'SIZEIS' | CONTAINSKEY | CONTAINSVALUE | XOR | 'FILTER') '(' left=expression ',' right=expression ')'
    ;

nAryExpression
    : op=(AND | OR | 'IF') '(' (expression? | (expression (',' expression)*)) ')'
    ;
*/
functionExpression
    : binaryFunction '(' left=expression ',' right=expression ')'                           #binary
    | op=(AND | OR | IF) '(' expression (',' expression)* ')'                               #nAry
    | aggregateExpression                                                                   #aggregate
    | CAST '(' expression AS castType ')'                                                   #cast
    ;

binaryFunction
    : op=(RLIKE | SIZEIS | CONTAINSKEY | CONTAINSVALUE | FILTER)
    ;

aggregateExpression
    : op=COUNT '(' ASTERISK ')'                                                             #groupOperation
    | op=(SUM | AVG | MIN | MAX) '(' expression ')'                                         #groupOperation
    | COUNT '(' DISTINCT expression ( ',' expression )* ')'                                 #countDistinct
    | distributionType '(' expression ',' inputMode ')'                                     #distribution
    | TOP '(' topKConfig ')'                                                                #topK
    ;

distributionType
    : QUANTILE | FREQ | CUMFREQ
    ;

inputMode
    : iMode=LINEAR ',' numberOfPoints=INTEGER_VALUE
    | iMode=REGION ',' start=signedNumber ',' end=signedNumber ',' increment=signedNumber
    | iMode=MANUAL ',' signedNumber (',' signedNumber)*
    ;

topKConfig
    : size=INTEGER_VALUE (',' threshold=INTEGER_VALUE)? ',' expression (',' expression)*
    ;

castType
    : INTEGER_TYPE | LONG_TYPE | FLOAT_TYPE | DOUBLE_TYPE | BOOLEAN_TYPE | STRING_TYPE
    ;

booleanValue
    : TRUE | FALSE
    ;

identifier
    : IDENTIFIER                                                                          #unquotedIdentifier
    | nonReserved                                                                         #unquotedIdentifier
    | DIGIT_IDENTIFIER                                                                    #digitIdentifier
    ;

number
    : DECIMAL_VALUE                                                                       #decimalLiteral
    | DOUBLE_VALUE                                                                        #doubleLiteral
    | INTEGER_VALUE                                                                       #integerLiteral
    ;

signedNumber
    : operator=(MINUS | PLUS)? number
    ;

string
    : STRING                                                                              #basicStringLiteral
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
    ;

ALL: 'ALL';
AND: 'AND';
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

EQ  : '=';
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
    | '"' ( ~'"' | '""' )* '"'
    ;

UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') ((NAME_LETTER | '.')* NAME_LETTER)?
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@' | ':')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
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
    : LETTER | DIGIT | '_' | '@' | ':'
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
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
