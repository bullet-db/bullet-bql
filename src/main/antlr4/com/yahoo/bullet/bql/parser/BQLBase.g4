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

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

singleExpression
    : expression EOF
    ;

statement
    : query                                                                               #statementDefault
    ;

query
    : queryNoWith
    ;

queryNoWith
    : queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (WINDOWING '(' windowOperation ')')?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
    ;

windowOperation
    : EVERY ',' emitEvery=INTEGER_VALUE ',' emitType=(TIME | RECORD) ',' include          #emitEvery
    | TUMBLING ',' emitEvery=INTEGER_VALUE ',' emitType=(TIME | RECORD)                   #tumbling
    ;

include
    : includeUnit=ALL
    | includeType=(FIRST | LAST) ',' INTEGER_VALUE ',' includeUnit=(TIME | RECORD)
    ;

queryTerm
    : queryPrimary                                                                        #queryTermDefault
    ;

queryPrimary
    : querySpecification                                                                  #queryPrimaryDefault
    ;

sortItem
    : expression ordering=(ASC | DESC)?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      FROM relation
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=topKThreshold)?
    ;

topKThreshold
    : expression comparisonOperator right=INTEGER_VALUE
    ;

groupBy
    : groupingElement
    ;

groupingElement
    : groupingExpressions                                                                 #singleGroupingSet
    ;

groupingExpressions
    : '(' (referenceExpression (',' referenceExpression)*)? ')'
    | referenceExpression (',' referenceExpression)*
    ;

setQuantifier
    : DISTINCT
    ;

selectItem
    : primaryExpression (AS? identifier)?                                                 #selectSingle
    | qualifiedName '.' ASTERISK (AS? identifier)?                                        #selectAll
    | ASTERISK                                                                            #selectAll
    ;

relation
    : sampledRelation                                                                     #relationDefault
    ;

sampledRelation
    : aliasedRelation
    ;

aliasedRelation
    : relationPrimary
    ;

relationPrimary
    : STREAM '(' (timeDuration=(INTEGER_VALUE | MAX) ',' TIME
        (',' recordDuration=(INTEGER_VALUE | MAX) ',' RECORD)?)? ')'                      #stream
    ;

expression
    : booleanExpression
    | primaryExpression
    | valueExpression
    ;

booleanExpression
    : predicated                                                                          #booleanDefault
    | NOT booleanExpression                                                               #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression                         #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                          #logicalBinary
    | '(' booleanExpression ')'                                                           #parenthesizedExpression
    ;

predicated
    : predicatedReferenceExpression predicate[$predicatedReferenceExpression.ctx]
    ;

predicatedReferenceExpression
    : referenceExpression                                                                 #referenceWithoutFunction
    | functionName '(' referenceExpression ')'                                            #referenceWithFunction
    ;

functionName
    : SIZEOF
    ;

predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                                            #comparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression                        #between
    | NOT? IN valueExpressionList                                                         #inList
    | NOT? LIKE valueExpressionList                                                       #likeList
    | IS NOT? NULL                                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                                         #distinctFrom
    | IS NOT? EMPTY                                                                       #emptyPredicate
    | NOT? containsOperator valueExpressionList                                           #containsList
    ;

valueExpressionList
    : '(' valueExpression (',' valueExpression)* ')'
    ;

containsOperator
    : CONTAINSKEY | CONTAINSVALUE
    ;

primaryExpression
    : qualifiedName '(' ASTERISK ')'                                                      #functionCall
    | qualifiedName '(' setQuantifier? referenceExpression (',' referenceExpression)*')'  #functionCall
    | referenceExpression                                                                 #reference
    | arithmeticExpression                                                                #computation
    | distributionType '(' referenceExpression ',' inputMode ')'                          #distributionOperation
    | TOP '(' topKConfig ')'                                                              #topK
    ;

arithmeticExpression
    : '(' arithmeticExpression ')'                                                        #parensExpression
    | CAST '(' arithmeticExpression ',' castType ')'                                      #castExpression
    | left=arithmeticExpression op=(ASTERISK | SLASH) right=arithmeticExpression          #infixExpression
    | left=arithmeticExpression op=(PLUS | MINUS) right=arithmeticExpression              #infixExpression
    | valueExpression                                                                     #leafExpression
    ;

castType
    : INTEGER_TYPE | LONG_TYPE | FLOAT_TYPE | DOUBLE_TYPE | BOOLEAN_TYPE | STRING_TYPE
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
    : size=INTEGER_VALUE (',' threshold=INTEGER_VALUE)? ',' referenceExpression (',' referenceExpression)*
    ;

referenceExpression
    : identifier                                                                          #columnReference
    | base=identifier '.' fieldName=identifier ('.' subFieldName=identifier)?             #dereference
    ;

valueExpression
    : NULL                                                                                #nullLiteral
    | number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | string                                                                              #stringLiteral
    | operator=(MINUS | PLUS) number                                                      #arithmeticUnary
    | referenceExpression                                                                 #fieldReference
    ;

signedNumber
    : operator=(MINUS | PLUS)? number
    ;

string
    : STRING                                                                              #basicStringLiteral
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

qualifiedName
    : identifier
    ;

//fieldIdentifier
//    : IDENTIFIER                                                                          #basicFieldIdentifier
//    | nonReserved                                                                         #basicFieldIdentifier
//    | INTEGER_VALUE                                                                       #basicFieldIdentifier
//    ;

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

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See BQLParser.exitNonReserved
    : ALL | ASC | DESC
    | FIRST | LAST | LIMIT
    | EMPTY
    | STREAM
    | TIME | RECORD | MAX
    | TUMBLING | WINDOWING | EVERY
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
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' | ':')*
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
