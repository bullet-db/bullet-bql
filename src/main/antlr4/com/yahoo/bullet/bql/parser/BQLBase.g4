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
      (ORDER BY sortItem)?
      (WINDOWING '(' windowOperation ')')?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
    ;

windowOperation
    : EVERY ',' emitEvery=INTEGER_VALUE ',' emitType=(TIME | RECORD) ',' include          #emitEvery
    | TUMBLING ',' emitEvery=INTEGER_VALUE ',' emitType=TIME                              #tumbling
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
    : qualifiedName '(' ASTERISK ')' ordering=DESC
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      FROM relation
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=topKThreshold)?
    ;

topKThreshold
    : qualifiedName '(' ASTERISK ')' GTE right=INTEGER_VALUE
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
    : referenceExpression predicate[$referenceExpression.ctx]
    ;

predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                                            #comparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression                        #between
    | NOT? IN '(' valueExpression (',' valueExpression)* ')'                              #inList
    | NOT? LIKE '(' valueExpression (',' valueExpression)* ')'                            #likeList
    | IS NOT? NULL                                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                                         #distinctFrom
    | IS NOT? EMPTY                                                                       #emptyPredicate
    ;

primaryExpression
    : qualifiedName '(' ASTERISK ')'                                                      #functionCall
    | qualifiedName '(' setQuantifier? referenceExpression (',' referenceExpression)*')'  #functionCall
    | referenceExpression                                                                 #reference
    | distributionType '(' referenceExpression ',' inputMode ')'                          #distributionOperation
    | TOP '(' topKConfig ')'                                                              #topK
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
    | base=identifier '.' fieldName=identifier                                            #dereference
    ;

valueExpression
    : NULL                                                                                #nullLiteral
    | number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | string                                                                              #stringLiteral
    | operator=(MINUS | PLUS) number                                                      #arithmeticUnary
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

identifier
    : IDENTIFIER                                                                          #unquotedIdentifier
    | QUOTED_IDENTIFIER                                                                   #quotedIdentifier
    | nonReserved                                                                         #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER                                                               #backQuotedIdentifier
    | DIGIT_IDENTIFIER                                                                    #digitIdentifier
    ;

number
    : DECIMAL_VALUE                                                                       #decimalLiteral
    | DOUBLE_VALUE                                                                        #doubleLiteral
    | INTEGER_VALUE                                                                       #integerLiteral
    ;

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See BQLParser.exitNonReserved
    : ADD | ALL | ANALYZE | ANY | ARRAY | ASC | AT
    | BERNOULLI
    | CALL | CASCADE | CATALOGS | COALESCE | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | CURRENT
    | DATA | DATE | DAY | DESC | DISTRIBUTED
    | EXCLUDING | EXPLAIN
    | FILTER | FIRST | FOLLOWING | FORMAT | FUNCTIONS
    | GRANT | GRANTS | GRAPHVIZ
    | HOUR
    | IF | INCLUDING | INPUT | INTEGER | INTERVAL | ISOLATION
    | LAST | LATERAL | LEVEL | LIMIT | LOGICAL
    | MAP | MINUTE | MONTH
    | NFC | NFD | NFKC | NFKD | NO | NULLIF | NULLS
    | ONLY | OPTION | ORDINALITY | OUTPUT | OVER
    | PARTITION | PARTITIONS | POSITION | PRECEDING | PRIVILEGES | PROPERTIES | PUBLIC
    | RANGE | READ | RENAME | REPEATABLE | REPLACE | RESET | RESTRICT | REVOKE | ROLLBACK | ROW | ROWS
    | SCHEMA | SCHEMAS | SECOND | SERIALIZABLE | SESSION | SET | SETS
    | SHOW | SMALLINT | SOME | START | STATS | SUBSTRING | SYSTEM
    | TABLES | TABLESAMPLE | TEXT | TIME | TIMESTAMP | TINYINT | TO | TRANSACTION | TRY_CAST | TYPE
    | UNBOUNDED | UNCOMMITTED | USE
    | VALIDATE | VERBOSE | VIEW
    | WORK | WRITE
    | YEAR
    | ZONE
    | TUMBLING | WINDOWING | EVERY
    | EMPTY
    | STREAM
    | RECORD
    | MAX
    | TOP
    | MANUAL | REGION | LINEAR
    | QUANTILE | FREQ | CUMFREQ
    ;

ADD: 'ADD';
ALL: 'ALL';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
AND: 'AND';
ANY: 'ANY';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
AT: 'AT';
BERNOULLI: 'BERNOULLI';
BETWEEN: 'BETWEEN';
BY: 'BY';
CALL: 'CALL';
CASCADE: 'CASCADE';
CASE: 'CASE';
CAST: 'CAST';
CATALOGS: 'CATALOGS';
COALESCE: 'COALESCE';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMMITTED: 'COMMITTED';
CONSTRAINT: 'CONSTRAINT';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DATA: 'DATA';
DATE: 'DATE';
DAY: 'DAY';
DEALLOCATE: 'DEALLOCATE';
DELETE: 'DELETE';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DISTINCT: 'DISTINCT';
DISTRIBUTED: 'DISTRIBUTED';
DROP: 'DROP';
ELSE: 'ELSE';
END: 'END';
ESCAPE: 'ESCAPE';
EXCEPT: 'EXCEPT';
EXCLUDING: 'EXCLUDING';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXTRACT: 'EXTRACT';
FALSE: 'FALSE';
FILTER: 'FILTER';
FIRST: 'FIRST';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FORMAT: 'FORMAT';
FROM: 'FROM';
FULL: 'FULL';
FUNCTIONS: 'FUNCTIONS';
GRANT: 'GRANT';
GRANTS: 'GRANTS';
GRAPHVIZ: 'GRAPHVIZ';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
HAVING: 'HAVING';
HOUR: 'HOUR';
IF: 'IF';
IN: 'IN';
INCLUDING: 'INCLUDING';
INNER: 'INNER';
INPUT: 'INPUT';
INSERT: 'INSERT';
INTEGER: 'INTEGER';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
IS: 'IS';
ISOLATION: 'ISOLATION';
JOIN: 'JOIN';
LAST: 'LAST';
LATERAL: 'LATERAL';
LEFT: 'LEFT';
LEVEL: 'LEVEL';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
LOGICAL: 'LOGICAL';
MAP: 'MAP';
MINUTE: 'MINUTE';
MONTH: 'MONTH';
NATURAL: 'NATURAL';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO';
NORMALIZE: 'NORMALIZE';
NOT: 'NOT';
NULL: 'NULL';
NULLIF: 'NULLIF';
NULLS: 'NULLS';
ON: 'ON';
ONLY: 'ONLY';
OPTION: 'OPTION';
OR: 'OR';
ORDER: 'ORDER';
ORDINALITY: 'ORDINALITY';
OUTER: 'OUTER';
OUTPUT: 'OUTPUT';
OVER: 'OVER';
PARTITION: 'PARTITION';
PARTITIONS: 'PARTITIONS';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PREPARE: 'PREPARE';
PRIVILEGES: 'PRIVILEGES';
PROPERTIES: 'PROPERTIES';
PUBLIC: 'PUBLIC';
RANGE: 'RANGE';
READ: 'READ';
RECURSIVE: 'RECURSIVE';
RENAME: 'RENAME';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
RESET: 'RESET';
RESTRICT: 'RESTRICT';
REVOKE: 'REVOKE';
RIGHT: 'RIGHT';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROW: 'ROW';
ROWS: 'ROWS';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECOND: 'SECOND';
SELECT: 'SELECT';
SERIALIZABLE: 'SERIALIZABLE';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
SMALLINT: 'SMALLINT';
SOME: 'SOME';
START: 'START';
STATS: 'STATS';
SUBSTRING: 'SUBSTRING';
SYSTEM: 'SYSTEM';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TEXT: 'TEXT';
THEN: 'THEN';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TINYINT: 'TINYINT';
TO: 'TO';
TRANSACTION: 'TRANSACTION';
TRUE: 'TRUE';
TRY_CAST: 'TRY_CAST';
TYPE: 'TYPE';
UESCAPE: 'UESCAPE';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNION: 'UNION';
UNNEST: 'UNNEST';
USE: 'USE';
USING: 'USING';
VALIDATE: 'VALIDATE';
VALUES: 'VALUES';
VERBOSE: 'VERBOSE';
VIEW: 'VIEW';
WHEN: 'WHEN';
WHERE: 'WHERE';
WITH: 'WITH';
WORK: 'WORK';
WRITE: 'WRITE';
YEAR: 'YEAR';
ZONE: 'ZONE';
STREAM: 'STREAM';
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

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
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

TIME_WITH_TIME_ZONE
    : 'TIME' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

TIMESTAMP_WITH_TIME_ZONE
    : 'TIMESTAMP' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

DOUBLE_PRECISION
    : 'DOUBLE' WS 'PRECISION'
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
