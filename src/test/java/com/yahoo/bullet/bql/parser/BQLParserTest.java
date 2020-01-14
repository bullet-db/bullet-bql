/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/test/java/com/facebook/presto/sql/parser/TestSqlParser.java
 */
package com.yahoo.bullet.bql.parser;

public class BQLParserTest {
    private static final BQLParser BQL_PARSER = new BQLParser();

    // TODO exceptions in here

    /*@Test
    public void testPossibleExponentialBacktracking() {
        BQL_PARSER.createExpression("(((((((((((((((((((((((((((a = true)))))))))))))))))))))))))))", new ParsingOptions());
    }

    @Test
    public void testQualifiedName() {
        assertEquals(QualifiedName.of("a", "b", "c", "d").toString(), "a.b.c.d");
        assertEquals(QualifiedName.of("A", "b", "C", "d").toString(), "a.b.c.d");
        assertTrue(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("b", "c", "d")));
        assertTrue(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("a", "b", "c", "d")));
        assertFalse(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("a", "c", "d")));
        assertFalse(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("z", "a", "b", "c", "d")));
        assertEquals(QualifiedName.of("a", "b", "c", "d"), QualifiedName.of("a", "b", "c", "d"));
    }

    @Test
    public void testDouble() {
        assertExpression("123E7", new DoubleLiteralNode("123E7"));
        assertExpression("123.E7", new DoubleLiteralNode("123E7"));
        assertExpression("123.0E7", new DoubleLiteralNode("123E7"));
        assertExpression("123E+7", new DoubleLiteralNode("123E7"));
        assertExpression("123E-7", new DoubleLiteralNode("123E-7"));

        assertExpression("123.456E7", new DoubleLiteralNode("123.456E7"));
        assertExpression("123.456E+7", new DoubleLiteralNode("123.456E7"));
        assertExpression("123.456E-7", new DoubleLiteralNode("123.456E-7"));

        assertExpression(".4E42", new DoubleLiteralNode(".4E42"));
        assertExpression(".4E+42", new DoubleLiteralNode(".4E42"));
        assertExpression(".4E-42", new DoubleLiteralNode(".4E-42"));
    }

    @Test
    public void testArithmeticUnary() {
        assertExpression("9", new LongLiteralNode("9"));

        assertExpression("+9", positive(new LongLiteralNode("9")));
        assertExpression("+ 9", positive(new LongLiteralNode("9")));

        assertExpression("-9", negative(new LongLiteralNode("9")));
        assertExpression("- 9", negative(new LongLiteralNode("9")));
    }

    @Test
    public void testDoubleInQuery() {
        assertStatement("SELECT a FROM STREAM(6000, TIME) WHERE b = 123.456E7",
                simpleQuery(
                        selectList(identifier("a")),
                        simpleStream("6000"),
                        equal(identifier("b"), new DoubleLiteralNode("123.456E7"))));
    }

    @Test
    public void testBetween() {
        assertExpression("a BETWEEN 2 AND 3", new BetweenPredicate(identifier("a"), new LongLiteralNode("2"), new LongLiteralNode("3")));
        assertExpression("a NOT BETWEEN 2 AND 3", logicalNot(new BetweenPredicate(identifier("a"), new LongLiteralNode("2"), new LongLiteralNode("3"))));
    }

    @Test
    public void testPrecedenceAndAssociativity() {
        assertExpression("a=1 AND b=2 OR b=3", or(
                and(equal(identifier("a"), new LongLiteralNode("1")), equal(identifier("b"), new LongLiteralNode("2"))),
                equal(identifier("b"), new LongLiteralNode("3"))));


        assertExpression("a=1 OR a=2 AND b=3", or(
                equal(identifier("a"), new LongLiteralNode("1")),
                and(equal(identifier("a"), new LongLiteralNode("2")), equal(identifier("b"), new LongLiteralNode("3")))));

        assertExpression("NOT a=1 AND b=2", and(
                logicalNot(equal(identifier("a"), new LongLiteralNode("1"))),
                equal(identifier("b"), new LongLiteralNode("2"))));

        assertExpression("NOT a=1 OR b=2", or(
                logicalNot(equal(identifier("a"), new LongLiteralNode("1"))),
                equal(identifier("b"), new LongLiteralNode("2"))));
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: no viable alternative at input '<EOF>'")
    public void testEmptyExpression() {
        BQL_PARSER.createExpression("", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: mismatched input '<EOF>' expecting 'SELECT'")
    public void testEmptyStatement() {
        BQL_PARSER.createStatement("", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: extraneous input '@'\\E.*")
    public void testTokenizeErrorStartOfLine() {
        BQL_PARSER.createStatement("@select", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:8: extraneous input '@'\\E.*")
    public void testTokenizeErrorMiddleOfLine() {
        BQL_PARSER.createStatement("select @what from stream(3, time)", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:37: extraneous input\\E.*")
    public void testTokenizeErrorIncompleteToken() {
        BQL_PARSER.createStatement("select * from stream(3, time) where 'oops=typo", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 3:1: extraneous input 'from' expecting\\E.*")
    public void testParseErrorStartOfLine() {
        BQL_PARSER.createStatement("select *\nfrom stream(3, time)\nfrom", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 3:7: mismatched input 'from'\\E.*")
    public void testParseErrorMiddleOfLine() {
        BQL_PARSER.createStatement("select *\nfrom stream(3, time)\nwhere from", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:14: mismatched input '<EOF>' expecting 'STREAM'")
    public void testParseErrorEndOfInput() {
        BQL_PARSER.createStatement("select * from", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:16: mismatched input '<EOF>' expecting 'STREAM'")
    public void testParseErrorEndOfInputWhitespace() {
        BQL_PARSER.createStatement("select * from  ", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:8: Identifiers must not start with a digit; surround the identifier with double quotes")
    public void testParseErrorDigitIdentifiers() {
        BQL_PARSER.createStatement("select 1x from stream(3, time)", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:8: Identifiers must not contain '@'")
    public void testIdentifierWithAtSign() {
        BQL_PARSER.createStatement("select foo@bar from stream(3, time)", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:8: Identifiers must not contain ':'")
    public void testIdentifierWithColon() {
        BQL_PARSER.createStatement("select foo:bar from stream(3, time)", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:42: mismatched input 'windowing' expecting <EOF>")
    public void testParseErrorReverseWindowingLimit() {
        BQL_PARSER.createStatement("select fuu from stream(3, time) limit 10 windowing(tumbling, 10, time)", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: Invalid numeric literal: 12223222232535343423232435343")
    public void testParseErrorInvalidPositiveNumeric() {
        BQL_PARSER.createStatement("select * from stream(3, time) where num=12223222232535343423232435343", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: Invalid numeric literal: 12223222232535343423232435343")
    public void testParseErrorInvalidNegativeNumeric() {
        BQL_PARSER.createStatement("select * from stream(3, time) where num=-12223222232535343423232435343", new ParsingOptions());
    }

    @Test
    public void testParsingExceptionPositionInfo() {
        try {
            BQL_PARSER.createStatement("select *\nfrom stream(3, time)\nwhere from", new ParsingOptions());
            fail("expected exception");
        } catch (ParsingException e) {
            assertTrue(e.getMessage().startsWith("line 3:7: mismatched input 'from'"));
        }
    }

    @Test
    public void testAllowIdentifierColon() {
        BQLParser bqlParser = new BQLParser(new BQLParserOptions().allowIdentifierSymbol(COLON));
        bqlParser.createStatement("select foo:bar from stream(3, time)", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:12: no viable alternative at input\\E.*")
    public void testInvalidArguments() {
        BQL_PARSER.createStatement("select foo(,1) FROM STREAM(3, TIME)", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:20: mismatched input\\E.*")
    public void testInvalidArguments2() {
        BQL_PARSER.createStatement("select foo(DISTINCT) from stream(3, time)", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:21: extraneous input\\E.*")
    public void testInvalidArguments3() {
        BQL_PARSER.createStatement("select foo(DISTINCT ,a)", new ParsingOptions());
    }

    @Test
    public void testAllowIdentifierAtSign() {
        BQLParser bqlParser = new BQLParser(new BQLParserOptions().allowIdentifierSymbol(AT_SIGN));
        bqlParser.createStatement("select foo@bar from stream(3, time)", new ParsingOptions());
    }

    @Test
    public void testAllowIdentifierAtSignIterable() {
        BQLParser bqlParser = new BQLParser(new BQLParserOptions().allowIdentifierSymbol(Collections.singleton(AT_SIGN)));
        bqlParser.createStatement("select foo@bar from stream(3, time)", new ParsingOptions());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: expression is too large \\(stack overflow while parsing\\)")
    public void testStackOverflowExpression() {
        for (int size = 3000; size <= 100_000; size *= 2) {
            BQL_PARSER.createExpression(Joiner.on(" OR ").join(nCopies(size, "x = 2")), new ParsingOptions());
        }
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: statement is too large \\(stack overflow while parsing\\)")
    public void testStackOverflowStatement() {
        for (int size = 6000; size <= 100_000; size *= 2) {
            BQL_PARSER.createStatement("SELECT * FROM STREAM(3, TIME) WHERE " + Joiner.on(" OR ").join(nCopies(size, "x = 1")), new ParsingOptions());
        }
    }

    @Test
    public void testSelectWithGroupBy() {
        assertStatement("SELECT * FROM STREAM(3, TIME) GROUP BY a",
                simpleQuery(
                        selectList(new AllColumns()),
                        simpleStream("3"),
                        new GroupByNode(false, ImmutableList.of(new SimpleGroupBy(ImmutableList.of(identifier("a")))))));

        assertStatement("SELECT * FROM STREAM(3, TIME) GROUP BY a, b",
                simpleQuery(
                        selectList(new AllColumns()),
                        simpleStream("3"),
                        new GroupByNode(false, ImmutableList.of(new SimpleGroupBy(ImmutableList.of(identifier("a"), identifier("b")))))));

        assertStatement("SELECT * FROM STREAM(3, TIME) GROUP BY ()",
                simpleQuery(
                        selectList(new AllColumns()),
                        simpleStream("3"),
                        new GroupByNode(false, ImmutableList.of(new SimpleGroupBy(ImmutableList.of())))));
    }

    @Test
    public void testNonReserved() {
        assertStatement("SELECT zone FROM STREAM(3, TIME)",
                simpleQuery(
                        selectList(identifier("zone")),
                        simpleStream("3")));

        assertStatement("SELECT INCLUDING, EXCLUDING, PROPERTIES FROM STREAM(3, TIME)",
                simpleQuery(
                        selectList(
                                identifier("INCLUDING"),
                                identifier("EXCLUDING"),
                                identifier("PROPERTIES")),
                        simpleStream("3")));

        assertStatement("SELECT ALL, SOME, ANY FROM STREAM(3, TIME)",
                simpleQuery(
                        selectList(
                                identifier("ALL"),
                                identifier("SOME"),
                                identifier("ANY")),
                        simpleStream("3")));

        assertExpression("stats", identifier("stats"));
        assertExpression("nfd", identifier("nfd"));
        assertExpression("nfc", identifier("nfc"));
        assertExpression("nfkd", identifier("nfkd"));
        assertExpression("nfkc", identifier("nfkc"));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = "\\QNot yet implemented\\E.*")
    public void testExpressionFormatterVisitNode() {
        new ExpressionFormatter.Formatter(Optional.empty()).process(simpleWithQuery());
    }

    @Test
    public void testExpressionFormatterConstructor() {
        assertTrue(new ExpressionFormatter() instanceof ExpressionFormatter);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QExpression inside formatExpression() must not be null\\E.*")
    public void testNullExpression() {
        formatExpression(null, Optional.empty());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Unexpected decimal literal\\E.*")
    public void testRejectParsingOptions() {
        BQL_PARSER.createExpression("10.5", new ParsingOptions(REJECT));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = "\\QNot yet implemented\\E.*")
    public void testAggregateResultNullNext() {
        new ASTBuilder(new ParsingOptions()).aggregateResult(identifier("aaa"), null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = "\\QNot yet implemented\\E.*")
    public void testAggregateResultNullAggregate() {
        new ASTBuilder(new ParsingOptions()).aggregateResult(identifier("aaa"), identifier("bbb"));
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:3: exception as expected\\E.*")
    public void testParsingException() {
        NodeLocation location = new NodeLocation(1, 1);
        throw new ParsingException("exception as expected", location);
    }

    private static void assertStatement(String query, Statement expected) {
        assertParsed(query, expected, BQL_PARSER.createStatement(query, new ParsingOptions()));
        assertFormattedBQL(BQL_PARSER, expected);
    }

    private static void assertExpression(String expression, ExpressionNode expected) {
        assertParsed(expression, expected, BQL_PARSER.createExpression(expression, new ParsingOptions()));
    }

    private static void assertParsed(String input, Node expected, Node parsed) {
        if (!parsed.equals(expected)) {
            fail(format("expected\n\n%s\n\nto parse as\n\n%s\n\nbut was\n\n%s\n",
                    indent(input),
                    indent(formatBQL(expected, Optional.empty())),
                    indent(formatBQL(parsed, Optional.empty()))));
        }
    }

    private static String indent(String value) {
        String indent = "    ";
        return indent + value.trim().replaceAll("\n", "\n" + indent);
    }*/
}
