/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/test/java/com/facebook/presto/sql/parser/TestStatementSplitter.java
 */
package com.yahoo.bullet.bql.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.yahoo.bullet.bql.parser.StatementSplitter.Statement;
import static com.yahoo.bullet.bql.parser.StatementSplitter.isEmptyStatement;
import static com.yahoo.bullet.bql.parser.StatementSplitter.squeezeStatement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class StatementSplitterTest {
    @Test
    public void testSplitterIncomplete() {
        StatementSplitter splitter = new StatementSplitter(" select * FROM foo  ");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), "select * FROM foo");
    }

    @Test
    public void testSplitterEmptyInput() {
        StatementSplitter splitter = new StatementSplitter("");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), "");
    }

    @Test
    public void testSplitterEmptyStatements() {
        StatementSplitter splitter = new StatementSplitter(";;;");
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), "");
    }

    @Test
    public void testSplitterSingle() {
        StatementSplitter splitter = new StatementSplitter("select * from foo;");
        assertEquals(splitter.getCompleteStatements(), statements("select * from foo", ";"));
        assertEquals(splitter.getPartialStatement(), "");
    }

    @Test
    public void testSplitterMultiple() {
        StatementSplitter splitter = new StatementSplitter(" select * from  foo ; select * from t; select * from ");
        assertEquals(splitter.getCompleteStatements(), statements("select * from  foo", ";", "select * from t", ";"));
        assertEquals(splitter.getPartialStatement(), "select * from");
    }

    @Test
    public void testSplitterMultipleWithEmpty() {
        StatementSplitter splitter = new StatementSplitter("; select * from  foo ; select * from t;;;select * from ");
        assertEquals(splitter.getCompleteStatements(), statements("select * from  foo", ";", "select * from t", ";"));
        assertEquals(splitter.getPartialStatement(), "select * from");
    }

    @Test
    public void testSplitterCustomDelimiters() {
        String bql = "// select * from  foo // select * from t;//select * from ";
        StatementSplitter splitter = new StatementSplitter(bql, ImmutableSet.of(";", "//"));
        assertEquals(splitter.getCompleteStatements(), statements("select * from  foo", "//", "select * from t", ";"));
        assertEquals(splitter.getPartialStatement(), "select * from");
    }

    @Test
    public void testSplitterErrorBeforeComplete() {
        StatementSplitter splitter = new StatementSplitter(" select * from z# oops ; select ");
        assertEquals(splitter.getCompleteStatements(), statements("select * from z# oops", ";"));
        assertEquals(splitter.getPartialStatement(), "select");
    }

    @Test
    public void testSplitterErrorAfterComplete() {
        StatementSplitter splitter = new StatementSplitter("select * from foo; select z# oops ");
        assertEquals(splitter.getCompleteStatements(), statements("select * from foo", ";"));
        assertEquals(splitter.getPartialStatement(), "select z# oops");
    }

    @Test
    public void testSplitterWithQuotedString() {
        String bql = "select 'foo bar' x from dual";
        StatementSplitter splitter = new StatementSplitter(bql);
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), bql);
    }

    @Test
    public void testSplitterWithIncompleteQuotedString() {
        String bql = "select 'foo', 'bar";
        StatementSplitter splitter = new StatementSplitter(bql);
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), bql);
    }

    @Test
    public void testSplitterWithEscapedSingleQuote() {
        String bql = "select 'hello''world' from dual";
        StatementSplitter splitter = new StatementSplitter(bql + ";");
        assertEquals(splitter.getCompleteStatements(), statements(bql, ";"));
        assertEquals(splitter.getPartialStatement(), "");
    }

    @Test
    public void testSplitterWithQuotedIdentifier() {
        String bql = "select \"0\"\"bar\" from dual";
        StatementSplitter splitter = new StatementSplitter(bql + ";");
        assertEquals(splitter.getCompleteStatements(), statements(bql, ";"));
        assertEquals(splitter.getPartialStatement(), "");
    }

    @Test
    public void testSplitterWithBackquote() {
        String bql = "select  ` f``o o ` from dual";
        StatementSplitter splitter = new StatementSplitter(bql);
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), bql);
    }

    @Test
    public void testSplitterWithDigitIdentifier() {
        String bql = "select   1x  from dual";
        StatementSplitter splitter = new StatementSplitter(bql);
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), bql);
    }

    @Test
    public void testSplitterWithSingleLineComment() {
        StatementSplitter splitter = new StatementSplitter("--empty\n;-- start\nselect * -- junk\n-- hi\nfrom foo; -- done");
        assertEquals(splitter.getCompleteStatements(), statements("--empty", ";", "-- start\nselect * -- junk\n-- hi\nfrom foo", ";"));
        assertEquals(splitter.getPartialStatement(), "-- done");
    }

    @Test
    public void testSplitterWithMultiLineComment() {
        StatementSplitter splitter = new StatementSplitter("/* empty */;/* start */ select * /* middle */ from foo; /* end */");
        assertEquals(splitter.getCompleteStatements(), statements("/* empty */", ";", "/* start */ select * /* middle */ from foo", ";"));
        assertEquals(splitter.getPartialStatement(), "/* end */");
    }

    @Test
    public void testSplitterWithSingleLineCommentPartial() {
        String bql = "-- start\nselect * -- junk\n-- hi\nfrom foo -- done";
        StatementSplitter splitter = new StatementSplitter(bql);
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), bql);
    }

    @Test
    public void testSplitterWithMultiLineCommentPartial() {
        String bql = "/* start */ select * /* middle */ from foo /* end */";
        StatementSplitter splitter = new StatementSplitter(bql);
        assertEquals(splitter.getCompleteStatements(), ImmutableList.of());
        assertEquals(splitter.getPartialStatement(), bql);
    }

    @Test
    public void testIsEmptyStatement() {
        assertTrue(isEmptyStatement(""));
        assertTrue(isEmptyStatement(" "));
        assertTrue(isEmptyStatement("\t\n "));
        assertTrue(isEmptyStatement("--foo\n  --what"));
        assertTrue(isEmptyStatement("/* oops */"));
        assertFalse(isEmptyStatement("x"));
        assertFalse(isEmptyStatement("select"));
        assertFalse(isEmptyStatement("123"));
        assertFalse(isEmptyStatement("z#oops"));
    }

    @Test
    public void testSqueezeStatement() {
        String bql = "select   *  from\n foo\n  order by x ; ";
        assertEquals(squeezeStatement(bql), "select * from foo order by x ;");
    }

    @Test
    public void testSqueezeStatementWithIncompleteQuotedString() {
        String bql = "select   *  from\n foo\n  where x = 'oops";
        assertEquals(squeezeStatement(bql), "select * from foo where x = 'oops");
    }

    @Test
    public void testSqueezeStatementWithBackquote() {
        String bql = "select  `  f``o  o`` `   from dual";
        assertEquals(squeezeStatement(bql), "select `  f``o  o`` ` from dual");
    }

    @Test
    public void testSqueezeStatementAlternateDelimiter() {
        String bql = "select   *  from\n foo\n  order by x // ";
        assertEquals(squeezeStatement(bql), "select * from foo order by x //");
    }

    @Test
    public void testSqueezeStatementError() {
        String bql = "select   *  from z#oops";
        assertEquals(squeezeStatement(bql), "select * from z#oops");
    }

    @Test
    public void testTerminator() {
        StatementSplitter splitter = new StatementSplitter(" select * FROM foo;");
        assertEquals(splitter.getCompleteStatements().get(0).terminator(), ";");
    }

    @Test
    public void testEquals() {
        Set<String> delimiters = ImmutableSet.of(";", "//");
        StatementSplitter splitter = new StatementSplitter("select * FROM foo;select * FROM foo;select * FROM foo//select *;", delimiters);
        Statement statement1 = splitter.getCompleteStatements().get(0);
        Statement statement2 = splitter.getCompleteStatements().get(1);
        Statement statement3 = splitter.getCompleteStatements().get(2);
        Statement statement4 = splitter.getCompleteStatements().get(3);
        assertTrue(statement1.equals(statement1));
        assertTrue(statement1.equals(statement2));
        assertFalse(statement1.equals(statement3));
        assertFalse(statement1.equals(statement4));
        assertFalse(statement1.equals(null));
        assertFalse(statement1.equals(Collections.emptyList()));
        assertEquals(statement1.hashCode(), -1074861806);
    }

    @Test
    public void testEqualssadfasdf() {
        StatementSplitter splitter = new StatementSplitter("      select * from foo;");
    }

    private static List<Statement> statements(String... args) {
        checkArgument(args.length % 2 == 0, "arguments not paired");
        ImmutableList.Builder<Statement> list = ImmutableList.builder();
        for (int i = 0; i < args.length; i += 2) {
            list.add(new Statement(args[i], args[i + 1]));
        }
        return list.build();
    }
}
