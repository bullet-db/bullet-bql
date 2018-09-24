/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.parser;

import com.yahoo.bullet.bql.tree.Expression;
import com.yahoo.bullet.bql.tree.Statement;
import com.yahoo.bullet.bql.util.BQLFormatter;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.yahoo.bullet.bql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.yahoo.bullet.bql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.yahoo.bullet.bql.util.TreeAssertions.assertFormattedBQL;
import static com.yahoo.bullet.bql.util.TreeAssertions.assertFormattedBQLDecimal;
import static org.testng.Assert.assertTrue;

// Test bql can be parsed into node tree, and node tree can be parsed into same formatted bql.
public class StatementBuilderTest {
    private static final BQLParser BQL_PARSER = new BQLParser();

    @Test
    public void testStatementBuilder() {
        assertStatement("SELECT * FROM STREAM(3, TIME)");

        assertStatement("SELECT * FROM STREAM(3, TIME) LIMIT 5");

        assertStatement("SELECT * FROM STREAM(3, TIME, 10, RECORD)");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME)");

        assertStatement("SELECT aaa.bbb AS ccc FROM STREAM(3, TIME)");

        assertStatement("SELECT aaa.* AS bbb FROM STREAM(3, TIME)");

        assertStatement("SELECT aaa, bbb.ccc, ddd.* FROM STREAM(3, TIME, 10, RECORD)");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa=+5");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa=-5");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa!=5");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa<=5");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa>=5");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa IS DISTINCT FROM 5");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa LIKE ('ccc', 'ddd')");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa NOT LIKE ('ccc', 'ddd')");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa BETWEEN 1 AND 3");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa NOT BETWEEN 1 AND 3");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa IN (1, 3, 'a')");

        assertStatementDecimal("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa NOT IN (1.5, 3, 'a')");

        assertStatementDecimal("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa=1 OR aaa=3");

        assertStatementDecimal("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa>1 AND aaa<3");

        assertStatementDecimal("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa=NULL");

        assertStatementDecimal("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa IS NULL");

        assertStatementDecimal("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa IS NOT NULL");

        assertStatementDecimal("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa=TRUE");

        assertStatement("SELECT aaa FROM STREAM(3, TIME) GROUP BY bbb, ccc.ddd, eee");

        assertStatement("SELECT SUM(aaa) AS totalA, SUM(bbb.ccc) AS totalBC FROM STREAM(3, TIME) GROUP BY ddd");

        assertStatement("SELECT COUNT(*) FROM STREAM(3, TIME) GROUP BY aaa.bbb");

        assertStatement("SELECT COUNT(aaa) FROM STREAM(3, TIME) GROUP BY aaa.bbb");

        assertStatement("SELECT MIN(*) FROM STREAM(3, TIME) GROUP BY aaa.bbb");

        assertStatement("SELECT MIN(aaa) AS minA, MIN(bbb.ccc) AS minBC FROM STREAM(3, TIME) GROUP BY ddd");

        assertStatement("SELECT MAX(aaa) AS maxA, MAX(bbb.ccc) AS maxBC FROM STREAM(3, TIME) GROUP BY ddd");

        assertStatement("SELECT AVG(aaa) AS avgA, AVG(bbb.ccc) AS avgBC FROM STREAM(3, TIME) GROUP BY ddd");

        assertStatement("SELECT SUM(aaa) AS sumA, MAX(bbb) AS maxB, MIN(ccc) AS minC, AVG(ddd) AS avgD, COUNT(eee) AS countE, ddd AS D FROM STREAM(3, TIME) GROUP BY ddd");

        assertStatement("SELECT COUNT(DISTINCT aaa) AS dstCountA FROM STREAM(3, TIME)");

        assertStatement("SELECT COUNT(DISTINCT aaa.bbb) AS dstCountAB FROM STREAM(3, TIME)");

        assertStatement("SELECT COUNT(DISTINCT aaa) AS dstCountA FROM STREAM(3, TIME)");

        assertStatement("SELECT QUANTILE(aaa, LINEAR, 11) AS quantileA FROM STREAM(3, TIME)");

        assertStatement("SELECT QUANTILE(aaa, REGION, 0.2, 0.8, 0.3) AS quantileA FROM STREAM(3, TIME)");

        assertStatement("SELECT QUANTILE(aaa, MANUAL, 0, 0.4, 0.8, 0.9) AS quantileA FROM STREAM(3, TIME)");

        assertStatement("SELECT QUANTILE(aaa, MANUAL, 0, 0.4, 0.8, 0.9) AS quantileA FROM STREAM(3, TIME) ORDER BY bbb");

        assertStatement("SELECT FREQ(aaa, LINEAR, 11) AS freqA FROM STREAM(3, TIME)");

        assertStatement("SELECT FREQ(aaa, LINEAR, 11) AS freqA FROM STREAM(3, TIME) ORDER BY bbb");

        assertStatement("SELECT CUMFREQ(aaa, LINEAR, 11) AS freqA FROM STREAM(3, TIME)");

        assertStatement("SELECT CUMFREQ(aaa, LINEAR, 11) AS freqA FROM STREAM(3, TIME) ORDER BY bbb");

        assertStatement("SELECT TOP(3, aaa) AS top3A FROM STREAM(3, TIME)");

        assertStatement("SELECT TOP(3, 5, aaa) AS top3A FROM STREAM(3, TIME)");

        assertStatement("SELECT TOP(3, 5, aaa) AS top3A FROM STREAM(3, TIME) ORDER BY bbb");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WINDOWING(EVERY, 3, TIME, FIRST, 3, TIME)");

        assertStatement("SELECT COUNT(*) FROM STREAM(3, TIME) GROUP BY aaa.bbb WINDOWING(EVERY, 3, TIME, FIRST, 3, TIME)");

        assertStatement("SELECT COUNT(DISTINCT aaa) AS dstCountA FROM STREAM(3, TIME) WINDOWING(EVERY, 3, TIME, FIRST, 3, TIME)");

        assertStatement("SELECT COUNT(DISTINCT aaa) AS dstCountA FROM STREAM(3, TIME) ORDER BY bbb WINDOWING(EVERY, 3, TIME, FIRST, 3, TIME)");

        assertStatement("SELECT QUANTILE(aaa, LINEAR, 11) AS quantileA FROM STREAM(3, TIME) WINDOWING(EVERY, 3, TIME, FIRST, 3, TIME)");

        assertStatement("SELECT TOP(3, aaa) AS top3A FROM STREAM(3, TIME) WINDOWING(EVERY, 3, TIME, FIRST, 3, TIME)");

        assertStatement("SELECT TOP(3, aaa) AS top3A FROM STREAM(3, TIME) WINDOWING(EVERY, 3, TIME, ALL)");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa CONTAINSKEY (1, 3, 'a')");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa NOT CONTAINSKEY (1, 3, 'a')");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa CONTAINSVALUE (1, 3, 'a')");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE aaa NOT CONTAINSVALUE (1, 3, a)");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE SIZEOF(aaa)=1");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE SIZEOF(aaa)!=1");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE SIZEOF(aaa) IS DISTINCT FROM 1");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE SIZEOF(aaa) IS NOT DISTINCT FROM 1");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE SIZEOF(aaa) IN (1, 2)");

        assertStatement("SELECT aaa AS bbb FROM STREAM(3, TIME) WHERE SIZEOF(aaa) NOT IN (1, 2)");

        assertStatement("SELECT aaa + bbb FROM STREAM(3, TIME)");

        assertStatement("SELECT aaa + bbb AS ccc FROM STREAM(3, TIME)");

        assertStatement("SELECT aaa + 5 AS bbb FROM STREAM(3, TIME)");

        assertStatement("SELECT CAST(aaa, FLOAT) AS bbb FROM STREAM(3, TIME)");

        assertStatement("SELECT CAST(aaa + 5, FLOAT) AS bbb FROM STREAM(3, TIME)");

        assertStatement("SELECT CAST((aaa + CAST(5, FLOAT)), INTEGER) + bbb AS ccc FROM STREAM(3, TIME)");
    }

    @Test
    public void testStringFormatter() {
        assertBQLFormatter("'hello world'", "'hello world'");
    }

    private static void assertStatement(String bql) {
        ParsingOptions parsingOptions = new ParsingOptions(AS_DOUBLE);
        Statement statement = BQL_PARSER.createStatement(bql, parsingOptions);
        assertFormattedBQL(BQL_PARSER, statement);
    }

    private static void assertStatementDecimal(String bql) {
        ParsingOptions parsingOptions = new ParsingOptions(AS_DECIMAL);
        Statement statement = BQL_PARSER.createStatement(bql, parsingOptions);
        assertFormattedBQLDecimal(BQL_PARSER, statement);
    }

    private static void assertBQLFormatter(String expression, String formatted) {
        Expression originalExpression = BQL_PARSER.createExpression(expression, new ParsingOptions());
        String real = BQLFormatter.formatBQL(originalExpression, Optional.empty());
        assertTrue(real.equals(formatted));
    }
}
