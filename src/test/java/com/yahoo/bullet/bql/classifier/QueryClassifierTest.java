/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.classifier;

import com.yahoo.bullet.bql.classifier.QueryClassifier.QueryType;
import com.yahoo.bullet.bql.parser.BQLParser;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.parser.ParsingOptions;
import com.yahoo.bullet.bql.tree.Statement;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;

public class QueryClassifierTest {
    private BQLParser bqlParser;
    private QueryClassifier queryClassifier;

    @BeforeClass
    public void setUp() {
        bqlParser = new BQLParser();
        queryClassifier = new QueryClassifier();
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Only order by fields supported\\E.*")
    public void testClassifyInvalidOrderBy() {
        String bql = "SELECT * FROM STREAM(2000, TIME) ORDER BY COUNT(*) DESC LIMIT 1";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: HAVING is only supported for TOP K\\E.*")
    public void testClassifyInvalidHaving() {
        String bql = "SELECT * FROM STREAM(2000, TIME) HAVING COUNT(*) >= 2 LIMIT 1";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: NonGroup aggregation cannot be followed by GROUP BY\\E.*")
    public void testClassifyTopKGroupBy() {
        String bql = "SELECT TOP(3, ddd, aaa.cc) FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: NonGroup aggregation cannot be followed by GROUP BY\\E.*")
    public void testClassifyDistributionGroupBy() {
        String bql = "SELECT CUMFREQ(ddd, LINEAR, 11) FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: NonGroup aggregation cannot be followed by GROUP BY\\E.*")
    public void testClassifyCountDistinctGroupBy() {
        String bql = "SELECT COUNT(DISTINCT ddd, aaa) FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: NonGroup aggregation cannot be followed by GROUP BY\\E.*")
    public void testClassifySelectAllGroupBy() {
        String bql = "SELECT * FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc LIMIT 1";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: HAVING is only supported for TOP K\\E.*")
    public void testClassifyTopKLimitAll() {
        String bql = "SELECT ddd AS d, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc HAVING COUNT(*) >= 1 ORDER BY COUNT(*) DESC LIMIT ALL";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: HAVING is only supported for TOP K\\E.*")
    public void testClassifyTopKNoField() {
        String bql = "SELECT COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc HAVING COUNT(*) >= 1 ORDER BY COUNT(*) DESC LIMIT 3";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: HAVING is only supported for TOP K\\E.*")
    public void testClassifyTopKNoGroupBy() {
        String bql = "SELECT ddd AS d, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) HAVING COUNT(*) >= 1 ORDER BY COUNT(*) DESC LIMIT 3";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: HAVING is only supported for TOP K\\E.*")
    public void testClassifyTopKNoOrderBy() {
        String bql = "SELECT ddd AS d, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc HAVING COUNT(*) >= 1 LIMIT 3";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: HAVING is only supported for TOP K\\E.*")
    public void testClassifyTopKNoLimit() {
        String bql = "SELECT ddd AS d, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc HAVING COUNT(*) >= 1 ORDER BY COUNT(*) DESC";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: HAVING is only supported for TOP K\\E.*")
    public void testClassifyTopKNoGroupByNoOrderBy() {
        String bql = "SELECT ddd AS d, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) HAVING COUNT(*) >= 1 LIMIT 3";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: HAVING is only supported for TOP K\\E.*")
    public void testClassifyTopKNoGroupByNoLimit() {
        String bql = "SELECT ddd AS d, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) HAVING COUNT(*) >= 1 ORDER BY COUNT(*) DESC";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: HAVING is only supported for TOP K\\E.*")
    public void testClassifyTopKNoOrderByNoLimit() {
        String bql = "SELECT ddd AS d, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc HAVING COUNT(*) >= 1";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: HAVING is only supported for TOP K\\E.*")
    public void testClassifyTopKInvalidField() {
        String bql = "SELECT ddd AS d, *, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc HAVING COUNT(*) >= 1 ORDER BY COUNT(*) DESC LIMIT 3";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: HAVING is only supported for TOP K\\E.*")
    public void testClassifyTopKInvalidGroup() {
        String bql = "SELECT ddd AS d, aaa.cc, AVG(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc HAVING COUNT(*) >= 1 ORDER BY COUNT(*) DESC LIMIT 3";
        parseAndGetType(bql);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: For Top K, there can only be one COUNT(*)\\E.*")
    public void testClassifyTopKMoreThanOneCount() {
        String bql = "SELECT ddd AS d, aaa.cc, COUNT(*), COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc ORDER BY COUNT(*) DESC LIMIT 3";
        parseAndGetType(bql);
    }

    private QueryType parseAndGetType(String bql) {
        Statement statement = bqlParser.createStatement(bql, new ParsingOptions(AS_DOUBLE));
        return queryClassifier.classifyQuery(statement);
    }
}
