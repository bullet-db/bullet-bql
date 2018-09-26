/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.BQLConfig;
import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.QuerySpecification;
import com.yahoo.bullet.common.BulletConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.classifier.QueryClassifier.QueryType.UNKNOWN;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static com.yahoo.bullet.bql.util.QueryUtil.selectList;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleQuerySpecification;
import static org.testng.Assert.assertEquals;

public class QueryExtractorTest {
    private BulletQueryBuilder builder;

    @BeforeClass
    public void setUp() {
        builder = new BulletQueryBuilder(new BulletConfig());
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Stream duration control based on record is not supported yet\\E.*")
    public void testStreamRecord() {
        builder.buildJson("SELECT aaa FROM STREAM(2000, TIME, 2000, RECORD)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:53: extraneous input '-'\\E.*")
    public void testWindowEmitNegative() {
        builder.buildJson("SELECT aaa FROM STREAM(2000, TIME) WINDOWING(EVERY, -1, TIME, ALL)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: WINDOWING doesn't support last include yet\\E.*")
    public void testWindowIncludeLast() {
        builder.buildJson("SELECT aaa FROM STREAM(2000, TIME) WINDOWING(EVERY, 1, RECORD, LAST, 1, RECORD)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: LIMIT ALL is not supported yet\\E.*")
    public void testLimitAll() {
        builder.buildJson("SELECT aaa FROM STREAM(2000, TIME) LIMIT ALL");
    }

    @Test
    public void testGroupFunctionWithoutGroupBy() {
        String countWithoutGroup = builder.buildJson("SELECT COUNT(*) FROM STREAM(2000, TIME)");
        String countWithGroup = builder.buildJson("SELECT COUNT(*) FROM STREAM(2000, TIME) GROUP BY ()");
        assertEquals(countWithGroup, countWithoutGroup);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: SELECT DISTINCT can only run with field, field.subField or field.*\\E.*")
    public void testInvalidDistinctSelect() {
        builder.buildJson("SELECT DISTINCT aaa, COUNT(*) FROM STREAM(2000, TIME)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: SELECT *, TOP_K, DISTRIBUTION, COUNT DISTINCT cannot run with other non-computation selectItems\\E.*")
    public void testCountDistinctWithOtherSelect() {
        builder.buildJson("SELECT COUNT(DISTINCT aaa, bbb), ccc FROM STREAM(2000, TIME)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: SELECT *, TOP_K, DISTRIBUTION, COUNT DISTINCT cannot run with other non-computation selectItems\\E.*")
    public void testTopKWithOtherSelect() {
        builder.buildJson("SELECT TOP(1, 3, bbb), ccc FROM STREAM(2000, TIME)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: SELECT *, TOP_K, DISTRIBUTION, COUNT DISTINCT cannot run with other non-computation selectItems\\E.*")
    public void testDistributionWithOtherSelect() {
        builder.buildJson("SELECT QUANTILE(aaa, LINEAR, 11), ccc FROM STREAM(2000, TIME)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: SELECT *, TOP_K, DISTRIBUTION, COUNT DISTINCT cannot run with other non-computation selectItems\\E.*")
    public void testSelectAllWithOtherSelect() {
        builder.buildJson("SELECT *, ccc FROM STREAM(2000, TIME)");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QNo enum constant\\E.*")
    public void testUnsupportedFunction() {
        builder.buildJson("SELECT RANDOM(aaa) FROM STREAM(2000, TIME)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: COUNT(*) does't support field\\E.*")
    public void testCountWithField() {
        builder.buildJson("SELECT COUNT(aaa, bbb) FROM STREAM(2000, TIME) GROUP BY ()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: MIN function doesn't support DISTINCT\\E.*")
    public void testMinDistinct() {
        builder.buildJson("SELECT MIN(DISTINCT aaa) FROM STREAM(2000, TIME) GROUP BY ()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: MAX function doesn't support DISTINCT\\E.*")
    public void testMaxDistinct() {
        builder.buildJson("SELECT MAX(DISTINCT aaa) FROM STREAM(2000, TIME) GROUP BY ()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: AVG function doesn't support DISTINCT\\E.*")
    public void testAvgDistinct() {
        builder.buildJson("SELECT AVG(DISTINCT aaa) FROM STREAM(2000, TIME) GROUP BY ()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: SUM function doesn't support DISTINCT\\E.*")
    public void testSumDistinct() {
        builder.buildJson("SELECT SUM(DISTINCT aaa) FROM STREAM(2000, TIME) GROUP BY ()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: MIN function requires only 1 field\\E.*")
    public void testMinMoreThanOneField() {
        builder.buildJson("SELECT MIN(aaa, bbb.ccc) FROM STREAM(2000, TIME) GROUP BY ()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: MAX function requires only 1 field\\E.*")
    public void testMaxMoreThanOneField() {
        builder.buildJson("SELECT MAX(aaa, bbb.ccc) FROM STREAM(2000, TIME) GROUP BY ()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: AVG function requires only 1 field\\E.*")
    public void testAvgMoreThanOneField() {
        builder.buildJson("SELECT AVG(aaa, bbb.ccc) FROM STREAM(2000, TIME) GROUP BY ()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: SUM function requires only 1 field\\E.*")
    public void testSumMoreThanOneField() {
        builder.buildJson("SELECT SUM(aaa, bbb.ccc) FROM STREAM(2000, TIME) GROUP BY ()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: LIMIT must be same as the k of TopK aggregation\\E.*")
    public void testTopKUnequalLimit() {
        builder.buildJson("SELECT TOP(3, 3, aaa.bbb, ccc) FROM STREAM(2000, TIME) LIMIT 10");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: LIMIT is not supported for CountDistinct aggregation\\E.*")
    public void testCountDistinctLimit() {
        builder.buildJson("SELECT COUNT(DISTINCT aaa.bbb, ccc) FROM STREAM(2000, TIME) LIMIT 10");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: GROUP BY () only supports grouping functions as selectItems\\E.*")
    public void testGroupAllField() {
        builder.buildJson("SELECT COUNT(*), aaa FROM STREAM(2000, TIME) GROUP BY ()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: NonGroup aggregation cannot be followed by GROUP BY\\E.*")
    public void testGroupByNonGroupAggregation() {
        builder.buildJson("SELECT TOP(1, aaa) FROM STREAM(2000, TIME) GROUP BY ()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: SELECT DISTINCT contains fields which are not GROUP BY fields\\E.*")
    public void testSelectDistinctDifferentGroupByField() {
        builder.buildJson("SELECT DISTINCT aaa, bbb FROM STREAM(2000, TIME) GROUP BY aaa");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: GROUP BY element (, element)* only supports grouping elements or grouping functions as selectItems\\E.*")
    public void testGroupByIncompleteSelect() {
        builder.buildJson("SELECT COUNT(*), aaa FROM STREAM(2000, TIME) GROUP BY aaa, bbb");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: NonGroup aggregation cannot be followed by GROUP BY\\E.*")
    public void testGroupBySelectAll() {
        builder.buildJson("SELECT * FROM STREAM(2000, TIME) GROUP BY aaa");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: GROUP BY element (, element)* only supports grouping elements or grouping functions as selectItems\\E.*")
    public void testGroupByInvalidSelectField() {
        builder.buildJson("SELECT COUNT(*), aaa, bbb FROM STREAM(2000, TIME) GROUP BY aaa");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: BQL cannot be classified\\E.*")
    public void testExtractNotKnownType() {
        QueryExtractor extractor = new QueryExtractor(new BQLConfig(""));
        QuerySpecification querySpecification = simpleQuerySpecification(selectList(identifier("aaa")));
        extractor.validateAndExtract(querySpecification, UNKNOWN);
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Select field aaa should be a grouping function or be in GROUP BY clause\\E.*")
    public void testGroupBySelectFieldNotInGroupByClause() {
        builder.buildJson("SELECT COUNT(*), aaa FROM STREAM(2000, TIME)");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Only casting of binary and leaf expressions supported\\E.*")
    public void testComputationCastCast() {
        builder.buildJson("SELECT CAST (CAST (a, FLOAT), FLOAT) FROM STREAM()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: CAST cannot be surrounded in parentheses\\E.*")
    public void testComputationParensCast() {
        builder.buildJson("SELECT CAST ((CAST (a, FLOAT)), FLOAT) FROM STREAM()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: SELECT DISTINCT can only run with field, field.subField or field.*\\E.*")
    public void testComputationDistinct() {
        builder.buildJson("SELECT DISTINCT a + 5 FROM STREAM()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: SELECT DISTINCT can only run with field, field.subField or field.*\\E.*")
    public void testComputationDistinctWithField() {
        builder.buildJson("SELECT DISTINCT a, a + 5 FROM STREAM()");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: GROUP BY element (, element)* only supports grouping elements or grouping functions as selectItems\\E.*")
    public void testComputationGroupBy() {
        builder.buildJson("SELECT a + 5 FROM STREAM() GROUP BY a");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: GROUP BY element (, element)* only supports grouping elements or grouping functions as selectItems\\E.*")
    public void testComputationGroupByWithField() {
        builder.buildJson("SELECT a, a + 5 FROM STREAM() GROUP BY a");
    }

}
