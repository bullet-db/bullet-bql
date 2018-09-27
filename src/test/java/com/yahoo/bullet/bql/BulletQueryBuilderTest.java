/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.common.BulletConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class BulletQueryBuilderTest {
    private BulletQueryBuilder builder;

    @BeforeClass
    public void setUp() {
        builder = new BulletQueryBuilder(new BulletConfig());
    }

    @Test
    public void testBuildRawAll() {
        assertEquals(builder.buildJson(
                "SELECT * FROM STREAM(2000, TIME) LIMIT 1"),
                "{\"aggregation\":{\"size\":1,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawAllWithDefaultDuration() {
        assertEquals(builder.buildJson(
                "SELECT * FROM STREAM() LIMIT 1"),
                "{\"aggregation\":{\"size\":1,\"type\":\"RAW\"}}");
    }

    @Test
    public void testBuildRawAllWithMaxDuration() {
        assertEquals(builder.buildJson(
                "SELECT * FROM STREAM(MAX, TIME) LIMIT 1"),
                "{\"aggregation\":{\"size\":1,\"type\":\"RAW\"}," +
                        "\"duration\":" + (long) Double.POSITIVE_INFINITY + "}");
    }

    @Test
    public void testBuildRawProjection() {
        assertEquals(builder.buildJson(
                "SELECT bbb AS b, aaa AS a FROM STREAM(2000, TIME) LIMIT 3"),
                "{\"projection\":{\"fields\":{\"aaa\":\"a\",\"bbb\":\"b\"}}," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildFilterWithFields() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa=bbb LIMIT 3"),
                     "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                             "\"filters\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"FIELD\",\"value\":\"bbb\"}],\"operation\":\"\\u003d\\u003d\"}]," +
                             "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                             "\"duration\":2000}");
    }

    @Test
    public void testBuildSizeOfFilter() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE SIZEOF(aaa)=4 LIMIT 3"),
                     "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                             "\"filters\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"4\"}],\"operation\":\"SIZEIS\"}]," +
                             "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                             "\"duration\":2000}");
    }

    @Test
    public void testBuildInSizeOfFilter() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE SIZEOF(aaa) IN(1, 4) LIMIT 3"),
                     "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                             "\"filters\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"1\"},{\"kind\":\"VALUE\",\"value\":\"4\"}],\"operation\":\"SIZEIS\"}]," +
                             "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                             "\"duration\":2000}");
    }

    @Test
    public void testBuildNotSizeOfFilter() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE SIZEOF(aaa) != 4 LIMIT 3"),
                     "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                             "\"filters\":[{\"clauses\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"4\"}],\"operation\":\"SIZEIS\"}],\"operation\":\"NOT\"}]," +
                             "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                             "\"duration\":2000}");
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE SIZEOF(aaa) is distinct from 4 LIMIT 3"),
                     "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                             "\"filters\":[{\"clauses\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"4\"}],\"operation\":\"SIZEIS\"}],\"operation\":\"NOT\"}]," +
                             "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                             "\"duration\":2000}");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Only '==', '!=', '<>', 'DISTINCT FROM' or 'IN' are supported in SIZEOF\\E.*")
    public void testBuildSizeOfFilterWithUnsupportedOperation() {
        builder.buildJson("SELECT aaa FROM STREAM(2000, TIME) WHERE SIZEOF(aaa) > 4 LIMIT 3");
    }


    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Only '==', '!=', '<>', 'DISTINCT FROM' or 'IN' are supported in SIZEOF\\E.*")
    public void testBuildSizeOfFilterWithBetween() {
        builder.buildJson("SELECT aaa FROM STREAM(2000, TIME) WHERE SIZEOF(aaa) between 1 and 4 LIMIT 3");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Only '==', '!=', '<>', 'DISTINCT FROM' or 'IN' are supported in SIZEOF\\E.*")
    public void testBuildSizeOfFilterWithLike() {
        builder.buildJson("SELECT aaa FROM STREAM(2000, TIME) WHERE SIZEOF(aaa) like (4) LIMIT 3");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Only '==', '!=', '<>', 'DISTINCT FROM' or 'IN' are supported in SIZEOF\\E.*")
    public void testBuildSizeOfFilterWithEmpty() {
        builder.buildJson("SELECT aaa FROM STREAM(2000, TIME) WHERE SIZEOF(aaa) is empty LIMIT 3");
    }

    @Test
    public void testBuildContainsKeyFilter() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa CONTAINSKEY(1, 4) LIMIT 3"),
                     "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                             "\"filters\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"1\"},{\"kind\":\"VALUE\",\"value\":\"4\"}],\"operation\":\"CONTAINSKEY\"}]," +
                             "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                             "\"duration\":2000}");
    }

    @Test
    public void testBuildContainsValueFilter() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa CONTAINSVALUE(1, 4) LIMIT 3"),
                     "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                             "\"filters\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"1\"},{\"kind\":\"VALUE\",\"value\":\"4\"}],\"operation\":\"CONTAINSVALUE\"}]," +
                             "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                             "\"duration\":2000}");
    }

    @Test
    public void testBuildNotContainsKeyFilter() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa NOT CONTAINSKEY(1, 4) LIMIT 3"),
                     "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                             "\"filters\":[{\"clauses\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"1\"},{\"kind\":\"VALUE\",\"value\":\"4\"}],\"operation\":\"CONTAINSKEY\"}],\"operation\":\"NOT\"}]," +
                             "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                             "\"duration\":2000}");
    }

    @Test
    public void testBuildNotContainsValueFilter() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa NOT CONTAINSVALUE(1, 4) LIMIT 3"),
                     "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                             "\"filters\":[{\"clauses\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"1\"},{\"kind\":\"VALUE\",\"value\":\"4\"}],\"operation\":\"CONTAINSVALUE\"}],\"operation\":\"NOT\"}]," +
                             "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                             "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionEqual() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa=5.12 LIMIT 3"),
                "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                        "\"filters\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"5.12\"}],\"operation\":\"\\u003d\\u003d\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionNotEqual() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa!='ccc' LIMIT 3"),
                "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                        "\"filters\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"ccc\"}],\"operation\":\"!\\u003d\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionDistinctFrom() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa IS DISTINCT FROM 'ccc' LIMIT 3"),
                "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                        "\"filters\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"ccc\"}],\"operation\":\"!\\u003d\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionNotDistinctFrom() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa IS NOT DISTINCT FROM 'ccc' LIMIT 3"),
                "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                        "\"filters\":[{\"clauses\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"ccc\"}],\"operation\":\"!\\u003d\"}],\"operation\":\"NOT\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionBetween() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WHERE ddd BETWEEN 2 AND 3 LIMIT 3"),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"filters\":[{\"clauses\":[{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"2\"}],\"operation\":\"\\u003e\\u003d\"},{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"3\"}],\"operation\":\"\\u003c\\u003d\"}],\"operation\":\"AND\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionLess() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WHERE ddd<3 LIMIT 3"),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"filters\":[{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"3\"}],\"operation\":\"\\u003c\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionLessEqual() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WHERE ddd<=3 LIMIT 3"),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"filters\":[{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"3\"}],\"operation\":\"\\u003c\\u003d\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionGreater() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WHERE ddd>2 LIMIT 3"),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"filters\":[{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"2\"}],\"operation\":\"\\u003e\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionGreaterEqual() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WHERE ddd>=2 LIMIT 3"),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"filters\":[{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"2\"}],\"operation\":\"\\u003e\\u003d\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionIn() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WHERE ddd IN (2, 3) LIMIT 3"),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"filters\":[{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"2\"},{\"kind\":\"VALUE\",\"value\":\"3\"}],\"operation\":\"\\u003d\\u003d\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionNotIn() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WHERE ddd NOT IN (1, 3, 4) LIMIT 3"),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"filters\":[{\"clauses\":[{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"1\"},{\"kind\":\"VALUE\",\"value\":\"3\"},{\"kind\":\"VALUE\",\"value\":\"4\"}],\"operation\":\"\\u003d\\u003d\"}],\"operation\":\"NOT\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionIsEmpty() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WHERE ddd IS EMPTY LIMIT 3"),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"filters\":[{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"\"}],\"operation\":\"\\u003d\\u003d\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionIsNotEmpty() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WHERE ddd IS NOT EMPTY LIMIT 3"),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"filters\":[{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"\"}],\"operation\":\"!\\u003d\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionIsNull() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WHERE ddd IS NULL LIMIT 3"),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"filters\":[{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"NULL\"}],\"operation\":\"\\u003d\\u003d\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionIsNotNull() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WHERE ddd IS NOT NULL LIMIT 3"),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"filters\":[{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"NULL\"}],\"operation\":\"!\\u003d\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionRLike() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa LIKE ('ccc[^*]', 'test') LIMIT 3"),
                "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                        "\"filters\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"ccc[^*]\"},{\"kind\":\"VALUE\",\"value\":\"test\"}],\"operation\":\"RLIKE\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionAnd() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa='ccc' AND ddd IS NOT NULL LIMIT 3"),
                "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                        "\"filters\":[{\"clauses\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"ccc\"}],\"operation\":\"\\u003d\\u003d\"},{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"NULL\"}],\"operation\":\"!\\u003d\"}],\"operation\":\"AND\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionOr() {
        assertEquals(builder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa='ccc' OR ddd IS NOT NULL LIMIT 3"),
                "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                        "\"filters\":[{\"clauses\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"ccc\"}],\"operation\":\"\\u003d\\u003d\"},{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"NULL\"}],\"operation\":\"!\\u003d\"}],\"operation\":\"OR\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionNestedFilter() {
        assertEquals(builder.buildJson(
                "SELECT * FROM STREAM(2000, TIME) WHERE (bbb='eee' AND ggg IS NULL) AND (browser_version<='test2' OR fff IS EMPTY OR hhh LIKE ('jjj', 'kkk', 'mmm')) AND (bbb IN ('test6', 'test7')) LIMIT 1"),
                "{\"filters\":[{\"clauses\":[{\"clauses\":[{\"clauses\":[{\"field\":\"bbb\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"eee\"}],\"operation\":\"\\u003d\\u003d\"},{\"field\":\"ggg\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"NULL\"}],\"operation\":\"\\u003d\\u003d\"}],\"operation\":\"AND\"},{\"clauses\":[{\"clauses\":[{\"field\":\"browser_version\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"test2\"}],\"operation\":\"\\u003c\\u003d\"},{\"field\":\"fff\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"\"}],\"operation\":\"\\u003d\\u003d\"}],\"operation\":\"OR\"},{\"field\":\"hhh\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"jjj\"},{\"kind\":\"VALUE\",\"value\":\"kkk\"},{\"kind\":\"VALUE\",\"value\":\"mmm\"}],\"operation\":\"RLIKE\"}],\"operation\":\"OR\"}],\"operation\":\"AND\"},{\"field\":\"bbb\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"test6\"},{\"kind\":\"VALUE\",\"value\":\"test7\"}],\"operation\":\"\\u003d\\u003d\"}],\"operation\":\"AND\"}]," +
                        "\"aggregation\":{\"size\":1,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildGroupByFilter() {
        assertEquals(builder.buildJson(
                "SELECT ddd AS bv, aaa AS uc FROM STREAM(2000, TIME) WHERE ddd IS NOT NULL GROUP BY ddd, aaa LIMIT 10"),
                "{\"filters\":[{\"field\":\"ddd\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"NULL\"}],\"operation\":\"!\\u003d\"}]," +
                        "\"aggregation\":{\"size\":10,\"type\":\"GROUP\",\"fields\":{\"aaa\":\"uc\",\"ddd\":\"bv\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildSelectDistinct() {
        assertEquals(builder.buildJson(
                "SELECT DISTINCT aaa AS a, bb.cc AS bc, dd.* AS d FROM STREAM(2000, TIME)"),
                "{\"aggregation\":{\"type\":\"GROUP\",\"fields\":{\"aaa\":\"a\",\"dd\":\"d\",\"bb.cc\":\"bc\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildSelectDistinctWithGroupBy() {
        assertEquals(builder.buildJson(
                "SELECT DISTINCT aaa AS a, bb.cc AS bc, dd.* AS d FROM STREAM(2000, TIME) GROUP BY aaa, bb.cc, dd"),
                "{\"aggregation\":{\"type\":\"GROUP\",\"fields\":{\"aaa\":\"a\",\"dd\":\"d\",\"bb.cc\":\"bc\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildGroupByAggregation() {
        assertEquals(builder.buildJson(
                "SELECT ddd AS bv, COUNT(*) AS count, MIN(ddd) AS min, MAX(ddd) AS max, SUM(ddd) AS sum, AVG(ddd) AS avg FROM STREAM(2000, TIME) GROUP BY ddd"),
                "{\"aggregation\":{\"type\":\"GROUP\",\"attributes\":{\"operations\":[{\"newName\":\"count\",\"type\":\"COUNT\"},{\"newName\":\"min\",\"field\":\"ddd\",\"type\":\"MIN\"},{\"newName\":\"max\",\"field\":\"ddd\",\"type\":\"MAX\"},{\"newName\":\"sum\",\"field\":\"ddd\",\"type\":\"SUM\"},{\"newName\":\"avg\",\"field\":\"ddd\",\"type\":\"AVG\"}]},\"fields\":{\"ddd\":\"bv\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildGroupAllAggregation() {
        assertEquals(builder.buildJson(
                "SELECT COUNT(*) AS count, MIN(ddd) AS min, MAX(ddd) AS max, SUM(ddd) AS sum, AVG(ddd) AS avg FROM STREAM(2000, TIME) GROUP BY ()"),
                "{\"aggregation\":{\"type\":\"GROUP\",\"attributes\":{\"operations\":[{\"newName\":\"count\",\"type\":\"COUNT\"},{\"newName\":\"min\",\"field\":\"ddd\",\"type\":\"MIN\"},{\"newName\":\"max\",\"field\":\"ddd\",\"type\":\"MAX\"},{\"newName\":\"sum\",\"field\":\"ddd\",\"type\":\"SUM\"},{\"newName\":\"avg\",\"field\":\"ddd\",\"type\":\"AVG\"}]}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildGroupAggregationWithoutAlias() {
        assertEquals(builder.buildJson(
                "SELECT COUNT(*), MIN(ddd), MAX(ddd), SUM(ddd), AVG(ddd) FROM STREAM(2000, TIME) GROUP BY ()"),
                "{\"aggregation\":{\"type\":\"GROUP\",\"attributes\":{\"operations\":[{\"type\":\"COUNT\"},{\"field\":\"ddd\",\"type\":\"MIN\"},{\"field\":\"ddd\",\"type\":\"MAX\"},{\"field\":\"ddd\",\"type\":\"SUM\"},{\"field\":\"ddd\",\"type\":\"AVG\"}]}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildCountDistinct() {
        assertEquals(builder.buildJson(
                "SELECT COUNT(DISTINCT ddd, aaa) FROM STREAM(2000, TIME)"),
                "{\"aggregation\":{\"type\":\"COUNT DISTINCT\",\"fields\":{\"aaa\":\"aaa\",\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildCountDistinctWithAlias() {
        assertEquals(builder.buildJson(
                "SELECT COUNT(DISTINCT ddd, aaa) AS countD FROM STREAM(2000, TIME)"),
                "{\"aggregation\":{\"type\":\"COUNT DISTINCT\",\"attributes\":{\"newName\":\"countD\"},\"fields\":{\"aaa\":\"aaa\",\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildQuantilesLinear() {
        assertEquals(builder.buildJson(
                "SELECT QUANTILE(ddd, LINEAR, 11) AS Q11 FROM STREAM(2000, TIME) LIMIT 4"),
                "{\"aggregation\":{\"size\":4,\"type\":\"DISTRIBUTION\",\"attributes\":{\"newName\":\"Q11\",\"numberOfPoints\":11,\"type\":\"QUANTILE\"},\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildQuantilesRegion() {
        assertEquals(builder.buildJson(
                "SELECT QUANTILE(ddd, REGION, 0, 0.9, +0.3) AS Q11 FROM STREAM(2000, TIME)"),
                "{\"aggregation\":{\"type\":\"DISTRIBUTION\",\"attributes\":{\"newName\":\"Q11\",\"start\":0.0,\"increment\":0.3,\"end\":0.9,\"type\":\"QUANTILE\"},\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildQuantilesManual() {
        assertEquals(builder.buildJson(
                "SELECT QUANTILE(ddd, MANUAL, 0, 0.3, 0.5, 0.8, 1.0) AS Q11 FROM STREAM(2000, TIME)"),
                "{\"aggregation\":{\"type\":\"DISTRIBUTION\",\"attributes\":{\"newName\":\"Q11\",\"type\":\"QUANTILE\",\"points\":[0.0,0.3,0.5,0.8,1.0]},\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildFrequenciesLinear() {
        assertEquals(builder.buildJson(
                "SELECT FREQ(ddd, LINEAR, 11) AS Q11 FROM STREAM(2000, TIME)"),
                "{\"aggregation\":{\"type\":\"DISTRIBUTION\",\"attributes\":{\"newName\":\"Q11\",\"numberOfPoints\":11,\"type\":\"PMF\"},\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildFrequenciesRegionNegative() {
        assertEquals(builder.buildJson(
                "SELECT FREQ(ddd, REGION, -100, 100, 40) AS Q11 FROM STREAM(2000, TIME)"),
                "{\"aggregation\":{\"type\":\"DISTRIBUTION\",\"attributes\":{\"newName\":\"Q11\",\"start\":-100.0,\"increment\":40.0,\"end\":100.0,\"type\":\"PMF\"},\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildFrequenciesManualNegativeDouble() {
        assertEquals(builder.buildJson(
                "SELECT FREQ(ddd, MANUAL, -100, -4e1, 40) AS Q11 FROM STREAM(2000, TIME)"),
                "{\"aggregation\":{\"type\":\"DISTRIBUTION\",\"attributes\":{\"newName\":\"Q11\",\"type\":\"PMF\",\"points\":[-100.0,-40.0,40.0]},\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildCumFrequenciesLinear() {
        assertEquals(builder.buildJson(
                "SELECT CUMFREQ(ddd, LINEAR, 11) AS Q11 FROM STREAM(2000, TIME)"),
                "{\"aggregation\":{\"type\":\"DISTRIBUTION\",\"attributes\":{\"newName\":\"Q11\",\"numberOfPoints\":11,\"type\":\"CDF\"},\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildDistributionWithoutAlias() {
        assertEquals(builder.buildJson(
                "SELECT CUMFREQ(ddd, LINEAR, 11) FROM STREAM(2000, TIME)"),
                "{\"aggregation\":{\"type\":\"DISTRIBUTION\",\"attributes\":{\"numberOfPoints\":11,\"type\":\"CDF\"},\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildTopK() {
        assertEquals(builder.buildJson(
                "SELECT ddd, aaa.cc, COUNT(*) FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc ORDER BY COUNT(*) DESC LIMIT 3"),
                "{\"aggregation\":{\"size\":3,\"type\":\"TOP K\",\"fields\":{\"aaa.cc\":\"aaa.cc\",\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildTopKAlias() {
        assertEquals(builder.buildJson(
                "SELECT ddd.*, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc ORDER BY COUNT(*) DESC LIMIT 3"),
                "{\"aggregation\":{\"size\":3,\"type\":\"TOP K\",\"attributes\":{\"newName\":\"top3\"},\"fields\":{\"aaa.cc\":\"aaa.cc\",\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildTopKWithOrderByAlias() {
        assertEquals(builder.buildJson(
                "SELECT ddd.*, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc ORDER BY top3 DESC LIMIT 3"),
                     "{\"aggregation\":{\"size\":3,\"type\":\"TOP K\",\"attributes\":{\"newName\":\"top3\"},\"fields\":{\"aaa.cc\":\"aaa.cc\",\"ddd\":\"ddd\"}}," +
                             "\"duration\":2000}");
    }

    @Test
    public void testBuildTopKThreshold() {
        assertEquals(builder.buildJson(
                "SELECT ddd AS d, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc HAVING COUNT(*) >= 1 ORDER BY COUNT(*) DESC LIMIT 3"),
                "{\"aggregation\":{\"size\":3,\"type\":\"TOP K\",\"attributes\":{\"newName\":\"top3\",\"threshold\":1},\"fields\":{\"aaa.cc\":\"aaa.cc\",\"ddd\":\"d\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildTopKThresholdWithGreaterThan() {
        assertEquals(builder.buildJson(
                "SELECT ddd AS d, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC LIMIT 3"),
                     "{\"aggregation\":{\"size\":3,\"type\":\"TOP K\",\"attributes\":{\"newName\":\"top3\",\"threshold\":2},\"fields\":{\"aaa.cc\":\"aaa.cc\",\"ddd\":\"d\"}}," +
                             "\"duration\":2000}");
    }

    @Test
    public void testBuildTopKThresholdWithAlias() {
        assertEquals(builder.buildJson(
                "SELECT ddd AS d, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc HAVING top3 >= 1 ORDER BY COUNT(*) DESC LIMIT 3"),
                     "{\"aggregation\":{\"size\":3,\"type\":\"TOP K\",\"attributes\":{\"newName\":\"top3\",\"threshold\":1},\"fields\":{\"aaa.cc\":\"aaa.cc\",\"ddd\":\"d\"}}," +
                             "\"duration\":2000}");
    }

    @Test
    public void testBuildTopKFunction() {
        assertEquals(builder.buildJson(
                "SELECT TOP(3, ddd, aaa.cc) FROM STREAM(2000, TIME)"),
                "{\"aggregation\":{\"size\":3,\"type\":\"TOP K\",\"fields\":{\"aaa.cc\":\"aaa.cc\",\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildTopKFunctionThresholdLimit() {
        assertEquals(builder.buildJson(
                "SELECT TOP(3, 1, ddd, aaa.cc) AS top3 FROM STREAM(2000, TIME) LIMIT 3"),
                "{\"aggregation\":{\"size\":3,\"type\":\"TOP K\",\"attributes\":{\"newName\":\"top3\",\"threshold\":1},\"fields\":{\"aaa.cc\":\"aaa.cc\",\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Only COUNT(*) or its alias name is supported in ORDER BY clause now\\E.*")
    public void testBuildTopKInvalidOrderBy() {
        assertEquals(builder.buildJson(
                "SELECT ddd, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc ORDER BY AVG(*) DESC LIMIT 3"),
                "{\"aggregation\":{\"size\":3,\"type\":\"TOP K\",\"attributes\":{\"newName\":\"top3\"},\"fields\":{\"aaa.cc\":\"aaa.cc\",\"ddd\":\"ddd\"}}," +
                        "\"duration\":2000}");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Only one field is supported in ORDER BY for TOP K\\E.*")
    public void testBuildTopKOrderByMultipleFields() {
        assertEquals(builder.buildJson(
                "SELECT ddd, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc ORDER BY COUNT(*), top3 DESC LIMIT 3"),
                     "{\"aggregation\":{\"size\":3,\"type\":\"TOP K\",\"attributes\":{\"newName\":\"top3\"},\"fields\":{\"aaa.cc\":\"aaa.cc\",\"ddd\":\"ddd\"}}," +
                             "\"duration\":2000}");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Only DESC is supported in ORDER BY for TOP K\\E.*")
    public void testBuildTopKOrderByASCOrder() {
        assertEquals(builder.buildJson(
                "SELECT ddd, aaa.cc, COUNT(*) AS top3 FROM STREAM(2000, TIME) GROUP BY ddd, aaa.cc ORDER BY COUNT(*) ASC LIMIT 3"),
                     "{\"aggregation\":{\"size\":3,\"type\":\"TOP K\",\"attributes\":{\"newName\":\"top3\"},\"fields\":{\"aaa.cc\":\"aaa.cc\",\"ddd\":\"ddd\"}}," +
                             "\"duration\":2000}");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Only COUNT(*) or its alias name is supported in HAVING clause now\\E.*")
    public void testBuildTopKInvalidHaving() {
        builder.buildJson(
                "SELECT aaa, bbb, COUNT(*) FROM STREAM(2000, TIME) GROUP BY aaa, bbb HAVING AVG(*) >= 4 ORDER BY COUNT(*) DESC LIMIT 3;");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Only > or >= are supported in HAVING clause\\E.*")
    public void testBuildTopKInvalidHavingOperation() {
        builder.buildJson(
                "SELECT aaa, bbb, COUNT(*) FROM STREAM(2000, TIME) GROUP BY aaa, bbb HAVING COUNT(*) < 4 ORDER BY COUNT(*) DESC LIMIT 3;");
    }

    @Test
    public void testBuildRawWindowEveryTime() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WINDOWING(EVERY, 3000, TIME, FIRST, 3000, TIME) LIMIT 5 "),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}},\"aggregation\":{\"size\":5,\"type\":\"RAW\"}," +
                        "\"window\":{\"emit\":{\"type\":\"TIME\",\"every\":3000},\"include\":{\"type\":\"TIME\",\"first\":3000}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawWindowEveryRecord() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WINDOWING(EVERY, 1, RECORD, FIRST, 1, RECORD) LIMIT 5 "),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}},\"aggregation\":{\"size\":5,\"type\":\"RAW\"}," +
                        "\"window\":{\"emit\":{\"type\":\"RECORD\",\"every\":1},\"include\":{\"type\":\"RECORD\",\"first\":1}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawTumblingTime() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WINDOWING(TUMBLING, 3000, TIME) LIMIT 5 "),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"aggregation\":{\"size\":5,\"type\":\"RAW\"}," +
                        "\"window\":{\"emit\":{\"type\":\"TIME\",\"every\":3000},\"include\":{\"type\":\"TIME\",\"first\":3000}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawTumblingRecord() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WINDOWING(TUMBLING, 3000, RECORD) LIMIT 5 "),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}}," +
                        "\"aggregation\":{\"size\":5,\"type\":\"RAW\"}," +
                        "\"window\":{\"emit\":{\"type\":\"RECORD\",\"every\":3000},\"include\":{\"type\":\"RECORD\",\"first\":3000}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawEveryWindowIncludeAll() {
        assertEquals(builder.buildJson(
                "SELECT ddd FROM STREAM(2000, TIME) WINDOWING(EVERY, 3000, TIME, ALL) LIMIT 5 "),
                "{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}},\"aggregation\":{\"size\":5,\"type\":\"RAW\"}," +
                        "\"window\":{\"emit\":{\"type\":\"TIME\",\"every\":3000},\"include\":{\"type\":\"ALL\"}}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildRawProjectionEqualDecimal() {
        BulletQueryBuilder decimalBuilder = new BulletQueryBuilder(new BQLConfig("bullet_bql_for_test_decimal.yaml"));
        assertEquals(decimalBuilder.buildJson(
                "SELECT aaa FROM STREAM(2000, TIME) WHERE aaa=5.12 LIMIT 3"),
                "{\"projection\":{\"fields\":{\"aaa\":\"aaa\"}}," +
                        "\"filters\":[{\"field\":\"aaa\",\"values\":[{\"kind\":\"VALUE\",\"value\":\"5.12\"}],\"operation\":\"\\u003d\\u003d\"}]," +
                        "\"aggregation\":{\"size\":3,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test
    public void testBuildSingleStatement() {
        assertEquals(builder.buildJson(
                "SELECT * FROM STREAM(2000, TIME) LIMIT 1;  SELECT *"),
                "{\"aggregation\":{\"size\":1,\"type\":\"RAW\"}," +
                        "\"duration\":2000}");
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: Please provide only 1 valid BQL statement\\E.*")
    public void testBuildMultipleStatement() {
        builder.buildJson(
                "SELECT * FROM STREAM(2000, TIME) LIMIT 1; SELECT * FROM STREAM(4000, TIME);");
    }

    @Test
    public void testBuildComputation() {
        assertEquals(builder.buildJson("SELECT a + b FROM STREAM()"),
                "{\"projection\":{\"fields\":{}},\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"expression\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"a\"}}," +
                        "\"right\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"b\"}},\"operation\":\"+\"},\"newName\":\"a + b\",\"type\":\"COMPUTATION\"}]}");
    }

    @Test
    public void testBuildComputationWithAs() {
        assertEquals(builder.buildJson("SELECT a + 5 AS b FROM STREAM()"),
                "{\"projection\":{\"fields\":{}},\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"expression\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"a\"}}," +
                        "\"right\":{\"value\":{\"kind\":\"VALUE\",\"value\":\"5\",\"type\":\"LONG\"}},\"operation\":\"+\"},\"newName\":\"b\",\"type\":\"COMPUTATION\"}]}");
    }

    @Test
    public void testBuildComputationWithAsterisk() {
        assertEquals(builder.buildJson("SELECT *, a + -5.0 AS b FROM STREAM()"),
                "{\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"expression\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"a\"}}," +
                        "\"right\":{\"value\":{\"kind\":\"VALUE\",\"value\":\"-5.0\",\"type\":\"DOUBLE\"}},\"operation\":\"+\"},\"newName\":\"b\",\"type\":\"COMPUTATION\"}]}");
    }

    @Test
    public void testBuildComputationWithTopK() {
        assertEquals(builder.buildJson("SELECT TOP(5, 1, a, b) AS c, c * +100 FROM STREAM()"),
                "{\"aggregation\":{\"size\":5,\"type\":\"TOP K\",\"attributes\":{\"newName\":\"c\",\"threshold\":1},\"fields\":{\"a\":\"a\",\"b\":\"b\"}}," +
                        "\"postAggregations\":[{\"expression\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"c\"}},\"right\":{\"value\":{\"kind\":\"VALUE\",\"value\":\"+100\",\"type\":\"LONG\"}}," +
                        "\"operation\":\"*\"},\"newName\":\"c * +100\",\"type\":\"COMPUTATION\"}]}");
    }

    @Test
    public void testBuildCastComputation() {
        assertEquals(builder.buildJson("SELECT CAST (a, FLOAT) AS b FROM STREAM()"),
                "{\"projection\":{\"fields\":{}},\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":" +
                        "[{\"expression\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"a\",\"type\":\"FLOAT\"}},\"newName\":\"b\",\"type\":\"COMPUTATION\"}]}");
    }

    @Test
    public void testBuildComputationPemdas() {
        assertEquals(builder.buildJson("SELECT CAST ((a) + b * ((c - (d))) / CAST ((g + (((f)))), FLOAT) - h, INTEGER) FROM STREAM()"),
                "{\"projection\":{\"fields\":{}},\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"expression\":{\"left\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"a\"}}," +
                        "\"right\":{\"left\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"b\"}},\"right\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"c\"}}," +
                        "\"right\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"d\"}},\"operation\":\"-\"},\"operation\":\"*\"},\"right\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"g\"}}," +
                        "\"right\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"f\"}},\"type\":\"FLOAT\",\"operation\":\"+\"},\"operation\":\"/\"},\"operation\":\"+\"},\"right\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"h\"}}," +
                        "\"type\":\"INTEGER\",\"operation\":\"-\"},\"newName\":\"CAST (a + b * (c - d) / CAST ((g + f), FLOAT) - h, INTEGER)\",\"type\":\"COMPUTATION\"}]}");
    }

    @Test
    public void testBuildComputationSubfield() {
        assertEquals(builder.buildJson("SELECT a.b * -5 FROM STREAM()"),
                "{\"projection\":{\"fields\":{}},\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"expression\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"a.b\"}}," +
                        "\"right\":{\"value\":{\"kind\":\"VALUE\",\"value\":\"-5\",\"type\":\"LONG\"}},\"operation\":\"*\"},\"newName\":\"a.b * -5\",\"type\":\"COMPUTATION\"}]}");
    }

    @Test
    public void testBuildComputationDouble() {
        assertEquals(builder.buildJson("SELECT a + 5.0 FROM STREAM()"),
                "{\"projection\":{\"fields\":{}},\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"expression\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"a\"}}," +
                        "\"right\":{\"value\":{\"kind\":\"VALUE\",\"value\":\"5.0\",\"type\":\"DOUBLE\"}},\"operation\":\"+\"},\"newName\":\"a + 5.0\",\"type\":\"COMPUTATION\"}]}");
    }

    @Test
    public void testBuildComputationDecimal() {
        BulletConfig config = new BulletConfig();
        config.set(BQLConfig.BQL_DECIMAL_LITERAL_TREATMENT, "AS_DECIMAL");
        builder = new BulletQueryBuilder(config);
        assertEquals(builder.buildJson("SELECT a + 5.0 FROM STREAM()"),
                "{\"projection\":{\"fields\":{}},\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"expression\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"a\"}}," +
                        "\"right\":{\"value\":{\"kind\":\"VALUE\",\"value\":\"5.0\",\"type\":\"DOUBLE\"}},\"operation\":\"+\"},\"newName\":\"a + 5.0\",\"type\":\"COMPUTATION\"}]}");
    }

    @Test
    public void testBuildComputationBoolean() {
        assertEquals(builder.buildJson("SELECT a + true FROM STREAM()"),
                "{\"projection\":{\"fields\":{}},\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"expression\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"a\"}}," +
                        "\"right\":{\"value\":{\"kind\":\"VALUE\",\"value\":\"true\",\"type\":\"BOOLEAN\"}},\"operation\":\"+\"},\"newName\":\"a + true\",\"type\":\"COMPUTATION\"}]}");
    }

    @Test
    public void testBuildComputationString() {
        assertEquals(builder.buildJson("SELECT a + 'hello' FROM STREAM()"),
                "{\"projection\":{\"fields\":{}},\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"expression\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"a\"}}," +
                        "\"right\":{\"value\":{\"kind\":\"VALUE\",\"value\":\"hello\",\"type\":\"STRING\"}},\"operation\":\"+\"},\"newName\":\"a + hello\",\"type\":\"COMPUTATION\"}]}");
    }

    @Test
    public void testBuildOrderBy() {
        assertEquals(builder.buildJson("SELECT * FROM STREAM() ORDER BY a"),
                "{\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"fields\":[{\"field\":\"a\",\"direction\":\"ASC\"}],\"type\":\"ORDERBY\"}]}");
        assertEquals(builder.buildJson("SELECT * FROM STREAM() ORDER BY a ASC"),
                "{\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"fields\":[{\"field\":\"a\",\"direction\":\"ASC\"}],\"type\":\"ORDERBY\"}]}");
        assertEquals(builder.buildJson("SELECT * FROM STREAM() ORDER BY a DESC"),
                "{\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"fields\":[{\"field\":\"a\",\"direction\":\"DESC\"}],\"type\":\"ORDERBY\"}]}");
        assertEquals(builder.buildJson("SELECT * FROM STREAM() ORDER BY a, b, c DESC"),
                "{\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"fields\":[{\"field\":\"a\",\"direction\":\"ASC\"},{\"field\":\"b\",\"direction\":\"ASC\"},{\"field\":\"c\",\"direction\":\"DESC\"}],\"type\":\"ORDERBY\"}]}");
        assertEquals(builder.buildJson("SELECT * FROM STREAM() ORDER BY a ASC, b, c DESC"),
                "{\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"fields\":[{\"field\":\"a\",\"direction\":\"ASC\"},{\"field\":\"b\",\"direction\":\"ASC\"},{\"field\":\"c\",\"direction\":\"DESC\"}],\"type\":\"ORDERBY\"}]}");
    }

    @Test
    public void testBuildOrderByWithTopK() {
        assertEquals(builder.buildJson("SELECT TOP(5, 1, a, b) AS c FROM STREAM() ORDER BY c DESC"),
                "{\"aggregation\":{\"size\":5,\"type\":\"TOP K\",\"attributes\":{\"newName\":\"c\",\"threshold\":1},\"fields\":{\"a\":\"a\",\"b\":\"b\"}}," +
                        "\"postAggregations\":[{\"fields\":[{\"field\":\"c\",\"direction\":\"DESC\"}],\"type\":\"ORDERBY\"}]}");
    }

    @Test
    public void testBuildOrderByWithComputation() {
        assertEquals(builder.buildJson("SELECT a + b AS c FROM STREAM() ORDER BY c"),
                "{\"projection\":{\"fields\":{}},\"aggregation\":{\"type\":\"RAW\"},\"postAggregations\":[{\"expression\":{\"left\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"a\"}}," +
                        "\"right\":{\"value\":{\"kind\":\"FIELD\",\"value\":\"b\"}},\"operation\":\"+\"},\"newName\":\"c\",\"type\":\"COMPUTATION\"},{\"fields\":[{\"field\":\"c\",\"direction\":\"ASC\"}],\"type\":\"ORDERBY\"}]}");
    }
}
