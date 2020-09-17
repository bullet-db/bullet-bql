/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TypeCheckTest extends IntegrationTest {
    @Test
    public void testTypeCheckNumericOperation() {
        build("SELECT 'foo' + 'bar', 'foo' + 0, 0 + 'foo' FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The left and right operands in 'foo' + 'bar' must be numeric. Types given: STRING, STRING");
        Assert.assertEquals(errors.get(1).getError(), "1:23: The left and right operands in 'foo' + 0 must be numeric. Types given: STRING, INTEGER");
        Assert.assertEquals(errors.get(2).getError(), "1:34: The left and right operands in 0 + 'foo' must be numeric. Types given: INTEGER, STRING");
        Assert.assertEquals(errors.size(), 3);
    }

    @Test
    public void testTypeCheckComparison() {
        build("SELECT 'foo' > 'bar', 'foo' = 0, 0 != 'foo', 'foo' = 'bar', [5] = [5L] FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The left operand in 'foo' > 'bar' must be numeric. Type given: STRING");
        Assert.assertEquals(errors.get(1).getError(), "1:8: The right operand in 'foo' > 'bar' must be numeric. Type given: STRING");
        Assert.assertEquals(errors.get(2).getError(), "1:23: The left and right operands in 'foo' = 0 must be comparable. Types given: STRING, INTEGER");
        Assert.assertEquals(errors.get(3).getError(), "1:34: The left and right operands in 0 != 'foo' must be comparable. Types given: INTEGER, STRING");
        Assert.assertEquals(errors.get(4).getError(), "1:61: The left and right operands in [5] = [5L] must be comparable. Types given: INTEGER_LIST, LONG_LIST");
        Assert.assertEquals(errors.size(), 5);
    }

    @Test
    public void testTypeCheckComparisonModifier() {
        build("SELECT 'foo' > ANY aaa, 5 > ANY ccc, 5 > ALL eee, 5 = ANY ccc, 5 = ALL 'foo', 5 = ANY aaa, 'foo' = ALL eee FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The left operand in 'foo' > ANY aaa must be numeric. Type given: STRING");
        Assert.assertEquals(errors.get(1).getError(), "1:8: The right operand in 'foo' > ANY aaa must be some numeric LIST. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.get(2).getError(), "1:38: The right operand in 5 > ALL eee must be some numeric LIST. Type given: STRING_LIST");
        Assert.assertEquals(errors.get(3).getError(), "1:64: The right operand in 5 = ALL 'foo' must be some LIST. Type given: STRING");
        Assert.assertEquals(errors.get(4).getError(), "1:79: The type of the left operand and the subtype of the right operand in 5 = ANY aaa must be comparable. Types given: INTEGER, STRING_MAP_LIST");
        Assert.assertEquals(errors.size(), 5);
    }

    @Test
    public void testTypeCheckRegexLike() {
        build("SELECT 'foo' RLIKE 0, 0 RLIKE 'foo' FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The types of the arguments in 'foo' RLIKE 0 must be STRING. Types given: STRING, INTEGER");
        Assert.assertEquals(errors.get(1).getError(), "1:23: The types of the arguments in 0 RLIKE 'foo' must be STRING. Types given: INTEGER, STRING");
        Assert.assertEquals(errors.size(), 2);
    }

    @Test
    public void testTypeCheckRegexLikeAny() {
        build("SELECT 0 RLIKE ANY 'foo' FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the left operand in 0 RLIKE ANY 'foo' must be STRING. Type given: INTEGER");
        Assert.assertEquals(errors.get(1).getError(), "1:8: The type of the right operand in 0 RLIKE ANY 'foo' must be STRING_LIST. Type given: STRING");
        Assert.assertEquals(errors.size(), 2);
    }

    @Test
    public void testTypeCheckSizeIs() {
        build("SELECT SIZEIS(abc, 5), SIZEIS(aaa, 'foo'), SIZEIS('foo', 'foo') FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the first argument in SIZEIS(abc, 5) must be some LIST, MAP, or STRING. Type given: INTEGER");
        Assert.assertEquals(errors.get(1).getError(), "1:24: The type of the second argument in SIZEIS(aaa, 'foo') must be numeric. Type given: STRING");
        Assert.assertEquals(errors.get(2).getError(), "1:44: The type of the second argument in SIZEIS('foo', 'foo') must be numeric. Type given: STRING");
        Assert.assertEquals(errors.size(), 3);
    }

    @Test
    public void testTypeCheckContainsKey() {
        build("SELECT CONTAINSKEY('foo', 5), CONTAINSKEY(aaa, 'foo'), CONTAINSKEY(bbb, 'foo') FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the first argument in CONTAINSKEY('foo', 5) must be some MAP or MAP_LIST. Type given: STRING");
        Assert.assertEquals(errors.get(1).getError(), "1:8: The type of the second argument in CONTAINSKEY('foo', 5) must be STRING. Type given: INTEGER");
        Assert.assertEquals(errors.size(), 2);
    }

    @Test
    public void testTypeCheckContainsValue() {
        build("SELECT CONTAINSVALUE('foo', aaa), CONTAINSVALUE(aaa, 5), CONTAINSVALUE(ddd, 5), CONTAINSVALUE(ddd, c) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the first argument in CONTAINSVALUE('foo', aaa) must be some LIST or MAP. Type given: STRING");
        Assert.assertEquals(errors.get(1).getError(), "1:8: The type of the second argument in CONTAINSVALUE('foo', aaa) must be primitive. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.get(2).getError(), "1:35: The primitive type of the first argument and the type of the second argument in CONTAINSVALUE(aaa, 5) must match. Types given: STRING_MAP_LIST, INTEGER");
        Assert.assertEquals(errors.get(3).getError(), "1:58: The primitive type of the first argument and the type of the second argument in CONTAINSVALUE(ddd, 5) must match. Types given: STRING_MAP, INTEGER");
        Assert.assertEquals(errors.size(), 4);
    }

    @Test
    public void testTypeCheckIn() {
        build("SELECT aaa IN 'foo', 5 IN aaa, 5 IN ddd, c IN ddd, aaa NOT IN 'foo', 5 NOT IN aaa, 5 NOT IN ddd, c NOT IN ddd FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the left operand in aaa IN 'foo' must be primitive. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.get(1).getError(), "1:8: The type of the right operand in aaa IN 'foo' must be some LIST or MAP. Type given: STRING");
        Assert.assertEquals(errors.get(2).getError(), "1:22: The type of the left operand and the primitive type of the right operand in 5 IN aaa must match. Types given: INTEGER, STRING_MAP_LIST");
        Assert.assertEquals(errors.get(3).getError(), "1:32: The type of the left operand and the primitive type of the right operand in 5 IN ddd must match. Types given: INTEGER, STRING_MAP");
        Assert.assertEquals(errors.get(4).getError(), "1:52: The type of the left operand in aaa NOT IN 'foo' must be primitive. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.get(5).getError(), "1:52: The type of the right operand in aaa NOT IN 'foo' must be some LIST or MAP. Type given: STRING");
        Assert.assertEquals(errors.get(6).getError(), "1:70: The type of the left operand and the primitive type of the right operand in 5 NOT IN aaa must match. Types given: INTEGER, STRING_MAP_LIST");
        Assert.assertEquals(errors.get(7).getError(), "1:84: The type of the left operand and the primitive type of the right operand in 5 NOT IN ddd must match. Types given: INTEGER, STRING_MAP");
        Assert.assertEquals(errors.size(), 8);
    }

    @Test
    public void testTypeCheckBooleanComparison() {
        build("SELECT 5 AND true, false OR 5, 'foo' XOR 5 FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The types of the arguments in 5 AND true must be BOOLEAN. Types given: INTEGER, BOOLEAN");
        Assert.assertEquals(errors.get(1).getError(), "1:20: The types of the arguments in false OR 5 must be BOOLEAN. Types given: BOOLEAN, INTEGER");
        Assert.assertEquals(errors.get(2).getError(), "1:32: The types of the arguments in 'foo' XOR 5 must be BOOLEAN. Types given: STRING, INTEGER");
        Assert.assertEquals(errors.size(), 3);
    }

    @Test
    public void testTypeCheckFilter() {
        build("SELECT FILTER('foo', 5) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the first argument in FILTER('foo', 5) must be some LIST. Type given: STRING");
        Assert.assertEquals(errors.get(1).getError(), "1:8: The type of the second argument in FILTER('foo', 5) must be BOOLEAN_LIST. Type given: INTEGER");
        Assert.assertEquals(errors.size(), 2);
    }

    @Test
    public void testTypeCheckDistinct() {
        build("SELECT DISTINCT aaa FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:17: The SELECT DISTINCT field aaa is non-primitive. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testTypeCheckCountDistinct() {
        build("SELECT COUNT(DISTINCT aaa) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The types of the arguments in COUNT(DISTINCT aaa) must be primitive. Types given: [STRING_MAP_LIST]");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testTypeCheckDistribution() {
        build("SELECT QUANTILE(aaa, LINEAR, 11) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the argument in QUANTILE(aaa, LINEAR, 11) must be numeric. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testTypeCheckTopK() {
        build("SELECT TOP(10, aaa) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The types of the arguments in TOP(10, aaa) must be primitive. Types given: [STRING_MAP_LIST]");
        Assert.assertEquals(errors.size(), 1);
    }
}
