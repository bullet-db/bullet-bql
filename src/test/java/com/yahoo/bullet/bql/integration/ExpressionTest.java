/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.cast;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.list;
import static com.yahoo.bullet.bql.util.QueryUtil.nary;
import static com.yahoo.bullet.bql.util.QueryUtil.unary;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class ExpressionTest extends IntegrationTest {
    @Test
    public void testQuotedField() {
        build("SELECT \"abc\" FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc");
        Assert.assertEquals(field.getValue(), field("abc", Type.INTEGER));
    }

    @Test
    public void testFieldExpressionWithIndex() {
        build("SELECT aaa[0] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "aaa[0]");
        Assert.assertEquals(field.getValue(), field("aaa", 0, Type.STRING_MAP));
        Assert.assertEquals(field.getValue().getType(), Type.STRING_MAP);
    }

    @Test
    public void testFieldExpressionWithIndexAndSubKey() {
        build("SELECT aaa[0].def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "aaa[0].def");
        Assert.assertEquals(field.getValue(), field("aaa", 0, "def", Type.STRING));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testFieldExpressionWithIndexAndStringSubKey() {
        build("SELECT aaa[0]['def'] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "aaa[0]['def']");
        Assert.assertEquals(field.getValue(), field("aaa", 0, "def", Type.STRING));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testFieldExpressionWithIndexAndExpressionSubKey() {
        build("SELECT aaa[0][c] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "aaa[0][c]");
        Assert.assertEquals(field.getValue(), field("aaa", 0, field("c", Type.STRING), Type.STRING));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testFieldExpressionWithKey() {
        build("SELECT bbb.def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb.def");
        Assert.assertEquals(field.getValue(), field("bbb", "def", Type.STRING_MAP));
        Assert.assertEquals(field.getValue().getType(), Type.STRING_MAP);
    }

    @Test
    public void testFieldExpressionWithStringKey() {
        build("SELECT bbb['def'] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb['def']");
        Assert.assertEquals(field.getValue(), field("bbb", "def", Type.STRING_MAP));
        Assert.assertEquals(field.getValue().getType(), Type.STRING_MAP);
    }

    @Test
    public void testFieldExpressionWithExpressionKey() {
        build("SELECT bbb[c] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb[c]");
        Assert.assertEquals(field.getValue(), field("bbb", field("c", Type.STRING), Type.STRING_MAP));
        Assert.assertEquals(field.getValue().getType(), Type.STRING_MAP);
    }

    @Test
    public void testFieldExpressionWithKeyAndSubKey() {
        build("SELECT bbb.def.one FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb.def.one");
        Assert.assertEquals(field.getValue(), field("bbb", "def", "one", Type.STRING));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testFieldExpressionWithKeyAndBracketSubKey() {
        build("SELECT bbb.def['one'] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb.def['one']");
        Assert.assertEquals(field.getValue(), field("bbb", "def", "one", Type.STRING));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testFieldExpressionWithBracketKeyAndSubKey() {
        build("SELECT bbb['def'].one FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb['def'].one");
        Assert.assertEquals(field.getValue(), field("bbb", "def", "one", Type.STRING));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testFieldExpressionWithBracketKeyAndBracketSubKey() {
        build("SELECT bbb['def']['one'] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb['def']['one']");
        Assert.assertEquals(field.getValue(), field("bbb", "def", "one", Type.STRING));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testQuotedFieldExpressionWithKeyAndSubKey() {
        build("SELECT \"bbb\".\"def\".\"one\" FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb.def.one");
        Assert.assertEquals(field.getValue(), field("bbb", "def", "one", Type.STRING));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testSignedNumbers() {
        build("SELECT 5 AS a, -5 AS b, 5L AS c, -5L AS d, 5.0 AS e, -5.0 AS f, 5.0f AS g, -5.0f AS h FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 8);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("a", value(5)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("b", value(-5)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("c", value(5L)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("d", value(-5L)));
        Assert.assertEquals(query.getProjection().getFields().get(4), new Field("e", value(5.0)));
        Assert.assertEquals(query.getProjection().getFields().get(5), new Field("f", value(-5.0)));
        Assert.assertEquals(query.getProjection().getFields().get(6), new Field("g", value(5.0f)));
        Assert.assertEquals(query.getProjection().getFields().get(7), new Field("h", value(-5.0f)));
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testBoolean() {
        build("SELECT true, false FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("true", value(true)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("false", value(false)));
    }

    @Test
    public void testBinaryOperations() {
        build("SELECT a + 5, a - 5, a * 5, a / 5, a % 5, a = 5, a != 5, a > 5, a < 5, a >= 5, a <= 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 11);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("a + 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.ADD,
                                                                                                Type.LONG)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("a - 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.SUB,
                                                                                                Type.LONG)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("a * 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.MUL,
                                                                                                Type.LONG)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("a / 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.DIV,
                                                                                                Type.LONG)));
        Assert.assertEquals(query.getProjection().getFields().get(4), new Field("a % 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.MOD,
                                                                                                Type.LONG)));
        Assert.assertEquals(query.getProjection().getFields().get(5), new Field("a = 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.EQUALS,
                                                                                                Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(6), new Field("a != 5", binary(field("a", Type.LONG),
                                                                                                 value(5),
                                                                                                 Operation.NOT_EQUALS,
                                                                                                 Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(7), new Field("a > 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.GREATER_THAN,
                                                                                                Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(8), new Field("a < 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.LESS_THAN,
                                                                                                Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(9), new Field("a >= 5", binary(field("a", Type.LONG),
                                                                                                 value(5),
                                                                                                 Operation.GREATER_THAN_OR_EQUALS,
                                                                                                 Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(10), new Field("a <= 5", binary(field("a", Type.LONG),
                                                                                                  value(5),
                                                                                                  Operation.LESS_THAN_OR_EQUALS,
                                                                                                  Type.BOOLEAN)));
    }

    @Test
    public void testBinaryOperationsAnyAll() {
        build("SELECT a = ANY [5], a = ALL [5], a != ANY [5], a != ALL [5], a > ANY [5], a > ALL [5], a < ANY [5], " +
              "a < ALL [5], a >= ANY [5], a >= ALL [5], a <= ANY [5], a <= ALL [5] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 12);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("a = ANY [5]", binary(field("a", Type.LONG),
                                                                                                      list(Type.INTEGER_LIST, value(5)),
                                                                                                      Operation.EQUALS_ANY,
                                                                                                      Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("a = ALL [5]", binary(field("a", Type.LONG),
                                                                                                      list(Type.INTEGER_LIST, value(5)),
                                                                                                      Operation.EQUALS_ALL,
                                                                                                      Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("a != ANY [5]", binary(field("a", Type.LONG),
                                                                                                       list(Type.INTEGER_LIST, value(5)),
                                                                                                       Operation.NOT_EQUALS_ANY,
                                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("a != ALL [5]", binary(field("a", Type.LONG),
                                                                                                       list(Type.INTEGER_LIST, value(5)),
                                                                                                       Operation.NOT_EQUALS_ALL,
                                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(4), new Field("a > ANY [5]", binary(field("a", Type.LONG),
                                                                                                      list(Type.INTEGER_LIST, value(5)),
                                                                                                      Operation.GREATER_THAN_ANY,
                                                                                                      Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(5), new Field("a > ALL [5]", binary(field("a", Type.LONG),
                                                                                                      list(Type.INTEGER_LIST, value(5)),
                                                                                                      Operation.GREATER_THAN_ALL,
                                                                                                      Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(6), new Field("a < ANY [5]", binary(field("a", Type.LONG),
                                                                                                      list(Type.INTEGER_LIST, value(5)),
                                                                                                      Operation.LESS_THAN_ANY,
                                                                                                      Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(7), new Field("a < ALL [5]", binary(field("a", Type.LONG),
                                                                                                      list(Type.INTEGER_LIST, value(5)),
                                                                                                      Operation.LESS_THAN_ALL,
                                                                                                      Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(8), new Field("a >= ANY [5]", binary(field("a", Type.LONG),
                                                                                                       list(Type.INTEGER_LIST, value(5)),
                                                                                                       Operation.GREATER_THAN_OR_EQUALS_ANY,
                                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(9), new Field("a >= ALL [5]", binary(field("a", Type.LONG),
                                                                                                       list(Type.INTEGER_LIST, value(5)),
                                                                                                       Operation.GREATER_THAN_OR_EQUALS_ALL,
                                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(10), new Field("a <= ANY [5]", binary(field("a", Type.LONG),
                                                                                                        list(Type.INTEGER_LIST, value(5)),
                                                                                                        Operation.LESS_THAN_OR_EQUALS_ANY,
                                                                                                        Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(11), new Field("a <= ALL [5]", binary(field("a", Type.LONG),
                                                                                                        list(Type.INTEGER_LIST, value(5)),
                                                                                                        Operation.LESS_THAN_OR_EQUALS_ALL,
                                                                                                        Type.BOOLEAN)));
    }

    @Test
    public void testBinaryOperationsRegexLike() {
        build("SELECT c RLIKE 'abc', c RLIKE ANY ['abc'], c NOT RLIKE 'abc', c NOT RLIKE ANY ['abc'] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 4);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("c RLIKE 'abc'",
                                                                                binary(field("c", Type.STRING),
                                                                                       value("abc"),
                                                                                       Operation.REGEX_LIKE,
                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("c RLIKE ANY ['abc']",
                                                                                binary(field("c", Type.STRING),
                                                                                       list(Type.STRING_LIST, value("abc")),
                                                                                       Operation.REGEX_LIKE_ANY,
                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("c NOT RLIKE 'abc'",
                                                                                binary(field("c", Type.STRING),
                                                                                       value("abc"),
                                                                                       Operation.NOT_REGEX_LIKE,
                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("c NOT RLIKE ANY ['abc']",
                                                                                binary(field("c", Type.STRING),
                                                                                       list(Type.STRING_LIST, value("abc")),
                                                                                       Operation.NOT_REGEX_LIKE_ANY,
                                                                                       Type.BOOLEAN)));
    }

    @Test
    public void testBinaryOperationsIn() {
        build("SELECT 'abc' IN aaa, 'abc' NOT IN aaa FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("'abc' IN aaa",
                                                                                binary(value("abc"),
                                                                                       field("aaa", Type.STRING_MAP_LIST),
                                                                                       Operation.IN,
                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("'abc' NOT IN aaa",
                                                                                binary(value("abc"),
                                                                                       field("aaa", Type.STRING_MAP_LIST),
                                                                                       Operation.NOT_IN,
                                                                                       Type.BOOLEAN)));
    }

    @Test
    public void testBinaryOperationsInNumerics() {
        build("SELECT 1.0 IN ccc, 2 IN fff FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("1.0 IN ccc",
                                                                                binary(value(1.0),
                                                                                       field("ccc", Type.INTEGER_LIST),
                                                                                       Operation.IN,
                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("2 IN fff",
                                                                                binary(value(2),
                                                                                       field("fff", Type.DOUBLE_MAP_MAP),
                                                                                       Operation.IN,
                                                                                       Type.BOOLEAN)));
    }

    @Test
    public void testBinaryOperationsInWithParentheses() {
        build("SELECT 'abc' IN ('abc'), 'abc' NOT IN ('abc'), 'abc' IN('abc', 'def'), 'abc' NOT IN('abc', 'def') FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 4);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("'abc' IN ('abc')",
                                                                                binary(value("abc"),
                                                                                       list(Type.STRING_LIST, value("abc")),
                                                                                       Operation.IN,
                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("'abc' NOT IN ('abc')",
                                                                                binary(value("abc"),
                                                                                        list(Type.STRING_LIST, value("abc")),
                                                                                       Operation.NOT_IN,
                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("'abc' IN ('abc', 'def')",
                                                                                binary(value("abc"),
                                                                                       list(Type.STRING_LIST, value("abc"), value("def")),
                                                                                       Operation.IN,
                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("'abc' NOT IN ('abc', 'def')",
                                                                                binary(value("abc"),
                                                                                       list(Type.STRING_LIST, value("abc"), value("def")),
                                                                                       Operation.NOT_IN,
                                                                                       Type.BOOLEAN)));
    }

    @Test
    public void testBinaryOperationsMiscellaneous() {
        build("SELECT SIZEIS(c, 5), CONTAINSKEY(bbb, 'abc'), CONTAINSVALUE(aaa, 'abc'), FILTER(aaa, [true, false]), " +
              "b AND true, b OR false, b XOR true FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 7);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("SIZEIS(c, 5)", binary(field("c", Type.STRING),
                                                                                                       value(5),
                                                                                                       Operation.SIZE_IS,
                                                                                                       Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("CONTAINSKEY(bbb, 'abc')", binary(field("bbb", Type.STRING_MAP_MAP),
                                                                                                                  value("abc"),
                                                                                                                  Operation.CONTAINS_KEY,
                                                                                                                  Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("CONTAINSVALUE(aaa, 'abc')", binary(field("aaa", Type.STRING_MAP_LIST),
                                                                                                                    value("abc"),
                                                                                                                    Operation.CONTAINS_VALUE,
                                                                                                                    Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("FILTER(aaa, [true, false])", binary(field("aaa", Type.STRING_MAP_LIST),
                                                                                                                     list(Type.BOOLEAN_LIST, value(true), value(false)),
                                                                                                                     Operation.FILTER,
                                                                                                                     Type.STRING_MAP_LIST)));
        Assert.assertEquals(query.getProjection().getFields().get(4), new Field("b AND true", binary(field("b", Type.BOOLEAN),
                                                                                                     value(true),
                                                                                                     Operation.AND,
                                                                                                     Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(5), new Field("b OR false", binary(field("b", Type.BOOLEAN),
                                                                                                     value(false),
                                                                                                     Operation.OR,
                                                                                                     Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(6), new Field("b XOR true", binary(field("b", Type.BOOLEAN),
                                                                                                     value(true),
                                                                                                     Operation.XOR,
                                                                                                     Type.BOOLEAN)));
    }

    @Test
    public void testAlternativeEqualsAndNotEquals() {
        // a == 5 and a <> 5 are the exact same expressions as a = 5 and a != 5 respectively and are in fact rewritten
        // with the field names "a = 5" and "a != 5" in the record
        build("SELECT a == 5, a <> 5, a = 5, a != 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("a = 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.EQUALS,
                                                                                                Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("a != 5", binary(field("a", Type.LONG),
                                                                                                 value(5),
                                                                                                 Operation.NOT_EQUALS,
                                                                                                 Type.BOOLEAN)));
    }

    @Test
    public void testTypes() {
        build("SELECT a : STRING, b : LIST[STRING], c : MAP[STRING], d : LIST[MAP[STRING]], e : MAP[MAP[STRING]] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 5);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("a", field("a", Type.STRING)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("b", field("b", Type.STRING_LIST)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("c", field("c", Type.STRING_MAP)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("d", field("d", Type.STRING_MAP_LIST)));
        Assert.assertEquals(query.getProjection().getFields().get(4), new Field("e", field("e", Type.STRING_MAP_MAP)));
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testUnaryOperationsSizeOf() {
        build("SELECT SIZEOF(aaa), SIZEOF(c) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);

        // SIZEOF collection
        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "SIZEOF(aaa)");
        Assert.assertEquals(field.getValue(), unary(field("aaa", Type.STRING_MAP_LIST), Operation.SIZE_OF, Type.INTEGER));

        // SIZEOF string
        field = query.getProjection().getFields().get(1);

        Assert.assertEquals(field.getName(), "SIZEOF(c)");
        Assert.assertEquals(field.getValue(), unary(field("c", Type.STRING), Operation.SIZE_OF, Type.INTEGER));
    }

    @Test
    public void testUnaryOperationsNot() {
        build("SELECT NOT abc, NOT b FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);

        // NOT number
        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "NOT abc");
        Assert.assertEquals(field.getValue(), unary(field("abc", Type.INTEGER), Operation.NOT, Type.BOOLEAN));

        // NOT boolean
        field = query.getProjection().getFields().get(1);

        Assert.assertEquals(field.getName(), "NOT b");
        Assert.assertEquals(field.getValue(), unary(field("b", Type.BOOLEAN), Operation.NOT, Type.BOOLEAN));
    }

    @Test
    public void testUnaryOperationsString() {
        build("SELECT TRIM(c), LOWER(c), UPPER(c) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 3);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "TRIM(c)");
        Assert.assertEquals(field.getValue(), unary(field("c", Type.STRING), Operation.TRIM, Type.STRING));

        field = query.getProjection().getFields().get(1);

        Assert.assertEquals(field.getName(), "LOWER(c)");
        Assert.assertEquals(field.getValue(), unary(field("c", Type.STRING), Operation.LOWER, Type.STRING));

        field = query.getProjection().getFields().get(2);

        Assert.assertEquals(field.getName(), "UPPER(c)");
        Assert.assertEquals(field.getValue(), unary(field("c", Type.STRING), Operation.UPPER, Type.STRING));
    }

    @Test
    public void testUnaryOperationsAbs() {
        build("SELECT ABS(abc) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "ABS(abc)");
        Assert.assertEquals(field.getValue(), unary(field("abc", Type.INTEGER), Operation.ABS, Type.INTEGER));
    }

    @Test
    public void testUnaryOperationsHash() {
        build("SELECT HASH(bbb) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "HASH(bbb)");
        Assert.assertEquals(field.getValue(), unary(field("bbb", Type.STRING_MAP_MAP), Operation.HASH, Type.INTEGER));
    }

    @Test
    public void testListExpression() {
        build("SELECT [abc, 5, 10] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "[abc, 5, 10]");
        Assert.assertEquals(field.getValue(), list(Type.INTEGER_LIST, field("abc", Type.INTEGER),
                                                                      value(5),
                                                                      value(10)));
        Assert.assertEquals(field.getValue().getType(), Type.INTEGER_LIST);
    }

    @Test
    public void testListExpressionSubMap() {
        build("SELECT [ddd, ddd, ddd] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "[ddd, ddd, ddd]");
        Assert.assertEquals(field.getValue(), list(Type.STRING_MAP_LIST, field("ddd", Type.STRING_MAP),
                                                                         field("ddd", Type.STRING_MAP),
                                                                         field("ddd", Type.STRING_MAP)));
        Assert.assertEquals(field.getValue().getType(), Type.STRING_MAP_LIST);
    }

    @Test
    public void testEmptyListNotAllowed() {
        build("SELECT [] FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: Empty lists are currently not supported.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testListExpressionTypeCheckMultiple() {
        build("SELECT [5, 'foo'] FROM STREAM()");
        Assert.assertTrue(errors.get(0).getError().equals("1:8: The list [5, 'foo'] consists of objects of multiple types: [INTEGER, STRING].") ||
                          errors.get(0).getError().equals("1:8: The list [5, 'foo'] consists of objects of multiple types: [STRING, INTEGER]."));
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testListExpressionTypeCheckSubType() {
        build("SELECT [[5], [10]] FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The list [[5], [10]] must consist of objects of a single primitive or primitive map type. Subtype given: INTEGER_LIST.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testNAryOperations() {
        build("SELECT IF(b, 5, 10), BETWEEN(abc, 5, 10) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "IF(b, 5, 10)");
        Assert.assertEquals(field.getValue(), nary(Type.INTEGER, Operation.IF, field("b", Type.BOOLEAN),
                                                                               value(5),
                                                                               value(10)));

        field = query.getProjection().getFields().get(1);

        Assert.assertEquals(field.getName(), "BETWEEN(abc, 5, 10)");
        Assert.assertEquals(field.getValue(), nary(Type.BOOLEAN, Operation.BETWEEN, field("abc", Type.INTEGER),
                                                                                    value(5),
                                                                                    value(10)));
    }

    @Test
    public void testNAryOperationsSubstring() {
        build("SELECT SUBSTRING('abc', 5), SUBSTR('abc', 5, 10) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "SUBSTRING('abc', 5)");
        Assert.assertEquals(field.getValue(), nary(Type.STRING, Operation.SUBSTRING, value("abc"), value(5)));

        // SUBSTR -> SUBSTRING
        field = query.getProjection().getFields().get(1);

        Assert.assertEquals(field.getName(), "SUBSTRING('abc', 5, 10)");
        Assert.assertEquals(field.getValue(), nary(Type.STRING, Operation.SUBSTRING, value("abc"), value(5), value(10)));
    }

    @Test
    public void testNAryOperationsUnixTimestamp() {
        build("SELECT UNIXTIMESTAMP(), UNIXTIMESTAMP('abc'), UNIXTIMESTAMP('abc', 'def'), UNIXTIMESTAMP(123, 'def') FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 4);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "UNIXTIMESTAMP()");
        Assert.assertEquals(field.getValue(), nary(Type.LONG, Operation.UNIX_TIMESTAMP));

        field = query.getProjection().getFields().get(1);

        Assert.assertEquals(field.getName(), "UNIXTIMESTAMP('abc')");
        Assert.assertEquals(field.getValue(), nary(Type.LONG, Operation.UNIX_TIMESTAMP, value("abc")));

        field = query.getProjection().getFields().get(2);

        Assert.assertEquals(field.getName(), "UNIXTIMESTAMP('abc', 'def')");
        Assert.assertEquals(field.getValue(), nary(Type.LONG, Operation.UNIX_TIMESTAMP, value("abc"), value("def")));

        field = query.getProjection().getFields().get(3);

        Assert.assertEquals(field.getName(), "UNIXTIMESTAMP(123, 'def')");
        Assert.assertEquals(field.getValue(), nary(Type.LONG, Operation.UNIX_TIMESTAMP, value(123), value("def")));
    }

    @Test
    public void testBetweenPredicate() {
        build("SELECT abc BETWEEN (5, 10), abc NOT BETWEEN (5, 10) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc BETWEEN (5, 10)");
        Assert.assertEquals(field.getValue(), nary(Type.BOOLEAN, Operation.BETWEEN, field("abc", Type.INTEGER),
                                                                                    value(5),
                                                                                    value(10)));

        field = query.getProjection().getFields().get(1);

        Assert.assertEquals(field.getName(), "abc NOT BETWEEN (5, 10)");
        Assert.assertEquals(field.getValue(), nary(Type.BOOLEAN, Operation.NOT_BETWEEN, field("abc", Type.INTEGER),
                                                                                        value(5),
                                                                                        value(10)));
    }

    @Test
    public void testCastExpression() {
        build("SELECT CAST(abc : INTEGER AS DOUBLE) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "CAST(abc AS DOUBLE)");
        Assert.assertEquals(field.getValue(), cast(field("abc", Type.INTEGER), Type.DOUBLE, Type.DOUBLE));
    }

    @Test
    public void testCastExpressionInvalid() {
        build("SELECT CAST(aaa AS INTEGER) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: Cannot cast aaa from STRING_MAP_LIST to INTEGER.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testIsNull() {
        build("SELECT abc IS NULL FROM STREAM() WHERE abc IS NOT NULL");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc IS NULL");
        Assert.assertEquals(field.getValue(), unary(field("abc", Type.INTEGER), Operation.IS_NULL, Type.BOOLEAN));
    }

    @Test
    public void testIsNotNull() {
        build("SELECT abc IS NOT NULL FROM STREAM() WHERE abc IS NOT NULL");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc IS NOT NULL");
        Assert.assertEquals(field.getValue(), unary(field("abc", Type.INTEGER), Operation.IS_NOT_NULL, Type.BOOLEAN));
    }

    @Test
    public void testNullValue() {
        build("SELECT NULL FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("NULL", value(null))));
    }

    @Test
    public void testBooleanOperatorPrecedence() {
        build("SELECT true OR true XOR true AND true FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().get(0).getValue(), binary(value(true),
                                                                                        binary(value(true),
                                                                                               binary(value(true),
                                                                                                      value(true),
                                                                                                      Operation.AND,
                                                                                                      Type.BOOLEAN),
                                                                                               Operation.XOR,
                                                                                               Type.BOOLEAN),
                                                                                        Operation.OR,
                                                                                        Type.BOOLEAN));
    }

    @Test
    public void testInfixOperatorPrecedence() {
        build("SELECT true AND 5 / 2 BETWEEN (0, 10), 5 / 2 IN [1, 2] AND true, 5 / 2 IN (1) AND true FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().get(0).getValue(), binary(value(true),
                                                                                        nary(Type.BOOLEAN,
                                                                                             Operation.BETWEEN,
                                                                                             binary(value(5), value(2), Operation.DIV, Type.INTEGER),
                                                                                             value(0),
                                                                                             value(10)),
                                                                                        Operation.AND,
                                                                                        Type.BOOLEAN));
        Assert.assertEquals(query.getProjection().getFields().get(1).getValue(), binary(binary(binary(value(5), value(2), Operation.DIV, Type.INTEGER),
                                                                                               list(Type.INTEGER_LIST, value(1), value(2)),
                                                                                               Operation.IN,
                                                                                               Type.BOOLEAN),
                                                                                        value(true),
                                                                                        Operation.AND,
                                                                                        Type.BOOLEAN));
        Assert.assertEquals(query.getProjection().getFields().get(2).getValue(), binary(binary(binary(value(5), value(2), Operation.DIV, Type.INTEGER),
                                                                                               list(Type.INTEGER_LIST, value(1)),
                                                                                               Operation.IN,
                                                                                               Type.BOOLEAN),
                                                                                        value(true),
                                                                                        Operation.AND,
                                                                                        Type.BOOLEAN));
    }

    @Test
    public void testUnaryOperatorPrecedence() {
        build("SELECT NOT true AND false FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().get(0).getValue(), binary(unary(value(true), Operation.NOT, Type.BOOLEAN),
                                                                                        value(false),
                                                                                        Operation.AND,
                                                                                        Type.BOOLEAN));
    }
}
