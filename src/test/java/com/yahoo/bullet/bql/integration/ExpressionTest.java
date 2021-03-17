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
        build("SELECT a + 5, a - 5, a * 5, a / 5, a = 5, a != 5, a > 5, a < 5, a >= 5, a <= 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 10);
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
        Assert.assertEquals(query.getProjection().getFields().get(4), new Field("a = 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.EQUALS,
                                                                                                Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(5), new Field("a != 5", binary(field("a", Type.LONG),
                                                                                                 value(5),
                                                                                                 Operation.NOT_EQUALS,
                                                                                                 Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(6), new Field("a > 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.GREATER_THAN,
                                                                                                Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(7), new Field("a < 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.LESS_THAN,
                                                                                                Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(8), new Field("a >= 5", binary(field("a", Type.LONG),
                                                                                                 value(5),
                                                                                                 Operation.GREATER_THAN_OR_EQUALS,
                                                                                                 Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(9), new Field("a <= 5", binary(field("a", Type.LONG),
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
    public void testBinaryOperationsMisc() {
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
    public void testUnaryExpressionSizeOfCollection() {
        build("SELECT SIZEOF(aaa) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "SIZEOF(aaa)");
        Assert.assertEquals(field.getValue(), unary(field("aaa", Type.STRING_MAP_LIST), Operation.SIZE_OF, Type.INTEGER));
    }

    @Test
    public void testUnaryExpressionSizeOfString() {
        build("SELECT SIZEOF(c) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "SIZEOF(c)");
        Assert.assertEquals(field.getValue(), unary(field("c", Type.STRING), Operation.SIZE_OF, Type.INTEGER));
    }

    @Test
    public void testUnaryExpressionSizeOfInvalid() {
        build("SELECT SIZEOF(5) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the argument in SIZEOF(5) must be some LIST, MAP, or STRING. Type given: INTEGER.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testUnaryExpressionNotNumber() {
        build("SELECT NOT abc FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "NOT abc");
        Assert.assertEquals(field.getValue(), unary(field("abc", Type.INTEGER), Operation.NOT, Type.BOOLEAN));
    }

    @Test
    public void testUnaryExpressionNotBoolean() {
        build("SELECT NOT b FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "NOT b");
        Assert.assertEquals(field.getValue(), unary(field("b", Type.BOOLEAN), Operation.NOT, Type.BOOLEAN));
    }

    @Test
    public void testUnaryExpressionNotInvalid() {
        build("SELECT NOT 'foo' FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the argument in NOT 'foo' must be numeric or BOOLEAN. Type given: STRING.");
        Assert.assertEquals(errors.size(), 1);
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
    public void testNAryExpression() {
        build("SELECT IF(b, 5, 10) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "IF(b, 5, 10)");
        Assert.assertEquals(field.getValue(), nary(Type.INTEGER, Operation.IF, field("b", Type.BOOLEAN),
                                                                               value(5),
                                                                               value(10)));
    }

    @Test
    public void testNAryExpressionBadArguments() {
        build("SELECT IF(c, 5, 10.0) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the first argument in IF(c, 5, 10.0) must be BOOLEAN. Type given: STRING.");
        Assert.assertEquals(errors.get(1).getError(), "1:8: The types of the second and third arguments in IF(c, 5, 10.0) must match. Types given: INTEGER, DOUBLE.");
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
}
