/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/QueryUtil.java
 */
package com.yahoo.bullet.bql.util;

import com.google.common.collect.ImmutableList;
import com.yahoo.bullet.bql.tree.BetweenPredicate;
import com.yahoo.bullet.bql.tree.ComparisonExpression;
import com.yahoo.bullet.bql.tree.DoubleLiteral;
import com.yahoo.bullet.bql.tree.Expression;
import com.yahoo.bullet.bql.tree.FunctionCall;
import com.yahoo.bullet.bql.tree.GroupBy;
import com.yahoo.bullet.bql.tree.Identifier;
import com.yahoo.bullet.bql.tree.InPredicate;
import com.yahoo.bullet.bql.tree.LikePredicate;
import com.yahoo.bullet.bql.tree.LinearDistribution;
import com.yahoo.bullet.bql.tree.ManualDistribution;
import com.yahoo.bullet.bql.tree.NotExpression;
import com.yahoo.bullet.bql.tree.OrderBy;
import com.yahoo.bullet.bql.tree.QueryBody;
import com.yahoo.bullet.bql.tree.QuerySpecification;
import com.yahoo.bullet.bql.tree.RegionDistribution;
import com.yahoo.bullet.bql.tree.Relation;
import com.yahoo.bullet.bql.tree.Select;
import com.yahoo.bullet.bql.tree.SelectItem;
import com.yahoo.bullet.bql.tree.SingleColumn;
import com.yahoo.bullet.bql.tree.SortItem;
import com.yahoo.bullet.bql.tree.Stream;
import com.yahoo.bullet.bql.tree.TopK;
import com.yahoo.bullet.bql.tree.ValueListExpression;
import com.yahoo.bullet.bql.tree.WindowInclude;
import com.yahoo.bullet.bql.tree.Windowing;
import com.yahoo.bullet.bql.tree.With;
import com.yahoo.bullet.bql.tree.WithQuery;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.aggregations.Distribution.Type.QUANTILE;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.bql.tree.SortItem.NullOrdering.FIRST;
import static com.yahoo.bullet.bql.tree.WindowInclude.IncludeType.LAST;
import static com.yahoo.bullet.parsing.Clause.Operation.EQUALS;
import static com.yahoo.bullet.parsing.Window.Unit.TIME;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class QueryUtil {
    private QueryUtil() {
    }

    public static Identifier identifier(String name) {
        return new Identifier(name);
    }

    public static Identifier quotedIdentifier(String name) {
        return new Identifier(name, true);
    }

    public static SelectItem unaliasedName(String name) {
        return new SingleColumn(identifier(name));
    }

    public static SelectItem aliasedName(String name, String alias) {
        return new SingleColumn(identifier(name), identifier(alias));
    }

    public static Select selectList(Expression... expressions) {
        return selectList(asList(expressions));
    }

    public static Select selectList(List<Expression> expressions) {
        ImmutableList.Builder<SelectItem> items = ImmutableList.builder();
        for (Expression expression : expressions) {
            items.add(new SingleColumn(expression));
        }
        return new Select(false, items.build());
    }

    public static Select selectList(SelectItem... items) {
        return new Select(false, ImmutableList.copyOf(items));
    }

    public static Select selectAll(List<SelectItem> items) {
        return new Select(false, items);
    }

    public static Relation simpleStream(String duration) {
        return new Stream(Optional.of(duration), Optional.empty());
    }

    public static Expression logicalNot(Expression value) {
        return new NotExpression(value);
    }

    public static Expression equal(Expression left, Expression right) {
        return new ComparisonExpression(EQUALS, left, right, false);
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(Select select) {
        return simpleQuery(simpleQuerySpecification(select));
    }

    public static QuerySpecification simpleQuerySpecification(Select select) {
        return new QuerySpecification(
                select,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(Select select, Relation from) {
        return simpleQuery(select, from, Optional.empty(), Optional.empty());
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(Select select, Relation from, OrderBy orderBy) {
        return simpleQuery(select, from, Optional.empty(), Optional.of(orderBy));
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(Select select, Relation from, GroupBy groupBy) {
        return simpleQuery(select, from, Optional.empty(), Optional.of(groupBy), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(Select select, Relation from, Expression where) {
        return simpleQuery(select, from, Optional.of(where), Optional.empty());
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(Select select, Relation from, Expression where, OrderBy orderBy) {
        return simpleQuery(select, from, Optional.of(where), Optional.of(orderBy));
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(Select select, Relation from, Optional<Expression> where, Optional<OrderBy> orderBy) {
        return simpleQuery(select, from, where, Optional.empty(), Optional.empty(), orderBy, Optional.empty(), Optional.empty());
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(Select select, Relation from, Optional<Expression> where, Optional<GroupBy> groupBy,
                                                              Optional<Expression> having, Optional<OrderBy> orderBy, Optional<String> limit, Optional<Windowing> windowing) {

        return simpleQuery(new QuerySpecification(
                select,
                Optional.of(from),
                where,
                groupBy,
                having,
                orderBy,
                limit,
                windowing));
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(QueryBody body) {
        return new com.yahoo.bullet.bql.tree.Query(
                Optional.empty(),
                body,
                Optional.empty(),
                Optional.empty());
    }

    public static RegionDistribution regionQuantile(Double start, Double end, Double increment) {
        return new RegionDistribution(singletonList(
                new Identifier("a")),
                QUANTILE,
                start,
                end,
                increment);
    }

    public static LinearDistribution linearQuantile(Long numberOfPoints) {
        return new LinearDistribution(singletonList(
                new Identifier("a")),
                QUANTILE,
                numberOfPoints);
    }

    public static ManualDistribution manualQuantile(List<Double> points) {
        return new ManualDistribution(
                singletonList(new Identifier("a")),
                QUANTILE,
                points);
    }

    public static WithQuery simpleWithQuery() {
        return new WithQuery(identifier("aaa"), simpleQuery(selectList(identifier("bbb"))), Optional.empty());
    }

    public static With simpleWith() {
        return new With(false, singletonList(simpleWithQuery()));
    }

    public static SortItem simpleSortItem() {
        return new SortItem(identifier("aaa"), FIRST);
    }

    public static OrderBy simpleOrderBy() {
        return new OrderBy(singletonList(simpleSortItem()), OrderBy.Ordering.ASCENDING);
    }

    public static FunctionCall simpleFunctionCall() {
        return new FunctionCall(COUNT, singletonList(identifier("aaa")));
    }

    public static WindowInclude simpleWindowInclude() {
        return new WindowInclude(TIME, Optional.of(LAST), Optional.of((long) 100));
    }

    public static Windowing simpleWindowing() {
        WindowInclude include = simpleWindowInclude();
        return new Windowing((long) 100, TIME, include);
    }

    public static BetweenPredicate simpleBetween() {
        Expression value = identifier("aaa");
        DoubleLiteral min = new DoubleLiteral("5.5");
        DoubleLiteral max = new DoubleLiteral("10");
        return new BetweenPredicate(value, min, max);
    }

    public static InPredicate simpleInPredicate() {
        return new InPredicate(identifier("bbb"), simpleValueList());
    }

    public static ValueListExpression simpleValueList() {
        return new ValueListExpression(singletonList(identifier("aaa")));
    }

    public static LikePredicate simpleLikePredicate() {
        return new LikePredicate(identifier("bbb"), simpleValueList(), Optional.of(identifier("ccc")));
    }


    public static TopK simpleTopK(long k) {
        return new TopK(singletonList(identifier("aaa")), k, Optional.empty());
    }
}
