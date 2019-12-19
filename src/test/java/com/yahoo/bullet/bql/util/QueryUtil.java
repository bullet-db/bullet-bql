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
import com.yahoo.bullet.bql.tree.DoubleLiteralNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.GroupByNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
import com.yahoo.bullet.bql.tree.LinearDistributionNode;
import com.yahoo.bullet.bql.tree.ManualDistributionNode;
import com.yahoo.bullet.bql.tree.OrderByNode;
import com.yahoo.bullet.bql.tree.RegionDistributionNode;
import com.yahoo.bullet.bql.tree.SelectNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.StreamNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.WindowIncludeNode;
import com.yahoo.bullet.bql.tree.WindowNode;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.aggregations.Distribution.Type.QUANTILE;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.bql.tree.SortItemNode.Ordering.ASCENDING;
import static com.yahoo.bullet.parsing.Window.Unit.TIME;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class QueryUtil {
    private QueryUtil() {
    }

    public static IdentifierNode identifier(String name) {
        return new IdentifierNode(name);
    }

    /*public static IdentifierNode quotedIdentifier(String name) {
        return new IdentifierNode(name, true);
    }

    public static SelectItemNode unaliasedName(String name) {
        return new SingleColumn(identifier(name));
    }

    public static SelectItemNode aliasedName(String name, String alias) {
        return new SingleColumn(identifier(name), identifier(alias));
    }*/

    /*public static SelectNode selectList(ExpressionNode... expressions) {
        return selectList(asList(expressions));
    }*/

    /*public static SelectNode selectList(List<ExpressionNode> expressions) {
        ImmutableList.Builder<SelectItemNode> items = ImmutableList.builder();
        for (ExpressionNode expression : expressions) {
            items.add(new SingleColumn(expression));
        }
        return new SelectNode(false, items.build());
    }*/

    public static SelectNode selectList(SelectItemNode... items) {
        return new SelectNode(false, ImmutableList.copyOf(items));
    }

    public static SelectNode selectAll(List<SelectItemNode> items) {
        return new SelectNode(false, items);
    }

    /*public static Relation simpleStream(String duration) {
        return new StreamNode(Optional.of(duration), Optional.empty());
    }

    public static ExpressionNode logicalNot(ExpressionNode value) {
        return new NotExpression(value);
    }

    public static ExpressionNode equal(ExpressionNode left, ExpressionNode right) {
        return new ComparisonExpression(EQUALS, left, right, false);
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(SelectNode select) {
        return simpleQuery(simpleQuerySpecification(select));
    }

    public static QuerySpecification simpleQuerySpecification(SelectNode select) {
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

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(SelectNode select, Relation from) {
        return simpleQuery(select, from, Optional.empty(), Optional.empty());
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(SelectNode select, Relation from, OrderByNode orderBy) {
        return simpleQuery(select, from, Optional.empty(), Optional.of(orderBy));
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(SelectNode select, Relation from, GroupByNode groupBy) {
        return simpleQuery(select, from, Optional.empty(), Optional.of(groupBy), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(SelectNode select, Relation from, ExpressionNode where) {
        return simpleQuery(select, from, Optional.of(where), Optional.empty());
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(SelectNode select, Relation from, ExpressionNode where, OrderByNode orderBy) {
        return simpleQuery(select, from, Optional.of(where), Optional.of(orderBy));
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(SelectNode select, Relation from, Optional<ExpressionNode> where, Optional<OrderByNode> orderBy) {
        return simpleQuery(select, from, where, Optional.empty(), Optional.empty(), orderBy, Optional.empty(), Optional.empty());
    }

    public static com.yahoo.bullet.bql.tree.Query simpleQuery(SelectNode select, Relation from, Optional<ExpressionNode> where, Optional<GroupByNode> groupBy,
                                                              Optional<ExpressionNode> having, Optional<OrderByNode> orderBy, Optional<String> limit, Optional<WindowNode> windowing) {

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

    public static RegionDistributionNode regionQuantile(Double start, Double end, Double increment) {
        return new RegionDistributionNode(singletonList(
                new IdentifierNode("a")),
                QUANTILE,
                start,
                end,
                increment);
    }

    public static LinearDistributionNode linearQuantile(Long numberOfPoints) {
        return new LinearDistributionNode(singletonList(
                new IdentifierNode("a")),
                QUANTILE,
                numberOfPoints);
    }

    public static ManualDistributionNode manualQuantile(List<Double> points) {
        return new ManualDistributionNode(
                singletonList(new IdentifierNode("a")),
                QUANTILE,
                points);
    }

    public static WithQuery simpleWithQuery() {
        return new WithQuery(identifier("aaa"), simpleQuery(selectList(identifier("bbb"))), Optional.empty());
    }

    public static With simpleWith() {
        return new With(false, singletonList(simpleWithQuery()));
    }

    public static SortItemNode simpleSortItem() {
        return new SortItemNode(identifier("aaa"), ASCENDING, FIRST);
    }

    public static OrderByNode simpleOrderBy() {
        return new OrderByNode(singletonList(simpleSortItem()));
    }

    public static FunctionCall simpleFunctionCall() {
        return new FunctionCall(COUNT, singletonList(identifier("aaa")));
    }

    public static WindowIncludeNode simpleWindowInclude() {
        return new WindowIncludeNode(TIME, Optional.of(LAST), Optional.of((long) 100));
    }

    public static WindowNode simpleWindowing() {
        WindowIncludeNode include = simpleWindowInclude();
        return new WindowNode((long) 100, TIME, include);
    }

    public static BetweenPredicate simpleBetween() {
        ExpressionNode value = identifier("aaa");
        DoubleLiteralNode min = new DoubleLiteralNode("5.5");
        DoubleLiteralNode max = new DoubleLiteralNode("10");
        return new BetweenPredicate(value, min, max);
    }

    public static InPredicate simpleInPredicate() {
        return new InPredicate(identifier("bbb"), simpleValueList());
    }

    public static ListExpressionNode simpleValueList() {
        return new ListExpressionNode(singletonList(identifier("aaa")));
    }

    public static LikePredicate simpleLikePredicate() {
        return new LikePredicate(identifier("bbb"), simpleValueList(), Optional.of(identifier("ccc")));
    }


    public static TopKNode simpleTopK(long k) {
        return new TopKNode(singletonList(identifier("aaa")), k, Optional.empty());
    }*/
}
