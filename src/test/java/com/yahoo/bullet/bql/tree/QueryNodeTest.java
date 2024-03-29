/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

public class QueryNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                                            new StreamNode("MAX", null),
                                                            new LateralViewNode((List<TableFunctionNode>) null, null),
                                                            new LiteralNode(true, null),
                                                            new GroupByNode(Collections.emptyList(), null),
                                                            new LiteralNode(false, null),
                                                            new OrderByNode(Collections.emptyList(), null),
                                                            new WindowNode(null, null, null, null),
                                                            "50",
                                                            null),
                                        new QueryNode(new SelectNode(false, null, null),
                                                      new StreamNode("MAX", null),
                                                      new LateralViewNode((List<TableFunctionNode>) null, null),
                                                      new LiteralNode(true, null),
                                                      new GroupByNode(Collections.emptyList(), null),
                                                      new LiteralNode(false, null),
                                                      new OrderByNode(Collections.emptyList(), null),
                                                      new WindowNode(null, null, null, null),
                                                      "50",
                                                      null),
                                        new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                                      new StreamNode("2000", null),
                                                      new LateralViewNode((List<TableFunctionNode>) null, null),
                                                      new LiteralNode(true, null),
                                                      new GroupByNode(Collections.emptyList(), null),
                                                      new LiteralNode(false, null),
                                                      new OrderByNode(Collections.emptyList(), null),
                                                      new WindowNode(null, null, null, null),
                                                      "50",
                                                      null),
                                        new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                                      new StreamNode("MAX", null),
                                                      new LateralViewNode(new TableFunctionNode(null, null, null, null, false, null), null),
                                                      new LiteralNode(true, null),
                                                      new GroupByNode(Collections.emptyList(), null),
                                                      new LiteralNode(false, null),
                                                      new OrderByNode(Collections.emptyList(), null),
                                                      new WindowNode(null, null, null, null),
                                                      "50",
                                                      null),
                                        new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                                      new StreamNode("MAX", null),
                                                      new LateralViewNode((List<TableFunctionNode>) null, null),
                                                      new LiteralNode(false, null),
                                                      new GroupByNode(Collections.emptyList(), null),
                                                      new LiteralNode(false, null),
                                                      new OrderByNode(Collections.emptyList(), null),
                                                      new WindowNode(null, null, null, null),
                                                      "50",
                                                      null),
                                        new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                                      new StreamNode("MAX", null),
                                                      new LateralViewNode((List<TableFunctionNode>) null, null),
                                                      new LiteralNode(true, null),
                                                      new GroupByNode(null, null),
                                                      new LiteralNode(false, null),
                                                      new OrderByNode(Collections.emptyList(), null),
                                                      new WindowNode(null, null, null, null),
                                                      "50",
                                                      null),
                                        new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                                      new StreamNode("MAX", null),
                                                      new LateralViewNode((List<TableFunctionNode>) null, null),
                                                      new LiteralNode(true, null),
                                                      new GroupByNode(Collections.emptyList(), null),
                                                      new LiteralNode(true, null),
                                                      new OrderByNode(Collections.emptyList(), null),
                                                      new WindowNode(null, null, null, null),
                                                      "50",
                                                      null),
                                        new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                                      new StreamNode("MAX", null),
                                                      new LateralViewNode((List<TableFunctionNode>) null, null),
                                                      new LiteralNode(true, null),
                                                      new GroupByNode(Collections.emptyList(), null),
                                                      new LiteralNode(false, null),
                                                      new OrderByNode(null, null),
                                                      new WindowNode(null, null, null, null),
                                                      "50",
                                                      null),
                                        new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                                      new StreamNode("MAX", null),
                                                      new LateralViewNode((List<TableFunctionNode>) null, null),
                                                      new LiteralNode(true, null),
                                                      new GroupByNode(Collections.emptyList(), null),
                                                      new LiteralNode(false, null),
                                                      new OrderByNode(Collections.emptyList(), null),
                                                      new WindowNode(2000, null, null, null),
                                                      "50",
                                                      null),
                                        new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                                      new StreamNode("MAX", null),
                                                      new LateralViewNode((List<TableFunctionNode>) null, null),
                                                      new LiteralNode(true, null),
                                                      new GroupByNode(Collections.emptyList(), null),
                                                      new LiteralNode(false, null),
                                                      new OrderByNode(Collections.emptyList(), null),
                                                      new WindowNode(null, null, null, null),
                                                      "500",
                                                      null));
    }

    @Test
    public void testEqualsAndHashCodeWithPostQuery() {
        QueryNode nodeA = new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                        new StreamNode("MAX", null),
                                        new LateralViewNode((List<TableFunctionNode>) null, null),
                                        new LiteralNode(true, null),
                                        new GroupByNode(Collections.emptyList(), null),
                                        new LiteralNode(false, null),
                                        new OrderByNode(Collections.emptyList(), null),
                                        new WindowNode(null, null, null, null),
                                        "50",
                                        null);
        nodeA.setOuterQuery(new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                          new StreamNode("MAX", null),
                                          new LateralViewNode((List<TableFunctionNode>) null, null),
                                          new LiteralNode(true, null),
                                          new GroupByNode(Collections.emptyList(), null),
                                          new LiteralNode(false, null),
                                          new OrderByNode(Collections.emptyList(), null),
                                          new WindowNode(null, null, null, null),
                                          "50",
                                          null));
        QueryNode nodeB = new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                        new StreamNode("MAX", null),
                                        new LateralViewNode((List<TableFunctionNode>) null, null),
                                        new LiteralNode(true, null),
                                        new GroupByNode(Collections.emptyList(), null),
                                        new LiteralNode(false, null),
                                        new OrderByNode(Collections.emptyList(), null),
                                        new WindowNode(null, null, null, null),
                                        "50",
                                        null);
        nodeB.setOuterQuery(new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                          new StreamNode("MAX", null),
                                          new LateralViewNode((List<TableFunctionNode>) null, null),
                                          new LiteralNode(true, null),
                                          new GroupByNode(Collections.emptyList(), null),
                                          new LiteralNode(false, null),
                                          new OrderByNode(Collections.emptyList(), null),
                                          new WindowNode(null, null, null, null),
                                          "50",
                                          null));

        Assert.assertEquals(nodeA, nodeB);
        Assert.assertEquals(nodeA.hashCode(), nodeB.hashCode());

        nodeB.setOuterQuery(new QueryNode(new SelectNode(false, Collections.emptyList(), null),
                                          new StreamNode("MAX", null),
                                          new LateralViewNode((List<TableFunctionNode>) null, null),
                                          new LiteralNode(true, null),
                                          new GroupByNode(Collections.emptyList(), null),
                                          new LiteralNode(false, null),
                                          new OrderByNode(Collections.emptyList(), null),
                                          new WindowNode(null, null, null, null),
                                          "500",
                                          null));

        Assert.assertNotEquals(nodeA, nodeB);
        Assert.assertNotEquals(nodeA.hashCode(), nodeB.hashCode());
    }
}
