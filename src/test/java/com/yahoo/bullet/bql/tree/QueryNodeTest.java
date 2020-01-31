/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import java.util.Collections;

public class QueryNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new QueryNode(new SelectNode(false, Collections.emptyList()),
                                                  new StreamNode("MAX"),
                                                  new LiteralNode(true),
                                                  new GroupByNode(Collections.emptyList()),
                                                  new LiteralNode(false),
                                                  new OrderByNode(Collections.emptyList()),
                                                  new WindowNode(null, null, null),
                                                  "50"),
                              new QueryNode(new SelectNode(false, null),
                                            new StreamNode("MAX"),
                                            new LiteralNode(true),
                                            new GroupByNode(Collections.emptyList()),
                                            new LiteralNode(false),
                                            new OrderByNode(Collections.emptyList()),
                                            new WindowNode(null, null, null),
                                            "50"),
                              new QueryNode(new SelectNode(false, Collections.emptyList()),
                                            new StreamNode("2000"),
                                            new LiteralNode(true),
                                            new GroupByNode(Collections.emptyList()),
                                            new LiteralNode(false),
                                            new OrderByNode(Collections.emptyList()),
                                            new WindowNode(null, null, null),
                                            "50"),
                              new QueryNode(new SelectNode(false, Collections.emptyList()),
                                            new StreamNode("MAX"),
                                            new LiteralNode(false),
                                            new GroupByNode(Collections.emptyList()),
                                            new LiteralNode(false),
                                            new OrderByNode(Collections.emptyList()),
                                            new WindowNode(null, null, null),
                                            "50"),
                              new QueryNode(new SelectNode(false, Collections.emptyList()),
                                            new StreamNode("MAX"),
                                            new LiteralNode(true),
                                            new GroupByNode(null),
                                            new LiteralNode(false),
                                            new OrderByNode(Collections.emptyList()),
                                            new WindowNode(null, null, null),
                                            "50"),
                              new QueryNode(new SelectNode(false, Collections.emptyList()),
                                            new StreamNode("MAX"),
                                            new LiteralNode(true),
                                            new GroupByNode(Collections.emptyList()),
                                            new LiteralNode(true),
                                            new OrderByNode(Collections.emptyList()),
                                            new WindowNode(null, null, null),
                                            "50"),
                              new QueryNode(new SelectNode(false, Collections.emptyList()),
                                            new StreamNode("MAX"),
                                            new LiteralNode(true),
                                            new GroupByNode(Collections.emptyList()),
                                            new LiteralNode(false),
                                            new OrderByNode(null),
                                            new WindowNode(null, null, null),
                                            "50"),
                              new QueryNode(new SelectNode(false, Collections.emptyList()),
                                            new StreamNode("MAX"),
                                            new LiteralNode(true),
                                            new GroupByNode(Collections.emptyList()),
                                            new LiteralNode(false),
                                            new OrderByNode(Collections.emptyList()),
                                            new WindowNode(2000L, null, null),
                                            "50"),
                              new QueryNode(new SelectNode(false, Collections.emptyList()),
                                            new StreamNode("MAX"),
                                            new LiteralNode(true),
                                            new GroupByNode(Collections.emptyList()),
                                            new LiteralNode(false),
                                            new OrderByNode(Collections.emptyList()),
                                            new WindowNode(null, null, null),
                                            "500"));
    }
}
