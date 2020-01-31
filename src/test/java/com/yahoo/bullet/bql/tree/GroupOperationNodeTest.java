/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class GroupOperationNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new GroupOperationNode(GroupOperation.GroupOperationType.AVG, identifier("abc")),
                              new GroupOperationNode(GroupOperation.GroupOperationType.SUM, identifier("abc")),
                              new GroupOperationNode(GroupOperation.GroupOperationType.AVG, identifier("def")));
    }
}
