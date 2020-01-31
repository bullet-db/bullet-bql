package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import java.util.Collections;

public class GroupByNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new GroupByNode(Collections.emptyList()), new GroupByNode(null));
    }
}
