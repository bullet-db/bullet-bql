package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import java.util.Collections;

public class SelectNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new SelectNode(true, Collections.emptyList()),
                              new SelectNode(false, Collections.emptyList()),
                              new SelectNode(true, null));
    }
}
