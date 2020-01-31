/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.parsing.Window;
import org.testng.annotations.Test;

public class WindowNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new WindowNode(5000L, Window.Unit.TIME, new WindowIncludeNode(50L, Window.Unit.TIME)),
                              new WindowNode(2000L, Window.Unit.TIME, new WindowIncludeNode(50L, Window.Unit.TIME)),
                              new WindowNode(5000L, Window.Unit.RECORD, new WindowIncludeNode(50L, Window.Unit.TIME)),
                              new WindowNode(5000L, Window.Unit.TIME, new WindowIncludeNode(500L, Window.Unit.TIME)));
    }
}
