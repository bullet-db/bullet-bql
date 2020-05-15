/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.Window;
import org.testng.annotations.Test;

public class WindowNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new WindowNode(5000, Window.Unit.TIME, new WindowIncludeNode(50, Window.Unit.TIME, null), null),
                                        new WindowNode(2000, Window.Unit.TIME, new WindowIncludeNode(50, Window.Unit.TIME, null), null),
                                        new WindowNode(5000, Window.Unit.RECORD, new WindowIncludeNode(50, Window.Unit.TIME, null), null),
                                        new WindowNode(5000, Window.Unit.TIME, new WindowIncludeNode(500, Window.Unit.TIME, null), null));
    }
}
