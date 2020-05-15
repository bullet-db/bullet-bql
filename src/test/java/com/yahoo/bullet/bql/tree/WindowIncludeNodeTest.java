/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.Window;
import org.testng.annotations.Test;

public class WindowIncludeNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new WindowIncludeNode(50, Window.Unit.RECORD, null),
                                        new WindowIncludeNode(500, Window.Unit.RECORD, null),
                                        new WindowIncludeNode(50, Window.Unit.TIME, null));
    }
}
