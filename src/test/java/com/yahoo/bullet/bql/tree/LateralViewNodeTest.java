/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.tablefunctions.TableFunctionType;
import org.testng.annotations.Test;

public class LateralViewNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new LateralViewNode(new TableFunctionNode(TableFunctionType.EXPLODE, null, null, null, false, null), null),
                                              new LateralViewNode(new TableFunctionNode(TableFunctionType.EXPLODE, null, null, null, true, null), null));
    }
}
