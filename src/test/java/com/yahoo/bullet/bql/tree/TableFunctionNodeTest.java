/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.tablefunctions.TableFunctionType;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class TableFunctionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new TableFunctionNode(TableFunctionType.EXPLODE,
                                                                    identifier("abc"),
                                                                    identifier("def"),
                                                                    identifier("ghi"),
                                                                    true,
                                                                    null),
                                        new TableFunctionNode(TableFunctionType.LATERAL_VIEW,
                                                              identifier("abc"),
                                                              identifier("def"),
                                                              identifier("ghi"),
                                                              true,
                                                              null),
                                        new TableFunctionNode(TableFunctionType.EXPLODE,
                                                              identifier(""),
                                                              identifier("def"),
                                                              identifier("ghi"),
                                                              true,
                                                              null),
                                        new TableFunctionNode(TableFunctionType.EXPLODE,
                                                              identifier("abc"),
                                                              identifier(""),
                                                              identifier("ghi"),
                                                              true,
                                                              null),
                                        new TableFunctionNode(TableFunctionType.EXPLODE,
                                                              identifier("abc"),
                                                              identifier("def"),
                                                              identifier(""),
                                                              true,
                                                              null),
                                        new TableFunctionNode(TableFunctionType.EXPLODE,
                                                              identifier("abc"),
                                                              identifier("def"),
                                                              identifier("ghi"),
                                                              false,
                                                              null));
    }
}
