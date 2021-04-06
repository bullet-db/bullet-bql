package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.tablefunctions.TableFunctionType;
import org.testng.annotations.Test;

public class LateralViewNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new LateralViewNode(new TableFunctionNode(TableFunctionType.EXPLODE, null, null, null, false, null), true, null),
                                              new LateralViewNode(new TableFunctionNode(TableFunctionType.EXPLODE, null, null, null, true, null), true, null),
                                              new LateralViewNode(new TableFunctionNode(TableFunctionType.EXPLODE, null, null, null, false, null), false, null));
    }
}
