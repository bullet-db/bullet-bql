/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

public class LayeredSchemaTest {
    private Schema baseSchema;

    @BeforeMethod
    public void setup() {
        baseSchema = new Schema();
        baseSchema.addField("abc", Type.INTEGER);
    }

    @Test
    public void testGetType() {
        LayeredSchema schema = new LayeredSchema(baseSchema);
        Assert.assertEquals(schema.getType("abc"), Type.INTEGER);
        Assert.assertEquals(schema.getType("foo"), Type.NULL);
    }

    @Test
    public void testGetTypeNoSchema() {
        LayeredSchema schema = new LayeredSchema(null);
        Assert.assertEquals(schema.getType("foo"), Type.UNKNOWN);
    }

    @Test
    public void testAddLayer() {
        LayeredSchema schema = new LayeredSchema(baseSchema);
        Assert.assertEquals(schema.getType("foo"), Type.NULL);
        Assert.assertEquals(schema.getType("bar"), Type.NULL);

        Schema newSchema = new Schema();
        newSchema.addField("foo", Type.FLOAT);

        schema.addLayer(newSchema, Collections.emptyMap());
        Assert.assertEquals(schema.getType("foo"), Type.FLOAT);
        Assert.assertEquals(schema.getType("bar"), Type.NULL);
    }

    @Test
    public void testAddLayerNoSchema() {
        LayeredSchema schema = new LayeredSchema(null);
        Assert.assertEquals(schema.getType("abc"), Type.UNKNOWN);

        schema.addLayer(baseSchema, Collections.emptyMap());
        Assert.assertEquals(schema.getType("abc"), Type.INTEGER);

        schema.addLayer(null, null);
        Assert.assertEquals(schema.getType("abc"), Type.UNKNOWN);
    }

    @Test
    public void testLock() {
        LayeredSchema schema = new LayeredSchema(null);
        Assert.assertEquals(schema.getType("abc"), Type.UNKNOWN);

        schema.addLayer(baseSchema, Collections.emptyMap());

        Assert.assertEquals(schema.getType("abc"), Type.INTEGER);

        schema.addLayer(new Schema(), Collections.emptyMap());

        Assert.assertEquals(schema.getType("abc"), Type.INTEGER);

        schema.lock();

        schema.addLayer(new Schema(), Collections.emptyMap());
        Assert.assertEquals(schema.getType("abc"), Type.NULL);
    }

    @Test
    public void testGetFieldNames() {
        LayeredSchema schema = new LayeredSchema(null);
        Assert.assertEquals(schema.getFieldNames(), Collections.emptySet());

        schema.addLayer(baseSchema, Collections.emptyMap());
        Assert.assertEquals(schema.getFieldNames(), Collections.singleton("abc"));

        schema.addLayer(new Schema(), Collections.emptyMap());
        Assert.assertEquals(schema.getFieldNames(), Collections.singleton("abc"));

        schema.lock();

        schema.addLayer(new Schema(), Collections.emptyMap());
        Assert.assertEquals(schema.getFieldNames(), Collections.emptySet());
    }
}
