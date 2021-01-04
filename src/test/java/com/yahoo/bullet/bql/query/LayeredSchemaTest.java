/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.query.LayeredSchema.FieldLocation;
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class LayeredSchemaTest {
    private Schema baseSchema;

    @BeforeMethod
    public void setup() {
        baseSchema = new Schema();
        baseSchema.addField("abc", Type.INTEGER);
        baseSchema.addField("bcd", Type.STRING);
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
    public void testLocking() {
        LayeredSchema schema = new LayeredSchema(null);
        Assert.assertEquals(schema.getType("abc"), Type.UNKNOWN);

        schema.addLayer(baseSchema, Collections.emptyMap());
        Assert.assertEquals(schema.getType("abc"), Type.INTEGER);

        schema.addLayer(new Schema(), Collections.emptyMap());
        Assert.assertEquals(schema.getType("abc"), Type.INTEGER);

        schema.lock();
        Assert.assertEquals(schema.getType("abc"), Type.NULL);

        schema.unlock();
        Assert.assertEquals(schema.getType("abc"), Type.INTEGER);
    }

    @Test
    public void testGettingNames() {
        LayeredSchema schema = new LayeredSchema(null);
        Assert.assertEquals(schema.getFieldNames(), Collections.emptySet());

        schema.addLayer(baseSchema, Collections.emptyMap());
        Assert.assertEquals(schema.getFieldNames(), new HashSet<>(Arrays.asList("abc", "bcd")));
        Assert.assertEquals(schema.getExtraneousAliases(), Collections.emptySet());

        schema.addLayer(new Schema(), Collections.singletonMap("abc", "foo"));
        Assert.assertEquals(schema.getFieldNames(), new HashSet<>(Arrays.asList("abc", "bcd")));
        Assert.assertEquals(schema.getExtraneousAliases(), Collections.singleton("abc"));

        schema.lock();
        Assert.assertEquals(schema.getFieldNames(), Collections.emptySet());
        Assert.assertEquals(schema.getExtraneousAliases(), Collections.singleton("abc"));
    }

    @Test
    public void testLayeringAndDepth() {
        LayeredSchema schema = new LayeredSchema(baseSchema);

        Schema middleLayer = new Schema();
        middleLayer.addField("abc", Type.STRING_MAP);
        middleLayer.addField("cde", Type.BOOLEAN);

        Schema topLayer = new Schema();
        topLayer.addField("abc", Type.STRING);
        topLayer.addField("bcd", Type.INTEGER_LIST);

        schema.addLayer(middleLayer, Collections.emptyMap());
        schema.addLayer(topLayer, Collections.emptyMap());

        FieldLocation field;

        field = schema.findField("bcd");
        Assert.assertEquals(field.getField().getName(), "bcd");
        Assert.assertEquals(field.getType(), Type.INTEGER_LIST);
        Assert.assertEquals(field.getDepth(), 0);

        field = schema.findField("bcd", 1);
        Assert.assertEquals(field.getField().getName(), "bcd");
        Assert.assertEquals(field.getType(), Type.STRING);
        Assert.assertEquals(field.getDepth(), 2);

        field = schema.findField("abc", 0);
        Assert.assertEquals(field.getField().getName(), "abc");
        Assert.assertEquals(field.getType(), Type.STRING);
        Assert.assertEquals(field.getDepth(), 0);

        field = schema.findField("abc", 1);
        Assert.assertEquals(field.getField().getName(), "abc");
        Assert.assertEquals(field.getType(), Type.STRING_MAP);
        Assert.assertEquals(field.getDepth(), 1);

        field = schema.findField("abc", 2);
        Assert.assertEquals(field.getField().getName(), "abc");
        Assert.assertEquals(field.getType(), Type.INTEGER);
        Assert.assertEquals(field.getDepth(), 2);


        schema = new LayeredSchema(baseSchema);
        schema.addLayer(middleLayer, Collections.emptyMap());
        schema.lock();
        schema.addLayer(topLayer, Collections.emptyMap());

        field = schema.findField("abc", 2);
        Assert.assertNull(field.getField());
        Assert.assertEquals(field.getType(), Type.NULL);
        Assert.assertEquals(field.getDepth(), 1);
    }
}
