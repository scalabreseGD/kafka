/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ToArrayTest {
    private final ToArray<SinkRecord> toArrayClass = new ToArray.Key<>();
    private static final Schema KEY_SCHEMA = SchemaBuilder.struct().field("magic", Schema.INT32_SCHEMA).build();
    private static final Schema SINGLETON_LIST_KEY_SCHEMA = SchemaBuilder.array(KEY_SCHEMA).build();

    @After
    public void teardown() {
        toArrayClass.close();
    }

    @Test
    public void schemaless() {

        final SinkRecord record = new SinkRecord("test", 0, null, Collections.singletonMap("magic", 42), null, null, 0);
        final SinkRecord transformedRecord = toArrayClass.apply(record);

        assertNull(transformedRecord.keySchema());
        final List<Map<String, Integer>> key = (List<Map<String, Integer>>) transformedRecord.key();
        assertEquals(1, key.size());
        key.get(0).forEach((k, v) -> assertTrue(k.equals("magic") && v == 42));
    }

    @Test
    public void testNullSchemaless() {

        final Map<String, Object> key = null;
        final SinkRecord record = new SinkRecord("test", 0, null, key, null, null, 0);
        final SinkRecord transformedRecord = toArrayClass.apply(record);

        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
    }

    @Test
    public void withSchema() {

        final Struct key = new Struct(KEY_SCHEMA).put("magic", 42);
        final SinkRecord record = new SinkRecord("test", 0, KEY_SCHEMA, key, null, null, 0);
        final SinkRecord transformedRecord = toArrayClass.apply(record);

        assertEquals(SINGLETON_LIST_KEY_SCHEMA, transformedRecord.keySchema());
        final List<Struct> newKey = (List<Struct>) transformedRecord.key();
        assertEquals(1, newKey.size());
        assertEquals(42, newKey.get(0).getInt32("magic").intValue());    }

    @Test
    public void testNullWithSchema() {

        final Struct key = null;
        final SinkRecord record = new SinkRecord("test", 0, KEY_SCHEMA, key, null, null, 0);
        final SinkRecord transformedRecord = toArrayClass.apply(record);

        assertEquals(SINGLETON_LIST_KEY_SCHEMA, transformedRecord.keySchema());
        assertNull(transformedRecord.key());
    }

}
