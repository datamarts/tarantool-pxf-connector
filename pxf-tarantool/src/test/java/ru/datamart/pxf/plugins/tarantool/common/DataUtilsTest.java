/**
 * Copyright © 2022 DATAMART LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.datamart.pxf.plugins.tarantool.common;

import org.greenplum.pxf.api.io.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataUtilsTest {


    @Test
    void expectedTypesTest() {
        // arrange
        DataType[] values = DataType.values();

        EnumSet<DataType> handled = EnumSet.of(
                DataType.BOOLEAN,
                DataType.BIGINT,
                DataType.INTEGER,
                DataType.REAL,
                DataType.FLOAT8,
                DataType.TEXT,
                DataType.VARCHAR);

        EnumSet<DataType> notHandled = EnumSet.of(
                DataType.BYTEA,
                DataType.SMALLINT,
                DataType.BPCHAR,
                DataType.DATE,
                DataType.TIME,
                DataType.TIMESTAMP,
                DataType.TIMESTAMP_WITH_TIME_ZONE,
                DataType.NUMERIC,
                DataType.INT2ARRAY,
                DataType.INT4ARRAY,
                DataType.INT8ARRAY,
                DataType.BOOLARRAY,
                DataType.TEXTARRAY,
                DataType.FLOAT4ARRAY,
                DataType.FLOAT8ARRAY,
                DataType.UNSUPPORTED_TYPE
        );

        EnumSet<DataType> unexpected = EnumSet.noneOf(DataType.class);
        Integer value = 1;

        // act assert
        for (DataType dataType : values) {
            if (handled.contains(dataType)) {
                Object result = DataUtils.mapAndValidate(dataType.getOID(), value);
                assertSame(result, value);
            } else if (notHandled.contains(dataType)) {
                Assertions.assertThrows(IllegalArgumentException.class, () -> DataUtils.mapAndValidate(dataType.getOID(), value));
            } else {
                unexpected.add(dataType);
            }
        }

        assertTrue(unexpected.isEmpty(), "There is UNEXPECTED DataType, handle it and register in this test.");
    }
}