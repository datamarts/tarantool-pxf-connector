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

public class DataUtils {
    private DataUtils() {
    }

    public static Object mapAndValidate(int type, Object value) {
        DataType dataType = DataType.get(type);
        switch (dataType) {
            case BOOLEAN:
            case BIGINT:
            case INTEGER:
            case REAL:
            case FLOAT8:
            case TEXT:
            case VARCHAR:
                return value;
            default:
                throw new IllegalArgumentException("DataType not supported: " + dataType.name());
        }
    }
}
