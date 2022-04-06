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

import io.tarantool.driver.api.metadata.TarantoolFieldMetadata;

public class TestTarantoolFieldMetadata implements TarantoolFieldMetadata {

    private static final long serialVersionUID = 20200708L;

    private final String fieldName;
    private final String fieldType;
    private final int fieldPosition;
    private final boolean isNullable;

    /**
     * Basic constructor.
     *
     * @param fieldName     field name
     * @param fieldType     field type (from the set of field types supported by the server)
     * @param fieldPosition field position in tuple starting from 0
     */
    public TestTarantoolFieldMetadata(String fieldName, String fieldType, int fieldPosition) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.fieldPosition = fieldPosition;
        this.isNullable = false;
    }

    /**
     * Basic constructor with isNullable parameter.
     *
     * @param fieldName     field name
     * @param fieldType     field type (from the set of field types supported by the server)
     * @param fieldPosition field position in tuple starting from 0
     * @param isNullable    is field nullable
     */
    public TestTarantoolFieldMetadata(String fieldName, String fieldType, int fieldPosition, boolean isNullable) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.fieldPosition = fieldPosition;
        this.isNullable = isNullable;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String getFieldType() {
        return fieldType;
    }

    @Override
    public int getFieldPosition() {
        return fieldPosition;
    }

    @Override
    public boolean getIsNullable() {
        return isNullable;
    }
}
