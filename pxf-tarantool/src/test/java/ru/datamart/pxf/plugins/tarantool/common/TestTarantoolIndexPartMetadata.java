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

import io.tarantool.driver.api.metadata.TarantoolIndexPartMetadata;

public class TestTarantoolIndexPartMetadata implements TarantoolIndexPartMetadata {
    private final int fieldIndex;
    private final String fieldType;
    private final String path;

    public TestTarantoolIndexPartMetadata(int fieldIndex, String fieldType) {
        this(fieldIndex, fieldType, null);
    }

    public TestTarantoolIndexPartMetadata(int fieldIndex, String fieldType, String path) {
        this.fieldIndex = fieldIndex;
        this.fieldType = fieldType;
        this.path = path;
    }

    @Override
    public int getFieldIndex() {
        return fieldIndex;
    }

    @Override
    public String getFieldType() {
        return fieldType;
    }

    @Override
    public String getPath() {
        return path;
    }
}
