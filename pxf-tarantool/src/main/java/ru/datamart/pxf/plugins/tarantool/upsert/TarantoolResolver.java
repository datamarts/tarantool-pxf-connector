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
package ru.datamart.pxf.plugins.tarantool.upsert;

import ru.datamart.pxf.plugins.tarantool.common.DataUtils;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;

import java.util.List;
import java.util.stream.Collectors;

public class TarantoolResolver extends BasePlugin implements Resolver {
    @Override
    public List<OneField> getFields(OneRow oneRow) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public OneRow setFields(List<OneField> list) throws Exception {
        List<?> fields = list.stream()
                .map(oneField -> DataUtils.mapAndValidate(oneField.type, oneField.val))
                .collect(Collectors.toList());
        return new OneRow(fields);
    }
}
