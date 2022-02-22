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

import ru.datamart.pxf.plugins.tarantool.common.TarantoolAccessorBase;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.metadata.TarantoolFieldMetadata;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TarantoolAccessor extends TarantoolAccessorBase implements Accessor {

    @Override
    public boolean openForWrite() throws Exception {
        super.openForWrite();

        List<TarantoolFieldMetadata> tarantoolFields = spaceOperations.getMetadata().getSpaceFormatMetadata().values().stream()
                .sorted(Comparator.comparingInt(TarantoolFieldMetadata::getFieldPosition))
                .collect(Collectors.toList());

        if (context.getColumns() != tarantoolFields.size()) {
            String externalTableColumns = IntStream.range(0, context.getColumns())
                    .mapToObj(i -> context.getColumn(i).columnName())
                    .collect(Collectors.joining(",", "[", "]"));
            String tarantoolIndex = tarantoolFields.stream()
                    .map(TarantoolFieldMetadata::getFieldName)
                    .collect(Collectors.joining(",", "[", "]"));

            throw new IllegalArgumentException(String.format("Columns don't match tarantool columns: %s, got: %s",
                    tarantoolIndex, externalTableColumns));
        }

        for (int i = 0; i < context.getColumns(); i++) {
            TarantoolFieldMetadata tarantoolColumn = tarantoolFields.get(i);
            ColumnDescriptor externalTableColumn = context.getColumn(i);

            if (!externalTableColumn.columnName().equals(tarantoolColumn.getFieldName())) {
                throw new IllegalArgumentException(String.format("Column %d (%s) not equal to tarantool column with order, expected: %s",
                        i, externalTableColumn.columnName(), tarantoolColumn.getFieldName()));
            }
        }

        return true;
    }

    @Override
    public boolean writeNextObject(OneRow oneRow) throws Exception {
        try {
            totalTasks.incrementAndGet();
            activeTasks.incrementAndGet();
            List<?> columns = (List<?>) oneRow.getData();
            TarantoolTuple tuple = connection.getTupleFactory().create(columns);
            spaceOperations.replace(tuple)
                    .whenComplete((tarantoolTuples, throwable) -> {
                        if (throwable != null) {
                            LOG.error("Task ended up with exception", throwable);
                            firstException.compareAndSet(null, throwable);
                            errorCount.incrementAndGet();
                        }
                        activeTasks.decrementAndGet();
                    });
            return true;
        } catch (Exception e) {
            LOG.error("Exception during request", e);
            return false;
        }
    }
}
