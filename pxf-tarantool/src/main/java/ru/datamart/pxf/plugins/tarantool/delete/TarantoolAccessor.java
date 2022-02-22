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
package ru.datamart.pxf.plugins.tarantool.delete;

import ru.datamart.pxf.plugins.tarantool.common.TarantoolAccessorBase;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolIndexPartMetadata;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TarantoolAccessor extends TarantoolAccessorBase implements Accessor {
    private static final int TARANTOOL_PRIMARY_INDEX = 0; // default in tarantool

    @Override
    public boolean openForWrite() throws Exception {
        super.openForWrite();

        Optional<TarantoolIndexMetadata> primaryIndexOptional = connection.getClient().metadata().getIndexById(spaceName, TARANTOOL_PRIMARY_INDEX);
        if (!primaryIndexOptional.isPresent()) {
            throw new IllegalArgumentException(String.format("Space: %s has no primary index", spaceName));
        }

        TarantoolIndexMetadata primaryIndex = primaryIndexOptional.get();
        List<TarantoolIndexPartMetadata> indexParts = primaryIndex.getIndexParts();

        if (indexParts.size() != context.getColumns()) {
            String externalTableColumns = IntStream.range(0, context.getColumns())
                    .mapToObj(i -> context.getColumn(i).columnName())
                    .collect(Collectors.joining(",", "[", "]"));
            String tarantoolIndex = indexParts.stream()
                    .map(TarantoolIndexPartMetadata::getPath)
                    .collect(Collectors.joining(",", "[", "]"));

            throw new IllegalArgumentException(String.format("Columns don't match tarantool primary key columns: %s, got: %s",
                    tarantoolIndex, externalTableColumns));
        }

        for (int i = 0; i < context.getColumns(); i++) {
            ColumnDescriptor externalTableColumn = context.getColumn(i);
            TarantoolIndexPartMetadata indexColumn = indexParts.get(i);
            if (!externalTableColumn.columnName().equals(indexColumn.getPath())) {
                throw new IllegalArgumentException(String.format("Column %d (%s) not equal to tarantool primary index column with order, expected: %s",
                        i, externalTableColumn.columnName(), indexColumn.getPath()));
            }
        }

        return true;
    }

    @Override
    public boolean writeNextObject(OneRow oneRow) throws Exception {
        try {
            totalTasks.incrementAndGet();
            activeTasks.incrementAndGet();
            List<?> fields = (List<?>) oneRow.getData();
            Conditions condition = Conditions.indexEquals(TARANTOOL_PRIMARY_INDEX, fields);

            spaceOperations.delete(condition)
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
