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
package ru.datamart.pxf.plugins.tarantool.client;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public final class TarantoolConnection implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TarantoolConnection.class);

    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;
    private final TarantoolTupleFactory tupleFactory;

    public TarantoolConnection(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client,
                               TarantoolTupleFactory tupleFactory) {
        this.client = client;
        this.tupleFactory = tupleFactory;
    }

    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> getClient() {
        return client;
    }

    public TarantoolTupleFactory getTupleFactory() {
        return tupleFactory;
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Interrupted on close", e);
        } catch (Exception e) {
            LOGGER.error("Exception during closing tarantool client, ignored", e);
        }
    }
}
