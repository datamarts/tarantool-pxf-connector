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
package ru.datamart.pxf.plugins.tarantool.discovery;

import io.tarantool.driver.api.*;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DiscoveryClusterAddressProvider implements TarantoolClusterAddressProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryClusterAddressProvider.class);
    private static final String DISCOVERY_COMMAND = "local cartridge = require('cartridge')\n" +
            "local function table_contains(table, element)\n" +
            "    for _, value in pairs(table) do\n" +
            "        if value == element then\n" +
            "            return true\n" +
            "        end\n" +
            "    end\n" +
            "    return false\n" +
            "end\n" +
            "\n" +
            "local servers, err = cartridge.admin_get_servers()\n" +
            "local routers = {}\n" +
            "\n" +
            "for _, server in pairs(servers) do\n" +
            "    if server.replicaset ~= nil then\n" +
            "        if table_contains(server.replicaset.roles, 'crud-router') then\n" +
            "            routers[server.uuid] = {\n" +
            "                status = server.status,\n" +
            "                uuid = server.uuid,\n" +
            "                uri = server.uri,\n" +
            "                priority = server.priority\n" +
            "            }\n" +
            "        end\n" +
            "    end\n" +
            "end\n" +
            "\n" +
            "return routers";
    private final TarantoolClientConfig config;
    private final TarantoolServerAddress routerAddress;
    private final DiscoveryClientProvider clientProvider;

    public DiscoveryClusterAddressProvider(TarantoolClientConfig config, TarantoolServerAddress routerAddress, DiscoveryClientProvider clientProvider) {
        this.config = config;
        this.routerAddress = routerAddress;
        this.clientProvider = clientProvider;
    }

    @Override
    public synchronized Collection<TarantoolServerAddress> getAddresses() {
        List<TarantoolServerAddress> tarantoolServerAddresses = new ArrayList<>();
        try (TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = clientProvider.provide(config, routerAddress)) {
            List<?> result;
            CompletableFuture<List<?>> resultFuture = client.eval(DISCOVERY_COMMAND);
            result = resultFuture.get();
            if (result.size() != 1) {
                throw new TarantoolClientException("Incorrect result of discovery call, expected: 1, got: " + result.size());
            }
            Map<String, Map<String, String>> foundServices = (Map<String, Map<String, String>>) result.get(0);
            if (foundServices.isEmpty()) {
                throw new TarantoolClientException("Could not discover servers. Result is empty.");
            }
            for (Map<String, String> value : foundServices.values()) {
                String uri = value.get("uri");
                tarantoolServerAddresses.add(new TarantoolServerAddress(uri));
            }
        } catch (Exception e) {
            LOGGER.error("Exception during discovery", e);
            throw new TarantoolClientException("Exception during discovery", e);
        }

        LOGGER.info("Successfully retrieved tarantool servers: {}", tarantoolServerAddresses);
        return tarantoolServerAddresses;
    }

    @Override
    public void close() {
        // no op
    }
}
