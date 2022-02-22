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

import ru.datamart.pxf.plugins.tarantool.client.TarantoolConnection;
import ru.datamart.pxf.plugins.tarantool.client.TarantoolConnectionProvider;
import ru.datamart.pxf.plugins.tarantool.client.TarantoolConnectionProviderImpl;
import ru.datamart.pxf.plugins.tarantool.discovery.DiscoveryClientProvider;
import ru.datamart.pxf.plugins.tarantool.discovery.DiscoveryClusterAddressProvider;
import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies;
import org.apache.commons.lang3.StringUtils;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public abstract class TarantoolAccessorBase extends BasePlugin implements Accessor {
    private static final int DEFAULT_TIMEOUT_CONNECT = 5000;
    private static final int DEFAULT_TIMEOUT_READ = 5000;
    private static final int DEFAULT_TIMEOUT_REQUEST = 5000;

    private static final String TARANTOOL_SERVER = "tarantool.cartridge.server";
    private static final String USER = "tarantool.cartridge.user";
    private static final String PASSWORD = "tarantool.cartridge.password";
    private static final String TIMEOUT_CONNECT = "tarantool.cartridge.timeout.connect";
    private static final String TIMEOUT_READ = "tarantool.cartridge.timeout.read";
    private static final String TIMEOUT_REQUEST = "tarantool.cartridge.timeout.request";
    protected String spaceName;
    protected TarantoolConnection connection;
    protected TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> spaceOperations;

    protected AtomicLong activeTasks = new AtomicLong();
    protected AtomicLong totalTasks = new AtomicLong();
    protected AtomicLong errorCount = new AtomicLong();
    protected AtomicReference<Throwable> firstException = new AtomicReference(null);

    private DiscoveryClientProvider discoveryClientProvider = ClusterTarantoolTupleClient::new;
    private TarantoolConnectionProvider tarantoolConnectionProvider = new TarantoolConnectionProviderImpl();

    private int connectTimeout;
    private int readTimeout;
    private int requestTimeout;
    private TarantoolCredentials credentials;
    private TarantoolServerAddress routerAddress;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        spaceName = requestContext.getDataSource();
        if (StringUtils.isBlank(spaceName)) {
            throw new IllegalArgumentException("Tarantool space must be set");
        }

        String serverHostPort = configuration.get(TARANTOOL_SERVER);
        if (StringUtils.isBlank(serverHostPort)) {
            throw new IllegalArgumentException("TARANTOOL_SERVER property must be set");
        }

        this.routerAddress = new TarantoolServerAddress(serverHostPort);

        String user = configuration.get(USER, "");
        String password = configuration.get(PASSWORD, "");
        if (!user.isEmpty()) {
            this.credentials = new SimpleTarantoolCredentials(user, password);
        } else {
            this.credentials = new SimpleTarantoolCredentials();
        }

        this.connectTimeout = configuration.getInt(TIMEOUT_CONNECT, DEFAULT_TIMEOUT_CONNECT);
        this.readTimeout = configuration.getInt(TIMEOUT_READ, DEFAULT_TIMEOUT_READ);
        this.requestTimeout = configuration.getInt(TIMEOUT_REQUEST, DEFAULT_TIMEOUT_REQUEST);
    }

    @Override
    public boolean openForRead() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public OneRow readNextObject() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeForRead() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean openForWrite() throws Exception {
        LOG.info("Opening \"{}\" for write in {}. Segment: {}, total: {}",
                context.getProfile(), spaceName, context.getSegmentId(), context.getTotalSegments());
        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(credentials)
                .withConnectTimeout(connectTimeout)
                .withReadTimeout(readTimeout)
                .withRequestTimeout(requestTimeout)
                .withConnectionSelectionStrategyFactory(TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory.INSTANCE)
                .build();
        TarantoolClusterAddressProvider discoveryClusterAddressProvider = new DiscoveryClusterAddressProvider(config, routerAddress, discoveryClientProvider);
        connection = tarantoolConnectionProvider.provide(config, discoveryClusterAddressProvider);
        spaceOperations = connection.getClient().space(spaceName);
        totalTasks.set(0);
        activeTasks.set(0);
        errorCount.set(0);
        firstException.set(null);
        return true;
    }

    @Override
    public void closeForWrite() throws Exception {
        LOG.info("Closing \"{}\" for write in \"{}\". Total futures: {}, active futures: {}, segment: {}, total: {}",
                context.getProfile(), spaceName, totalTasks.get(), activeTasks.get(), context.getSegmentId(), context.getTotalSegments());

        try {
            while (activeTasks.get() > 0 && errorCount.get() == 0) {
                Thread.sleep(100L);
            }

            if (errorCount.get() > 0) {
                LOG.error("Failed \"{}\" for write in \"{}\". Errors: {}, segment: {}, total: {}",
                        context.getProfile(), spaceName, errorCount.get(), context.getSegmentId(), context.getTotalSegments(), firstException.get());
                throw new IllegalStateException("Some of the tasks completed exceptionally", firstException.get());
            }

            LOG.info("Closing \"{}\" for write in \"{}\". All futures complete, segment: {}, total: {}",
                    context.getProfile(), spaceName, context.getSegmentId(), context.getTotalSegments());
        } finally {
            totalTasks.set(0);
            activeTasks.set(0);
            errorCount.set(0);
            firstException.set(null);
            if (connection != null) {
                connection.close();
                connection = null;
                spaceOperations = null;
            }
            LOG.info("Closed \"{}\" for write in \"{}\", segment: {}, total: {}",
                    context.getProfile(), spaceName, context.getSegmentId(), context.getTotalSegments());
        }
    }

    public void setDiscoveryClientProvider(DiscoveryClientProvider discoveryClientProvider) {
        this.discoveryClientProvider = discoveryClientProvider;
    }

    public void setTarantoolConnectionProvider(TarantoolConnectionProvider tarantoolConnectionProvider) {
        this.tarantoolConnectionProvider = tarantoolConnectionProvider;
    }
}
