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

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DiscoveryClusterAddressProviderTest {
    private static final String HOST = "127.0.0.1:1343";

    private static final Map<String, Map<String, String>> CORRECT_RESULT = new HashMap<>();
    private static final Map<String, Map<String, String>> EMPTY_RESULT = emptyMap();
    private static final Map<String, Map<String, String>> INCORRECT_RESULT = new HashMap<>();

    static {
        HashMap<String, String> address1 = new HashMap<>();
        address1.put("uri", "localhost:1111");

        HashMap<String, String> address2 = new HashMap<>();
        address2.put("uri", "10.10.10.10:2222");

        CORRECT_RESULT.put("key1", address1);
        CORRECT_RESULT.put("key2", address2);

        Map<String, String> incorrectAddress = new HashMap<>();
        incorrectAddress.put("uri", "::::");
        INCORRECT_RESULT.put("key1", incorrectAddress);
    }

    @Mock
    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient;

    @Mock
    private DiscoveryClientProvider discoveryClientProvider;

    private DiscoveryClusterAddressProvider discoveryClusterAddressProvider;

    @BeforeEach
    void setUp() {
        TarantoolClientConfig build = TarantoolClientConfig.builder().build();
        discoveryClusterAddressProvider = new DiscoveryClusterAddressProvider(build, new TarantoolServerAddress(HOST), discoveryClientProvider);
        when(discoveryClientProvider.provide(Mockito.any(), Mockito.any())).thenReturn(tarantoolClient);
    }

    @Test
    void discoverServersSuccess() throws Exception {
        // arrange
        when(tarantoolClient.eval(Mockito.anyString())).thenReturn(CompletableFuture.completedFuture(asList(CORRECT_RESULT)));

        // act
        Collection<TarantoolServerAddress> addresses = discoveryClusterAddressProvider.getAddresses();

        // assert
        verify(tarantoolClient).eval(Mockito.anyString());
        verify(tarantoolClient).close();
        assertThat(addresses, containsInAnyOrder(
                allOf(
                        hasProperty("host", is("localhost")),
                        hasProperty("port", is(1111))
                ),
                allOf(
                        hasProperty("host", is("10.10.10.10")),
                        hasProperty("port", is(2222))
                )
        ));
    }

    @Test
    void failOnCloseAfterSuccessfulCall() throws Exception {
        // arrange
        when(tarantoolClient.eval(Mockito.anyString())).thenReturn(CompletableFuture.completedFuture(asList(CORRECT_RESULT)));
        doThrow(new RuntimeException("Exception")).when(tarantoolClient).close();

        // act
        assertThrows(TarantoolClientException.class, () -> discoveryClusterAddressProvider.getAddresses());

        // assert
        verify(tarantoolClient).eval(Mockito.anyString());
        verify(tarantoolClient).close();
    }

    @Test
    void shouldThrowWhenExceptionDuringClientConstruction() throws Exception {
        // arrange
        when(tarantoolClient.eval(Mockito.anyString())).thenThrow(new RuntimeException("Exception"));

        // act
        assertThrows(RuntimeException.class, () -> discoveryClusterAddressProvider.getAddresses());

        // assert
        verify(tarantoolClient).eval(Mockito.anyString());
        verify(tarantoolClient).close();
    }

    @Test
    void shouldThrowWhenExceptionDuringEval() {
        // arrange
        when(discoveryClientProvider.provide(Mockito.any(), Mockito.any())).thenThrow(new RuntimeException("Exception"));

        // act
        assertThrows(TarantoolClientException.class, () -> discoveryClusterAddressProvider.getAddresses());

        // assert
        verifyNoInteractions(tarantoolClient);
    }

    @Test
    void shouldThrowWhenUnexpectedAnswer() throws Exception {
        // arrange
        when(tarantoolClient.eval(Mockito.anyString())).thenReturn(CompletableFuture.completedFuture(asList(CORRECT_RESULT, CORRECT_RESULT)));

        // act
        assertThrows(TarantoolClientException.class, () -> discoveryClusterAddressProvider.getAddresses());

        // assert
        verify(tarantoolClient).eval(Mockito.anyString());
        verify(tarantoolClient).close();
    }

    @Test
    void shouldThrowWhenNoServersFound() throws Exception {
        // arrange
        when(tarantoolClient.eval(Mockito.anyString())).thenReturn(CompletableFuture.completedFuture(singletonList(EMPTY_RESULT)));

        // act
        assertThrows(TarantoolClientException.class, () -> discoveryClusterAddressProvider.getAddresses());

        // assert
        verify(tarantoolClient).eval(Mockito.anyString());
        verify(tarantoolClient).close();
    }

    @Test
    void shouldThrowWhenInvalidAddressReturn() throws Exception {
        // arrange
        when(tarantoolClient.eval(Mockito.anyString())).thenReturn(CompletableFuture.completedFuture(singletonList(INCORRECT_RESULT)));

        // act
        assertThrows(TarantoolClientException.class, () -> discoveryClusterAddressProvider.getAddresses());

        // assert
        verify(tarantoolClient).eval(Mockito.anyString());
        verify(tarantoolClient).close();
    }

}