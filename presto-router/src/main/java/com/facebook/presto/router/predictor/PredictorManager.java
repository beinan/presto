/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.router.predictor;

import io.airlift.log.Logger;

import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.Objects.requireNonNull;

/**
 * The manager of fetching predicted resource usage of a SQL statement from the
 * Presto query predictor.
 * Note that it does not validate the SQL statements passed in.
 */
public class PredictorManager
{
    private static final Logger log = Logger.get(PredictorManager.class);

    private final RemoteQueryFactory remoteQueryFactory;
    private URI uri;

    @Inject
    public PredictorManager(RemoteQueryFactory remoteQueryFactory)
    {
        this.remoteQueryFactory = requireNonNull(remoteQueryFactory, "");
        try {
            this.uri = new URI("http://127.0.0.1:8080/v1");
        }
        catch (URISyntaxException e) {
            log.error("Error in creating PredictorManager");
        }
    }

    public ResourceGroup fetchPrediction(String statement)
    {
        try {
            return new ResourceGroup(fetchCpuPrediction(statement), fetchMemoryPrediction(statement));
        }
        catch (Exception e) {
            log.error("Error in fetching prediction", e);
        }
        return null;
    }

    public ResourceGroup fetchPredictionParallel(String statement)
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        Future<CpuInfo> cpuInfoFuture = executor.submit(() -> fetchCpuPrediction(statement));
        Future<MemoryInfo> memoryInfoFuture = executor.submit(() -> fetchMemoryPrediction(statement));
        try {
            return new ResourceGroup(cpuInfoFuture.get(), memoryInfoFuture.get());
        }
        catch (Exception e) {
            log.error("Error in fetching prediction in parallel", e);
        }
        return null;
    }

    public CpuInfo fetchCpuPrediction(String statement)
    {
        RemoteQueryCpu remoteQueryCpu;
        try {
            remoteQueryCpu = this.remoteQueryFactory.createRemoteQueryCPU(this.uri);
            remoteQueryCpu.execute(statement);
            return remoteQueryCpu.getCpuInfo();
        }
        catch (Exception e) {
            log.error("Error in fetching CPU prediction", e);
        }
        return null;
    }

    public MemoryInfo fetchMemoryPrediction(String statement)
    {
        RemoteQueryMemory remoteQueryMemory;
        try {
            remoteQueryMemory = this.remoteQueryFactory.createRemoteQueryMemory(this.uri);
            remoteQueryMemory.execute(statement);
            return remoteQueryMemory.getMemoryInfo();
        }
        catch (Exception e) {
            log.error("Error in fetching memory prediction", e);
        }
        return null;
    }
}
