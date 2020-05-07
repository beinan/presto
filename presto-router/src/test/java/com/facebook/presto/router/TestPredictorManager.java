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
package com.facebook.presto.router;

import com.facebook.presto.router.predictor.CpuInfo;
import com.facebook.presto.router.predictor.MemoryInfo;
import com.facebook.presto.router.predictor.PredictorManager;
import com.facebook.presto.router.predictor.ResourceGroup;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.log.Logging;
import io.airlift.node.testing.TestingNodeModule;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPredictorManager
{
    private static final int NUM_CLUSTERS = 2;

    private List<TestingPrestoServer> prestoServers;
    private LifeCycleManager lifeCycleManager;
    private HttpServerInfo httpServerInfo;
    private PredictorManager predictorManager;
    private File configFile;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();

        // set up server
        ImmutableList.Builder builder = ImmutableList.builder();
        for (int i = 0; i < NUM_CLUSTERS; ++i) {
            builder.add(createPrestoServer());
        }
        prestoServers = builder.build();
        configFile = getConfigFile(prestoServers);

        System.out.println(configFile.getAbsolutePath());

        Bootstrap app = new Bootstrap(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                new RouterModule());

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperty("router.config-file", configFile.getAbsolutePath())
                .quiet()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        httpServerInfo = injector.getInstance(HttpServerInfo.class);
        predictorManager = injector.getInstance(PredictorManager.class);
    }

    @Test
    public void testPredictor()
    {
        String sql = "select * from presto.logs";

        ResourceGroup resourceGroup = predictorManager.fetchPrediction(sql);
        assertNotNull(resourceGroup, "The resource group should not be null");
        assertNotNull(resourceGroup.getCpuInfo());
        assertNotNull(resourceGroup.getMemoryInfo());

        resourceGroup = predictorManager.fetchPredictionParallel(sql);
        assertNotNull(resourceGroup, "The resource group should not be null");
        assertNotNull(resourceGroup.getCpuInfo());
        assertNotNull(resourceGroup.getMemoryInfo());

        CpuInfo cpuInfo = predictorManager.fetchCpuPrediction(sql);
        MemoryInfo memoryInfo = predictorManager.fetchMemoryPrediction(sql);
        assertNotNull(cpuInfo);
        assertNotNull(memoryInfo);

        int low = 0;
        int high = 4;
        assertTrue(low <= cpuInfo.getCpuTimeLabel(), "CPU time label should be larger or equal to " + low);
        assertTrue(cpuInfo.getCpuTimeLabel() <= high, "CPU time label should be smaller or equal to " + high);
        assertTrue(low <= memoryInfo.getMemoryBytesLabel(), "Memory bytes label should be larger or equal to " + low);
        assertTrue(memoryInfo.getMemoryBytesLabel() <= high, "Memory bytes label should be smaller or equal to " + high);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        for (TestingPrestoServer prestoServer : prestoServers) {
            prestoServer.close();
        }
        lifeCycleManager.stop();
    }

    private static TestingPrestoServer createPrestoServer()
            throws Exception
    {
        TestingPrestoServer server = new TestingPrestoServer();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        server.refreshNodes();

        return server;
    }

    private File getConfigFile(List<TestingPrestoServer> servers)
            throws IOException
    {
        // setup router config file
        File tempFile = File.createTempFile("router", "json");
        FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
        String configTemplate = new String(Files.readAllBytes(Paths.get(getResourceFilePath("simple_router_template.json"))));
        fileOutputStream.write(configTemplate.replaceAll("\\$\\{SERVERS}", getClusterList(servers)).getBytes(UTF_8));
        fileOutputStream.close();
        return tempFile;
    }

    private static String getClusterList(List<TestingPrestoServer> servers)
    {
        JsonCodec<List<URI>> codec = JsonCodec.listJsonCodec(URI.class);
        return codec.toJson(
                servers.stream()
                        .map(TestingPrestoServer::getBaseUrl)
                        .collect(toImmutableList()));
    }

    private static Connection createConnection(URI uri)
            throws SQLException
    {
        String url = format("jdbc:presto://%s:%s", uri.getHost(), uri.getPort());
        return DriverManager.getConnection(url, "test", null);
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
