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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Map;

import static io.airlift.http.client.HttpStatus.OK;

/**
 * The wrapper class for predicted peak memory bytes queries.
 * In a successful query's response body, the class explicitly checks
 * `MEMORY_BYTES_LABEL` and `MEMORY_BYTES_STR`.
 */
@ThreadSafe
public class RemoteQueryMemory
        extends RemoteQuery
{
    private static final Logger log = Logger.get(RemoteQueryMemory.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String MEMORY_BYTES_LABEL = "memory_pred_label";
    private static final String MEMORY_BYTES_STR = "memory_pred_str";

    private MemoryInfo memoryInfo;

    public RemoteQueryMemory(HttpClient httpClient, URI remoteUri)
    {
        super(httpClient, remoteUri);
    }

    @Override
    public void handleResponse(JsonNode response)
    {
        try {
            Map<String, Object> fields = mapper.convertValue(response, Map.class);
            if (fields.containsKey("status") && (int) fields.get("status") != OK.code()) {
                memoryInfo = null;
                return;
            }
            memoryInfo = new MemoryInfo((int) fields.get(MEMORY_BYTES_LABEL), (String) fields.get(MEMORY_BYTES_STR));
        }
        catch (Exception e) {
            log.error("Error handling response: %s", response.toString(), e);
        }
    }

    public MemoryInfo getMemoryInfo()
    {
        return memoryInfo;
    }
}
