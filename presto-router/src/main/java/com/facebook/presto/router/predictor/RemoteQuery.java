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
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public abstract class RemoteQuery
{
    private static final Logger log = Logger.get(RemoteQuery.class);
    private static final JsonCodec<JsonNode> JSON_CODEC = jsonCodec(JsonNode.class);

    private final HttpClient httpClient;
    private final URI remoteUri;

    public RemoteQuery(HttpClient httpClient, URI remoteUri)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.remoteUri = requireNonNull(remoteUri, "remoteUri is null");
    }

    public synchronized void execute(String statement)
    {
        JsonCodec<Map<String, String>> jsonCodec = JsonCodec.mapJsonCodec(String.class, String.class);
        Map<String, String> queryMap = new HashMap<>();
        queryMap.put("query", statement);

        String body = jsonCodec.toJson(queryMap);
        log.info("Sending request to %s", remoteUri);
        Request request =
                preparePost()
                        .setUri(remoteUri)
                        .addHeader(CONTENT_TYPE, "application/json")
                        .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                        .build();

        FullJsonResponseHandler.JsonResponse<JsonNode> result = httpClient.execute(request, createFullJsonResponseHandler(JSON_CODEC));
        log.info("Received response from %s", remoteUri);
        if (result != null) {
            if (result.getStatusCode() != OK.code()) {
                log.error(
                        "Error fetching info from %s returned status %d: %s",
                        remoteUri, result.getStatusCode(), result.getStatusMessage());
            }
            if (result.hasValue()) {
                handleResponse(result.getValue());
            }
        }
        else {
            log.error("Error fetching request");
        }
    }

    public void handleResponse(JsonNode response) {}
}
