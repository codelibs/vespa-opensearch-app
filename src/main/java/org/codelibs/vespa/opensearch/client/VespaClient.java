package org.codelibs.vespa.opensearch.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlException;
import org.codelibs.curl.CurlResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;

public class VespaClient {

    private static final Logger log = Logger.getLogger(VespaClient.class.getName());;

    private final String endpoint;

    protected static final Function<CurlResponse, Map<String, Object>> PARSER = response -> {
        try (InputStream is = response.getContentAsStream()) {
            return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, is).map();
        } catch (final Exception e) {
            throw new CurlException("Failed to access the content.", e);
        }
    };

    public VespaClient(String endpoint) {
        if (endpoint.endsWith("/")) {
            this.endpoint = endpoint;
        } else {
            this.endpoint = endpoint + "/";
        }
    }

    public Map<String, Object> getInfo() {
        try (CurlResponse response = Curl.get(endpoint).header("Content-Type", "application/json").execute()) {
            if (response.getHttpStatusCode() == 200) {
                return response.getContent(PARSER);
            }
        } catch (IOException e) {
            log.log(Level.WARNING, e, () -> "Failed to access to " + endpoint);
        }
        return Collections.emptyMap();
    }

    public void insert(String namespace, String docType, String id, Map<String, Object> data) {
        Map<String, Object> flattenedMap = new HashMap<>();
        flattenMap("", data, flattenedMap);

        CurlResponse response = Curl.post(endpoint + "document/v1/" + namespace + "/" + docType + "/docid/" + id)
                .header("Content-Type", "application/json").execute();
    }

    private void flattenMap(String currentPath, Map<String, Object> map, Map<String, Object> flattenedMap) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            String newPath = currentPath.isEmpty() ? key : currentPath + "." + key;

            // TODO List
            if (value instanceof Map<?, ?>) {
                flattenMap(newPath, (Map<String, Object>) value, flattenedMap);
            } else {
                flattenedMap.put(newPath, value);
            }
        }
    }

}
