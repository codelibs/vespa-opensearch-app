package org.codelibs.vespa.opensearch.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codelibs.core.exception.IORuntimeException;
import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlException;
import org.codelibs.curl.CurlResponse;
import org.codelibs.vespa.opensearch.exception.VespaClientException;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;

public class VespaClient {

    private static final Logger log = Logger.getLogger(VespaClient.class.getName());

    private final String endpoint;

    protected static final Function<CurlResponse, Map<String, Object>> PARSER = response -> {
        try (InputStream is = response.getContentAsStream()) {
            return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, is).map();
        } catch (final Exception e) {
            throw new CurlException("Failed to access the content.", e);
        }
    };

    public VespaClient(final String endpoint) {
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
        } catch (final IOException e) {
            log.log(Level.WARNING, e, () -> "Failed to access to " + endpoint);
        }
        return Collections.emptyMap();
    }

    public Map<String, Object> insert(final String namespace, final String docType, final String id, final Map<String, Object> data) {
        final Map<String, Object> fieldMap = new HashMap<>();
        flattenMap("", data, fieldMap);

        try (CurlResponse response = Curl.post(endpoint + "document/v1/" + namespace + "/" + docType + "/docid/" + id)
                .header("Content-Type", "application/json").onConnect((req, con) -> {
                    con.setDoOutput(true);
                    try (XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, con.getOutputStream())) {
                        final Map<String, Object> obj = new HashMap<>();
                        obj.put("fields", fieldMap);
                        builder.value(obj);
                    } catch (final IOException e) {
                        throw new IORuntimeException(e);
                    }
                }).execute()) {
            if (response.getHttpStatusCode() == 200) {
                return response.getContent(PARSER);
            }
            throw new VespaClientException("[" + namespace + "][" + docType + "][" + id + "] Failed to insert a doc. The response is "
                    + response.getHttpStatusCode());
        } catch (final Exception e) {
            throw new VespaClientException("[" + namespace + "][" + docType + "][" + id + "] Failed to insert a doc.", e);
        }
    }

    private void flattenMap(final String currentPath, final Map<String, Object> map, final Map<String, Object> flattenedMap) {
        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();

            final String newPath = currentPath.isEmpty() ? key : currentPath + "." + key;

            // TODO List
            if (value instanceof Map<?, ?>) {
                flattenMap(newPath, (Map<String, Object>) value, flattenedMap);
            } else {
                flattenedMap.put(newPath, value);
            }
        }
    }

    public Map<String, Object> get(final String namespace, final String docType, final String id) {
        try (CurlResponse response = Curl.get(endpoint + "document/v1/" + namespace + "/" + docType + "/docid/" + id)
                .header("Content-Type", "application/json").execute()) {
            if (response.getHttpStatusCode() == 200) {
                return response.getContent(PARSER);
            }
            throw new VespaClientException("[" + namespace + "][" + docType + "][" + id + "] The doc is not found. The response is "
                    + response.getHttpStatusCode());
        } catch (final Exception e) {
            throw new VespaClientException("[" + namespace + "][" + docType + "][" + id + "] Failed to get the doc.", e);
        }
    }

    public Map<String, Object> delete(final String namespace, final String docType, final String id) {
        try (CurlResponse response = Curl.delete(endpoint + "document/v1/" + namespace + "/" + docType + "/docid/" + id)
                .header("Content-Type", "application/json").execute()) {
            if (response.getHttpStatusCode() == 200) {
                return response.getContent(PARSER);
            }
            throw new VespaClientException("[" + namespace + "][" + docType + "][" + id + "] Failed to delete the doc. The response is "
                    + response.getHttpStatusCode());
        } catch (final Exception e) {
            throw new VespaClientException("[" + namespace + "][" + docType + "][" + id + "] Failed to delete the doc.", e);
        }
    }

}
