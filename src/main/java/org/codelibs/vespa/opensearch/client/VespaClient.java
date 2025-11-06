package org.codelibs.vespa.opensearch.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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

    // In-memory index metadata storage (since Vespa schemas are static)
    private final Map<String, Map<String, Object>> indexMetadata = new ConcurrentHashMap<>();

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

    public Map<String, Object> update(final String namespace, final String docType, final String id, final Map<String, Object> data) {
        final Map<String, Object> fieldMap = new HashMap<>();
        flattenMap("", data, fieldMap);

        try (CurlResponse response = Curl.put(endpoint + "document/v1/" + namespace + "/" + docType + "/docid/" + id)
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
            throw new VespaClientException("[" + namespace + "][" + docType + "][" + id + "] Failed to update a doc. The response is "
                    + response.getHttpStatusCode());
        } catch (final Exception e) {
            throw new VespaClientException("[" + namespace + "][" + docType + "][" + id + "] Failed to update a doc.", e);
        }
    }

    // Index management methods
    public Map<String, Object> createIndex(final String indexName, final Map<String, Object> settings) {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("uuid", UUID.randomUUID().toString());
        metadata.put("settings", settings != null ? settings : new HashMap<>());
        metadata.put("mappings", new HashMap<>());
        indexMetadata.put(indexName, metadata);

        final Map<String, Object> result = new HashMap<>();
        result.put("acknowledged", true);
        result.put("shards_acknowledged", true);
        result.put("index", indexName);
        return result;
    }

    public boolean indexExists(final String indexName) {
        return indexMetadata.containsKey(indexName);
    }

    public Map<String, Object> getIndex(final String indexName) {
        if (!indexMetadata.containsKey(indexName)) {
            throw new VespaClientException("Index [" + indexName + "] does not exist");
        }
        final Map<String, Object> result = new HashMap<>();
        result.put(indexName, indexMetadata.get(indexName));
        return result;
    }

    public Map<String, Map<String, Object>> getAllIndices() {
        return new HashMap<>(indexMetadata);
    }

    public Map<String, Object> deleteIndex(final String indexName) {
        if (!indexMetadata.containsKey(indexName)) {
            throw new VespaClientException("Index [" + indexName + "] does not exist");
        }
        indexMetadata.remove(indexName);

        final Map<String, Object> result = new HashMap<>();
        result.put("acknowledged", true);
        return result;
    }

    public Map<String, Object> updateMapping(final String indexName, final Map<String, Object> mappings) {
        if (!indexMetadata.containsKey(indexName)) {
            throw new VespaClientException("Index [" + indexName + "] does not exist");
        }
        final Map<String, Object> metadata = indexMetadata.get(indexName);
        metadata.put("mappings", mappings);

        final Map<String, Object> result = new HashMap<>();
        result.put("acknowledged", true);
        return result;
    }

    public Map<String, Object> getMapping(final String indexName) {
        if (!indexMetadata.containsKey(indexName)) {
            throw new VespaClientException("Index [" + indexName + "] does not exist");
        }
        final Map<String, Object> result = new HashMap<>();
        final Map<String, Object> indexMap = new HashMap<>();
        indexMap.put("mappings", indexMetadata.get(indexName).get("mappings"));
        result.put(indexName, indexMap);
        return result;
    }

    public Map<String, Object> updateSettings(final String indexName, final Map<String, Object> settings) {
        if (!indexMetadata.containsKey(indexName)) {
            throw new VespaClientException("Index [" + indexName + "] does not exist");
        }
        final Map<String, Object> metadata = indexMetadata.get(indexName);
        @SuppressWarnings("unchecked")
        final Map<String, Object> currentSettings = (Map<String, Object>) metadata.get("settings");
        currentSettings.putAll(settings);

        final Map<String, Object> result = new HashMap<>();
        result.put("acknowledged", true);
        return result;
    }

    public Map<String, Object> getSettings(final String indexName) {
        if (!indexMetadata.containsKey(indexName)) {
            throw new VespaClientException("Index [" + indexName + "] does not exist");
        }
        final Map<String, Object> result = new HashMap<>();
        final Map<String, Object> indexMap = new HashMap<>();
        indexMap.put("settings", indexMetadata.get(indexName).get("settings"));
        result.put(indexName, indexMap);
        return result;
    }

    // Cluster information
    public Map<String, Object> getClusterHealth() {
        final Map<String, Object> result = new HashMap<>();
        result.put("cluster_name", "vespa-cluster");
        result.put("status", "green");
        result.put("timed_out", false);
        result.put("number_of_nodes", 1);
        result.put("number_of_data_nodes", 1);
        result.put("active_primary_shards", indexMetadata.size());
        result.put("active_shards", indexMetadata.size());
        result.put("relocating_shards", 0);
        result.put("initializing_shards", 0);
        result.put("unassigned_shards", 0);
        result.put("delayed_unassigned_shards", 0);
        result.put("number_of_pending_tasks", 0);
        result.put("number_of_in_flight_fetch", 0);
        result.put("task_max_waiting_in_queue_millis", 0);
        result.put("active_shards_percent_as_number", 100.0);
        return result;
    }

    public Map<String, Object> getClusterState() {
        final Map<String, Object> result = new HashMap<>();
        result.put("cluster_name", "vespa-cluster");
        result.put("cluster_uuid", UUID.randomUUID().toString());
        result.put("version", 1);
        result.put("state_uuid", UUID.randomUUID().toString());
        result.put("master_node", "node1");

        final Map<String, Object> metadata = new HashMap<>();
        result.put("metadata", metadata);
        metadata.put("cluster_uuid", UUID.randomUUID().toString());
        metadata.put("templates", new HashMap<>());
        metadata.put("indices", indexMetadata);

        return result;
    }

}
