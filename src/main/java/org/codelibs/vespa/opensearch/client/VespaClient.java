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

    // Search operations
    public Map<String, Object> search(final String namespace, final String docType, final Map<String, Object> searchRequest) {
        try {
            // Extract query parameters
            final String yql = buildYqlFromOpenSearchQuery(searchRequest);
            final int size = searchRequest.containsKey("size") ? (Integer) searchRequest.get("size") : 10;
            final int from = searchRequest.containsKey("from") ? (Integer) searchRequest.get("from") : 0;

            final StringBuilder url = new StringBuilder(endpoint).append("search/?");
            url.append("yql=").append(java.net.URLEncoder.encode(yql, "UTF-8"));
            url.append("&hits=").append(size);
            url.append("&offset=").append(from);

            try (CurlResponse response = Curl.get(url.toString()).header("Content-Type", "application/json").execute()) {
                if (response.getHttpStatusCode() == 200) {
                    final Map<String, Object> vespaResult = response.getContent(PARSER);
                    return convertVespaSearchToOpenSearch(vespaResult);
                }
                throw new VespaClientException("Search failed with status: " + response.getHttpStatusCode());
            }
        } catch (final Exception e) {
            throw new VespaClientException("Failed to execute search", e);
        }
    }

    private String buildYqlFromOpenSearchQuery(final Map<String, Object> searchRequest) {
        // Simple implementation - build YQL from OpenSearch query
        if (searchRequest.containsKey("query")) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> query = (Map<String, Object>) searchRequest.get("query");

            if (query.containsKey("match_all")) {
                return "select * from sources * where true";
            } else if (query.containsKey("match")) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> match = (Map<String, Object>) query.get("match");
                final String field = match.keySet().iterator().next();
                final Object value = match.get(field);
                final String searchValue = value instanceof Map ? ((Map<?, ?>) value).get("query").toString() : value.toString();
                return "select * from sources * where " + field + " contains \"" + searchValue + "\"";
            } else if (query.containsKey("term")) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> term = (Map<String, Object>) query.get("term");
                final String field = term.keySet().iterator().next();
                final Object value = term.get(field);
                final String termValue = value instanceof Map ? ((Map<?, ?>) value).get("value").toString() : value.toString();
                return "select * from sources * where " + field + " contains \"" + termValue + "\"";
            }
        }
        // Default query
        return "select * from sources * where true";
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> convertVespaSearchToOpenSearch(final Map<String, Object> vespaResult) {
        final Map<String, Object> result = new HashMap<>();
        result.put("took", vespaResult.getOrDefault("timing", Map.of("searchtime", 0)));
        result.put("timed_out", false);

        final Map<String, Object> shards = new HashMap<>();
        shards.put("total", 1);
        shards.put("successful", 1);
        shards.put("skipped", 0);
        shards.put("failed", 0);
        result.put("_shards", shards);

        final Map<String, Object> hits = new HashMap<>();
        final Map<String, Object> root = (Map<String, Object>) vespaResult.get("root");
        final int totalCount = root != null ? (Integer) root.getOrDefault("coverage", Map.of("documents", 0)) : 0;

        final Map<String, Object> total = new HashMap<>();
        total.put("value", totalCount);
        total.put("relation", "eq");
        hits.put("total", total);
        hits.put("max_score", 1.0);

        final java.util.List<Map<String, Object>> hitList = new java.util.ArrayList<>();
        if (root != null && root.containsKey("children")) {
            final java.util.List<Map<String, Object>> children = (java.util.List<Map<String, Object>>) root.get("children");
            for (final Map<String, Object> child : children) {
                final Map<String, Object> hit = new HashMap<>();
                hit.put("_index", "default");
                hit.put("_id", child.get("id"));
                hit.put("_score", child.getOrDefault("relevance", 1.0));
                hit.put("_source", child.get("fields"));
                hitList.add(hit);
            }
        }
        hits.put("hits", hitList);
        result.put("hits", hits);

        return result;
    }

    public Map<String, Object> count(final String namespace, final String docType, final Map<String, Object> query) {
        try {
            final String yql = query != null ? buildYqlFromOpenSearchQuery(query) : "select * from sources * where true";
            final String url = endpoint + "search/?yql=" + java.net.URLEncoder.encode(yql, "UTF-8") + "&hits=0";

            try (CurlResponse response = Curl.get(url).header("Content-Type", "application/json").execute()) {
                if (response.getHttpStatusCode() == 200) {
                    final Map<String, Object> vespaResult = response.getContent(PARSER);
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> root = (Map<String, Object>) vespaResult.get("root");
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> coverage = root != null ? (Map<String, Object>) root.get("coverage") : new HashMap<>();
                    final int count = coverage != null ? (Integer) coverage.getOrDefault("documents", 0) : 0;

                    final Map<String, Object> result = new HashMap<>();
                    result.put("count", count);
                    result.put("_shards", Map.of("total", 1, "successful", 1, "skipped", 0, "failed", 0));
                    return result;
                }
                throw new VespaClientException("Count failed with status: " + response.getHttpStatusCode());
            }
        } catch (final Exception e) {
            throw new VespaClientException("Failed to execute count", e);
        }
    }

    public Map<String, Object> multiGet(final String namespace, final String docType, final List<String> ids) {
        final Map<String, Object> result = new HashMap<>();
        final java.util.List<Map<String, Object>> docs = new java.util.ArrayList<>();

        for (final String id : ids) {
            try {
                final Map<String, Object> doc = get(namespace, docType, id);
                final Map<String, Object> docResult = new HashMap<>();
                docResult.put("_index", namespace);
                docResult.put("_id", id);
                docResult.put("_version", 1);
                docResult.put("found", true);
                docResult.put("_source", doc.get("fields"));
                docs.add(docResult);
            } catch (final VespaClientException e) {
                final Map<String, Object> docResult = new HashMap<>();
                docResult.put("_index", namespace);
                docResult.put("_id", id);
                docResult.put("found", false);
                docs.add(docResult);
            }
        }

        result.put("docs", docs);
        return result;
    }

    public Map<String, Object> partialUpdate(final String namespace, final String docType, final String id,
            final Map<String, Object> partialDoc) {
        // For partial updates, we need to get the existing document, merge, and update
        try {
            final Map<String, Object> existing = get(namespace, docType, id);
            @SuppressWarnings("unchecked")
            final Map<String, Object> existingFields = (Map<String, Object>) existing.get("fields");

            // Merge with partial document
            if (partialDoc.containsKey("doc")) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> doc = (Map<String, Object>) partialDoc.get("doc");
                existingFields.putAll(doc);
            } else {
                existingFields.putAll(partialDoc);
            }

            return update(namespace, docType, id, existingFields);
        } catch (final Exception e) {
            throw new VespaClientException("[" + namespace + "][" + docType + "][" + id + "] Failed to partial update.", e);
        }
    }

    public Map<String, Object> refresh(final String indexName) {
        // Vespa doesn't have a direct refresh concept, but we can return success
        final Map<String, Object> result = new HashMap<>();
        result.put("_shards", Map.of("total", 1, "successful", 1, "failed", 0));
        return result;
    }

}
