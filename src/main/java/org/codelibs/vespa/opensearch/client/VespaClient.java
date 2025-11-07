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

    /**
     * In-memory index metadata storage (since Vespa schemas are static).
     * WARNING: This data is stored only in memory and will be lost on application restart.
     * It is NOT persisted to Vespa or any external storage.
     * For production deployments, ensure that loss of index metadata on restart is acceptable,
     * or implement a persistence mechanism if required.
     */
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
        if (searchRequest.containsKey("query")) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> query = (Map<String, Object>) searchRequest.get("query");
            final String condition = buildConditionFromQuery(query);
            return "select * from sources * where " + condition;
        }
        return "select * from sources * where true";
    }

    private String buildConditionFromQuery(final Map<String, Object> query) {
        if (query == null || query.isEmpty()) {
            return "true";
        }

        if (query.containsKey("match_all")) {
            return "true";
        }
        if (query.containsKey("match")) {
            return buildMatchQuery(query.get("match"));
        }
        if (query.containsKey("match_phrase")) {
            return buildMatchPhraseQuery(query.get("match_phrase"));
        }
        if (query.containsKey("multi_match")) {
            return buildMultiMatchQuery(query.get("multi_match"));
        }
        if (query.containsKey("term")) {
            return buildTermQuery(query.get("term"));
        }
        if (query.containsKey("terms")) {
            return buildTermsQuery(query.get("terms"));
        }
        if (query.containsKey("range")) {
            return buildRangeQuery(query.get("range"));
        }
        if (query.containsKey("exists")) {
            return buildExistsQuery(query.get("exists"));
        }
        if (query.containsKey("prefix")) {
            return buildPrefixQuery(query.get("prefix"));
        }
        if (query.containsKey("wildcard")) {
            return buildWildcardQuery(query.get("wildcard"));
        }
        if (query.containsKey("bool")) {
            return buildBoolQuery(query.get("bool"));
        }
        if (query.containsKey("ids")) {
            return buildIdsQuery(query.get("ids"));
        }
        if (query.containsKey("query_string")) {
            return buildQueryStringQuery(query.get("query_string"));
        }
        return "true";
    }

    private String buildMatchQuery(final Object matchObj) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> match = (Map<String, Object>) matchObj;
        final String field = match.keySet().iterator().next();
        final Object value = match.get(field);
        final String searchValue = value instanceof Map ? ((Map<?, ?>) value).get("query").toString() : value.toString();
        return field + " contains \"" + escapeYqlString(searchValue) + "\"";
    }

    private String buildMatchPhraseQuery(final Object matchPhraseObj) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> matchPhrase = (Map<String, Object>) matchPhraseObj;
        final String field = matchPhrase.keySet().iterator().next();
        final Object value = matchPhrase.get(field);
        final String phraseValue = value instanceof Map ? ((Map<?, ?>) value).get("query").toString() : value.toString();
        return field + " contains phrase(\"" + escapeYqlString(phraseValue) + "\")";
    }

    private String buildMultiMatchQuery(final Object multiMatchObj) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> multiMatch = (Map<String, Object>) multiMatchObj;
        final String queryValue = (String) multiMatch.get("query");
        @SuppressWarnings("unchecked")
        final java.util.List<String> fields = (java.util.List<String>) multiMatch.get("fields");
        if (fields == null || fields.isEmpty()) {
            return "true";
        }
        final java.util.List<String> conditions = new java.util.ArrayList<>();
        for (final String field : fields) {
            conditions.add(field + " contains \"" + escapeYqlString(queryValue) + "\"");
        }
        return "(" + String.join(" OR ", conditions) + ")";
    }

    private String buildTermQuery(final Object termObj) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> term = (Map<String, Object>) termObj;
        final String field = term.keySet().iterator().next();
        final Object value = term.get(field);
        final String termValue = value instanceof Map ? ((Map<?, ?>) value).get("value").toString() : value.toString();
        return field + " matches \"" + escapeYqlString(termValue) + "\"";
    }

    private String buildTermsQuery(final Object termsObj) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> terms = (Map<String, Object>) termsObj;
        final String field = terms.keySet().iterator().next();
        @SuppressWarnings("unchecked")
        final java.util.List<Object> values = (java.util.List<Object>) terms.get(field);
        if (values == null || values.isEmpty()) {
            return "false";
        }
        final java.util.List<String> conditions = new java.util.ArrayList<>();
        for (final Object value : values) {
            conditions.add(field + " matches \"" + escapeYqlString(value.toString()) + "\"");
        }
        return "(" + String.join(" OR ", conditions) + ")";
    }

    private String buildRangeQuery(final Object rangeObj) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> range = (Map<String, Object>) rangeObj;
        final String field = range.keySet().iterator().next();
        @SuppressWarnings("unchecked")
        final Map<String, Object> conditions = (Map<String, Object>) range.get(field);
        final java.util.List<String> rangeParts = new java.util.ArrayList<>();
        if (conditions.containsKey("gte")) {
            rangeParts.add(field + " >= " + conditions.get("gte"));
        } else if (conditions.containsKey("gt")) {
            rangeParts.add(field + " > " + conditions.get("gt"));
        }
        if (conditions.containsKey("lte")) {
            rangeParts.add(field + " <= " + conditions.get("lte"));
        } else if (conditions.containsKey("lt")) {
            rangeParts.add(field + " < " + conditions.get("lt"));
        }
        return rangeParts.isEmpty() ? "true" : "(" + String.join(" AND ", rangeParts) + ")";
    }

    private String buildExistsQuery(final Object existsObj) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> exists = (Map<String, Object>) existsObj;
        final String field = (String) exists.get("field");
        return "(" + field + " matches \"*\" OR " + field + " > 0 OR " + field + " < 0)";
    }

    private String buildPrefixQuery(final Object prefixObj) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> prefix = (Map<String, Object>) prefixObj;
        final String field = prefix.keySet().iterator().next();
        final Object value = prefix.get(field);
        final String prefixValue = value instanceof Map ? ((Map<?, ?>) value).get("value").toString() : value.toString();
        return field + " matches \"" + escapeYqlString(prefixValue) + "*\"";
    }

    private String buildWildcardQuery(final Object wildcardObj) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> wildcard = (Map<String, Object>) wildcardObj;
        final String field = wildcard.keySet().iterator().next();
        final Object value = wildcard.get(field);
        final String wildcardValue = value instanceof Map ? ((Map<?, ?>) value).get("value").toString() : value.toString();
        return field + " matches \"" + escapeYqlString(wildcardValue) + "\"";
    }

    private String buildBoolQuery(final Object boolObj) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> bool = (Map<String, Object>) boolObj;
        final java.util.List<String> allConditions = new java.util.ArrayList<>();

        if (bool.containsKey("must")) {
            @SuppressWarnings("unchecked")
            final Object mustObj = bool.get("must");
            if (mustObj instanceof java.util.List) {
                @SuppressWarnings("unchecked")
                final java.util.List<Map<String, Object>> mustClauses = (java.util.List<Map<String, Object>>) mustObj;
                final java.util.List<String> mustConditions = new java.util.ArrayList<>();
                for (final Map<String, Object> clause : mustClauses) {
                    mustConditions.add(buildConditionFromQuery(clause));
                }
                if (!mustConditions.isEmpty()) {
                    allConditions.add("(" + String.join(" AND ", mustConditions) + ")");
                }
            } else {
                @SuppressWarnings("unchecked")
                final Map<String, Object> mustClause = (Map<String, Object>) mustObj;
                allConditions.add("(" + buildConditionFromQuery(mustClause) + ")");
            }
        }

        if (bool.containsKey("filter")) {
            final Object filter = bool.get("filter");
            if (filter instanceof java.util.List) {
                @SuppressWarnings("unchecked")
                final java.util.List<Map<String, Object>> filterClauses = (java.util.List<Map<String, Object>>) filter;
                final java.util.List<String> filterConditions = new java.util.ArrayList<>();
                for (final Map<String, Object> clause : filterClauses) {
                    filterConditions.add(buildConditionFromQuery(clause));
                }
                if (!filterConditions.isEmpty()) {
                    allConditions.add("(" + String.join(" AND ", filterConditions) + ")");
                }
            } else {
                @SuppressWarnings("unchecked")
                final Map<String, Object> filterClause = (Map<String, Object>) filter;
                allConditions.add("(" + buildConditionFromQuery(filterClause) + ")");
            }
        }

        if (bool.containsKey("should")) {
            @SuppressWarnings("unchecked")
            final java.util.List<Map<String, Object>> shouldClauses = (java.util.List<Map<String, Object>>) bool.get("should");
            final java.util.List<String> shouldConditions = new java.util.ArrayList<>();
            for (final Map<String, Object> clause : shouldClauses) {
                shouldConditions.add(buildConditionFromQuery(clause));
            }
            if (!shouldConditions.isEmpty()) {
                if (allConditions.isEmpty()) {
                    allConditions.add("(" + String.join(" OR ", shouldConditions) + ")");
                } else {
                    allConditions.add("(" + String.join(" OR ", shouldConditions) + ")");
                }
            }
        }

        if (bool.containsKey("must_not")) {
            @SuppressWarnings("unchecked")
            final Object mustNotObj = bool.get("must_not");
            if (mustNotObj instanceof java.util.List) {
                @SuppressWarnings("unchecked")
                final java.util.List<Map<String, Object>> mustNotClauses = (java.util.List<Map<String, Object>>) mustNotObj;
                for (final Map<String, Object> clause : mustNotClauses) {
                    allConditions.add("!(" + buildConditionFromQuery(clause) + ")");
                }
            } else {
                @SuppressWarnings("unchecked")
                final Map<String, Object> mustNotClause = (Map<String, Object>) mustNotObj;
                allConditions.add("!(" + buildConditionFromQuery(mustNotClause) + ")");
            }
        }

        return allConditions.isEmpty() ? "true" : "(" + String.join(" AND ", allConditions) + ")";
    }

    private String buildIdsQuery(final Object idsObj) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> ids = (Map<String, Object>) idsObj;
        @SuppressWarnings("unchecked")
        final java.util.List<String> values = (java.util.List<String>) ids.get("values");
        if (values == null || values.isEmpty()) {
            return "false";
        }
        final java.util.List<String> conditions = new java.util.ArrayList<>();
        for (final String id : values) {
            conditions.add("documentid contains \"" + escapeYqlString(id) + "\"");
        }
        return "(" + String.join(" OR ", conditions) + ")";
    }

    private String buildQueryStringQuery(final Object queryStringObj) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> queryString = (Map<String, Object>) queryStringObj;
        final String query = (String) queryString.get("query");
        @SuppressWarnings("unchecked")
        final java.util.List<String> fields = queryString.containsKey("fields") ? (java.util.List<String>) queryString.get("fields")
                : null;
        if (fields == null || fields.isEmpty()) {
            return "default contains \"" + escapeYqlString(query) + "\"";
        }
        final java.util.List<String> conditions = new java.util.ArrayList<>();
        for (final String field : fields) {
            conditions.add(field + " contains \"" + escapeYqlString(query) + "\"");
        }
        return "(" + String.join(" OR ", conditions) + ")";
    }

    private String escapeYqlString(final String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
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
        int totalCount = 0;
        if (root != null) {
            final Map<String, Object> coverage = (Map<String, Object>) root.getOrDefault("coverage", Map.of("documents", 0));
            final Object documentsObj = coverage.getOrDefault("documents", 0);
            if (documentsObj instanceof Number) {
                totalCount = ((Number) documentsObj).intValue();
            } else {
                try {
                    totalCount = Integer.parseInt(documentsObj.toString());
                } catch (final NumberFormatException e) {
                    totalCount = 0;
                }
            }
        }

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
