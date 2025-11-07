package org.codelibs.vespa.opensearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.vespa.opensearch.action.MgetAction;
import org.codelibs.vespa.opensearch.action.SearchAction;
import org.codelibs.vespa.opensearch.client.VespaClient;
import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.json.JsonXContent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;

public class EdgeCaseTests {

    private RestApiProxyHandler handler;
    private VespaClient vespaClient;

    @BeforeEach
    void setUp() {
        handler = mock(RestApiProxyHandler.class);
        vespaClient = mock(VespaClient.class);
        when(handler.getVespaClient()).thenReturn(vespaClient);
        when(handler.getDocumentType()).thenReturn("doc");
    }

    @Test
    void testSearchWithVeryLongQuery() throws IOException {
        // Create a very long query string
        StringBuilder longQuery = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longQuery.append("word").append(i).append(" ");
        }

        Map<String, Object> requestBody = Map.of(
                "query", Map.of("match", Map.of("content", longQuery.toString())));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithEmptyString() throws IOException {
        // Test search with empty string
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("match", Map.of("title", "")));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithUnicodeCharacters() throws IOException {
        // Test search with Unicode characters (Japanese, Chinese, Arabic, etc.)
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("match", Map.of("title", "日本語 中文 العربية हिन्दी 한국어")));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 5), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithSpecialRegexCharacters() throws IOException {
        // Test search with special regex characters that need escaping
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("match", Map.of("title", "test.*[]()+?{}")));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithSQLInjectionAttempt() throws IOException {
        // Test search with SQL injection attempt (should be safe)
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("match", Map.of("title", "'; DROP TABLE users; --")));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithYQLInjectionAttempt() throws IOException {
        // Test search with YQL injection attempt (should be escaped)
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("term", Map.of("status", "active\" OR true=\"true")));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithVeryLargePagination() throws IOException {
        // Test search with very large pagination values
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("match_all", new HashMap<>()),
                "from", 1000000,
                "size", 10000);
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithZeroSize() throws IOException {
        // Test search with size=0 (should return only count)
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("match_all", new HashMap<>()),
                "size", 0);
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 100), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithNegativeValues() throws IOException {
        // Test search with negative values (should handle gracefully)
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("match_all", new HashMap<>()),
                "from", -10,
                "size", -5);
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        // Should still process (Vespa will handle invalid values)
        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithDeeplyNestedBoolQuery() throws IOException {
        // Test search with very deeply nested bool queries
        Map<String, Object> level5 = Map.of("term", Map.of("field5", "value5"));
        Map<String, Object> level4 = Map.of("bool", Map.of("must", List.of(level5)));
        Map<String, Object> level3 = Map.of("bool", Map.of("must", List.of(level4)));
        Map<String, Object> level2 = Map.of("bool", Map.of("must", List.of(level3)));
        Map<String, Object> level1 = Map.of("bool", Map.of("must", List.of(level2)));

        Map<String, Object> requestBody = Map.of("query", level1);
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testMgetWithVeryLongIdList() throws IOException {
        // Test mget with very long list of IDs
        List<String> manyIds = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            manyIds.add("doc" + i);
        }

        Map<String, Object> requestBody = Map.of("ids", manyIds);
        HttpRequest request = createMockRequest("POST", "/_mget", requestBody);

        // Mock response
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            docs.add(Map.of("_id", "doc" + i, "_found", false));
        }
        Map<String, Object> vespaResponse = Map.of("docs", docs);
        when(vespaClient.multiGet(anyString(), anyString(), anyList())).thenReturn(vespaResponse);

        MgetAction action = new MgetAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithEmptyBoolQuery() throws IOException {
        // Test search with empty bool query (no must/should/must_not/filter)
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("bool", new HashMap<>()));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithNullFieldValue() throws IOException {
        // Test search where field value is null
        Map<String, Object> matchQuery = new HashMap<>();
        matchQuery.put("title", null);

        Map<String, Object> query = new HashMap<>();
        query.put("match", matchQuery);

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("query", query);

        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        // Should handle gracefully
        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithQuotesInFieldValue() throws IOException {
        // Test search with quotes in field value (important for escaping)
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("term", Map.of("title", "test \"quoted\" value")));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithBackslashesInFieldValue() throws IOException {
        // Test search with backslashes (important for escaping)
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("term", Map.of("path", "C:\\Users\\test\\file.txt")));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithEmptyArray() throws IOException {
        // Test search with empty terms array
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("terms", Map.of("status", List.of())));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithVeryLargeNumber() throws IOException {
        // Test search with very large numbers
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("range", Map.of("price", Map.of("gte", Long.MAX_VALUE))));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithMixedDataTypes() throws IOException {
        // Test search with mixed data types in bool query
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("bool", Map.of(
                        "must", List.of(
                                Map.of("match", Map.of("title", "test")),
                                Map.of("range", Map.of("price", Map.of("gte", 10.5))),
                                Map.of("term", Map.of("category", "electronics")),
                                Map.of("exists", Map.of("field", "description"))))));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("hits", Map.of("total", Map.of("value", 0), "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        SearchAction action = new SearchAction(handler);
        HttpResponse response = action.execute(request);

        assertEquals(200, response.getStatus());
    }

    private HttpRequest createMockRequest(String method, String path, Map<String, Object> body) throws IOException {
        HttpRequest request = mock(HttpRequest.class);
        when(request.getUri()).thenReturn(java.net.URI.create("http://localhost" + path));

        if (body != null) {
            // Convert map to JSON
            byte[] jsonBytes = new ObjectMapper().writeValueAsBytes(body);
            when(request.getData()).thenReturn(new ByteArrayInputStream(jsonBytes));
        } else {
            when(request.getData()).thenReturn(new ByteArrayInputStream(new byte[0]));
        }

        return request;
    }
}
