package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.vespa.opensearch.client.VespaClient;
import org.codelibs.vespa.opensearch.exception.VespaClientException;
import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.json.JsonXContent;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;

public class SearchActionExecutionTests {

    private RestApiProxyHandler handler;
    private VespaClient vespaClient;
    private SearchAction action;

    @BeforeEach
    void setUp() {
        handler = mock(RestApiProxyHandler.class);
        vespaClient = mock(VespaClient.class);
        when(handler.getVespaClient()).thenReturn(vespaClient);
        when(handler.getDocumentType()).thenReturn("doc");
        action = new SearchAction(handler);
    }

    @Test
    void testSearchWithMatchAllQuery() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("query", Map.of("match_all", new HashMap<>()));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "hits", Map.of(
                        "total", Map.of("value", 10),
                        "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertNotNull(responseBody);
    }

    @Test
    void testSearchWithMatchQuery() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("match", Map.of("title", "test")));
        HttpRequest request = createMockRequest("POST", "/myindex/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "hits", Map.of(
                        "total", Map.of("value", 5),
                        "hits", List.of(
                                Map.of("_id", "1", "_source", Map.of("title", "test document")))));
        when(vespaClient.search(eq("myindex"), eq("doc"), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertNotNull(responseBody);
    }

    @Test
    void testSearchWithBoolQuery() throws IOException {
        // Prepare complex bool query
        Map<String, Object> requestBody = Map.of(
                "query", Map.of(
                        "bool", Map.of(
                                "must", List.of(
                                        Map.of("match", Map.of("title", "test")),
                                        Map.of("range", Map.of("price", Map.of("gte", 10, "lte", 100)))),
                                "should", List.of(
                                        Map.of("term", Map.of("category", "electronics"))),
                                "must_not", List.of(
                                        Map.of("term", Map.of("status", "deleted"))))));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "hits", Map.of(
                        "total", Map.of("value", 3),
                        "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithPagination() throws IOException {
        // Prepare request with pagination
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("match_all", new HashMap<>()),
                "from", 10,
                "size", 20);
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "hits", Map.of(
                        "total", Map.of("value", 100),
                        "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithEmptyBody() throws IOException {
        // Prepare request with no body (should default to match_all)
        HttpRequest request = createMockRequest("GET", "/_search", null);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "hits", Map.of(
                        "total", Map.of("value", 10),
                        "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithInvalidJson() throws IOException {
        // Prepare request with invalid JSON
        HttpRequest request = mock(HttpRequest.class);
        when(request.getUri()).thenReturn(java.net.URI.create("http://localhost/_search"));
        when(request.getData()).thenReturn(new ByteArrayInputStream("{invalid json".getBytes()));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 400
        assertEquals(400, response.getStatus());
    }

    @Test
    void testSearchWithVespaClientException() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("query", Map.of("match_all", new HashMap<>()));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock exception
        when(vespaClient.search(anyString(), anyString(), any()))
                .thenThrow(new VespaClientException("Vespa connection failed"));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 500
        assertEquals(500, response.getStatus());
    }

    @Test
    void testSearchWithRangeQuery() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of(
                "query", Map.of(
                        "range", Map.of(
                                "timestamp", Map.of(
                                        "gte", "2024-01-01",
                                        "lt", "2024-12-31"))));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "hits", Map.of(
                        "total", Map.of("value", 15),
                        "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithTermsQuery() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of(
                "query", Map.of(
                        "terms", Map.of(
                                "status", List.of("active", "pending", "approved"))));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "hits", Map.of(
                        "total", Map.of("value", 25),
                        "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithMultiMatchQuery() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of(
                "query", Map.of(
                        "multi_match", Map.of(
                                "query", "test search",
                                "fields", List.of("title", "content", "description"))));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "hits", Map.of(
                        "total", Map.of("value", 8),
                        "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testSearchWithNestedBoolQuery() throws IOException {
        // Prepare deeply nested bool query
        Map<String, Object> requestBody = Map.of(
                "query", Map.of(
                        "bool", Map.of(
                                "must", List.of(
                                        Map.of("match", Map.of("title", "test")),
                                        Map.of("bool", Map.of(
                                                "should", List.of(
                                                        Map.of("term", Map.of("category", "A")),
                                                        Map.of("term", Map.of("category", "B")))))))));
        HttpRequest request = createMockRequest("POST", "/_search", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "hits", Map.of(
                        "total", Map.of("value", 2),
                        "hits", List.of()));
        when(vespaClient.search(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    private HttpRequest createMockRequest(String method, String path, Map<String, Object> body) throws IOException {
        HttpRequest request = mock(HttpRequest.class);
        when(request.getUri()).thenReturn(java.net.URI.create("http://localhost" + path));

        if (body != null) {
            // Convert map to JSON
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            JsonXContent.jsonXContent.createGenerator(baos).writeValue(body).close();
            byte[] jsonBytes = baos.toByteArray();
            when(request.getData()).thenReturn(new ByteArrayInputStream(jsonBytes));
        } else {
            when(request.getData()).thenReturn(new ByteArrayInputStream(new byte[0]));
        }

        return request;
    }

    private Map<String, Object> parseResponse(HttpResponse response) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        response.render(baos);
        String json = baos.toString(StandardCharsets.UTF_8);

        if (json.trim().isEmpty()) {
            return new HashMap<>();
        }

        return JsonXContent.jsonXContent
                .createParser(org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                        org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                        new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)))
                .map();
    }
}
