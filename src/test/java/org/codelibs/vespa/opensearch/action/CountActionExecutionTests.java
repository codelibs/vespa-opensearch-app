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

public class CountActionExecutionTests {

    private RestApiProxyHandler handler;
    private VespaClient vespaClient;
    private CountAction action;

    @BeforeEach
    void setUp() {
        handler = mock(RestApiProxyHandler.class);
        vespaClient = mock(VespaClient.class);
        when(handler.getVespaClient()).thenReturn(vespaClient);
        when(handler.getDocumentType()).thenReturn("doc");
        action = new CountAction(handler);
    }

    @Test
    void testCountWithMatchAllQuery() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("query", Map.of("match_all", new HashMap<>()));
        HttpRequest request = createMockRequest("POST", "/_count", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("count", 100);
        when(vespaClient.count(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertNotNull(responseBody);
        assertEquals(100, responseBody.get("count"));
    }

    @Test
    void testCountWithMatchQuery() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("match", Map.of("title", "test")));
        HttpRequest request = createMockRequest("POST", "/myindex/_count", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("count", 42);
        when(vespaClient.count(eq("myindex"), eq("doc"), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testCountWithBoolQuery() throws IOException {
        // Prepare complex bool query
        Map<String, Object> requestBody = Map.of(
                "query", Map.of(
                        "bool", Map.of(
                                "must", List.of(
                                        Map.of("match", Map.of("title", "test")),
                                        Map.of("range", Map.of("price", Map.of("gte", 10, "lte", 100)))),
                                "must_not", List.of(
                                        Map.of("term", Map.of("status", "deleted"))))));
        HttpRequest request = createMockRequest("POST", "/_count", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("count", 15);
        when(vespaClient.count(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testCountWithEmptyBody() throws IOException {
        // Prepare request with no body
        HttpRequest request = createMockRequest("GET", "/_count", null);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("count", 1000);
        when(vespaClient.count(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testCountWithInvalidJson() throws IOException {
        // Prepare request with invalid JSON
        HttpRequest request = mock(HttpRequest.class);
        when(request.getUri()).thenReturn(java.net.URI.create("http://localhost/_count"));
        when(request.getData()).thenReturn(new ByteArrayInputStream("{invalid json".getBytes()));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 400
        assertEquals(400, response.getStatus());
    }

    @Test
    void testCountWithVespaClientException() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("query", Map.of("match_all", new HashMap<>()));
        HttpRequest request = createMockRequest("POST", "/_count", requestBody);

        // Mock exception
        when(vespaClient.count(anyString(), anyString(), any()))
                .thenThrow(new VespaClientException("Vespa connection failed"));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 500
        assertEquals(500, response.getStatus());
    }

    @Test
    void testCountWithTermQuery() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("term", Map.of("status", "active")));
        HttpRequest request = createMockRequest("POST", "/_count", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("count", 250);
        when(vespaClient.count(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testCountWithRangeQuery() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of(
                "query", Map.of(
                        "range", Map.of(
                                "timestamp", Map.of(
                                        "gte", "2024-01-01",
                                        "lt", "2024-12-31"))));
        HttpRequest request = createMockRequest("POST", "/_count", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("count", 500);
        when(vespaClient.count(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testCountWithSpecificIndex() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("query", Map.of("match_all", new HashMap<>()));
        HttpRequest request = createMockRequest("POST", "/products/_count", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("count", 75);
        when(vespaClient.count(eq("products"), eq("doc"), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testCountReturnsZero() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of(
                "query", Map.of("term", Map.of("nonexistent_field", "value")));
        HttpRequest request = createMockRequest("POST", "/_count", requestBody);

        // Mock response with zero count
        Map<String, Object> vespaResponse = Map.of("count", 0);
        when(vespaClient.count(anyString(), anyString(), any())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
        Map<String, Object> responseBody = parseResponse(response);
        assertEquals(0, responseBody.get("count"));
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
