package org.codelibs.vespa.opensearch.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.codelibs.vespa.opensearch.client.VespaClient;
import org.codelibs.vespa.opensearch.exception.VespaClientException;
import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.json.JsonXContent;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;

public class DocumentActionExecutionTests {

    private RestApiProxyHandler handler;
    private VespaClient vespaClient;
    private DocumentAction action;

    @BeforeEach
    void setUp() {
        handler = mock(RestApiProxyHandler.class);
        vespaClient = mock(VespaClient.class);
        when(handler.getVespaClient()).thenReturn(vespaClient);
        when(handler.getDocumentType()).thenReturn("doc");
        action = new DocumentAction(handler);
    }

    @Test
    void testCreateDocument() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of(
                "title", "Test Document",
                "content", "This is a test");
        HttpRequest request = createMockRequest("POST", "/myindex/_doc/doc1", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("id", "doc1");
        when(vespaClient.insert(eq("myindex"), eq("doc"), eq("doc1"), anyMap())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(201, response.getStatus());
        verify(vespaClient, times(1)).insert(eq("myindex"), eq("doc"), eq("doc1"), anyMap());
    }

    @Test
    void testGetDocument() throws IOException {
        // Prepare request
        HttpRequest request = createMockRequest("GET", "/myindex/_doc/doc1", null);

        // Mock response
        Map<String, Object> vespaResponse = Map.of(
                "id", "doc1",
                "fields", Map.of("title", "Test Document"));
        when(vespaClient.get(eq("myindex"), eq("doc"), eq("doc1"))).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testGetNonExistentDocument() throws IOException {
        // Prepare request
        HttpRequest request = createMockRequest("GET", "/myindex/_doc/nonexistent", null);

        // Mock exception
        when(vespaClient.get(anyString(), anyString(), anyString()))
                .thenThrow(new VespaClientException("Document not found"));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 404
        assertEquals(404, response.getStatus());
    }

    @Test
    void testDeleteDocument() throws IOException {
        // Prepare request
        HttpRequest request = createMockRequest("DELETE", "/myindex/_doc/doc1", null);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("id", "doc1");
        when(vespaClient.delete(eq("myindex"), eq("doc"), eq("doc1"))).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(200, response.getStatus());
    }

    @Test
    void testDeleteNonExistentDocument() throws IOException {
        // Prepare request
        HttpRequest request = createMockRequest("DELETE", "/myindex/_doc/nonexistent", null);

        // Mock exception
        when(vespaClient.delete(anyString(), anyString(), anyString()))
                .thenThrow(new VespaClientException("Document not found"));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 404
        assertEquals(404, response.getStatus());
    }

    @Test
    void testCreateDocumentWithInvalidJson() throws IOException {
        // Prepare request with invalid JSON
        HttpRequest request = mock(HttpRequest.class);
        when(request.getUri()).thenReturn(java.net.URI.create("http://localhost/myindex/_doc/doc1"));
        when(request.getData()).thenReturn(new ByteArrayInputStream("{invalid json".getBytes()));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 400
        assertEquals(400, response.getStatus());
    }

    @Test
    void testCreateDocumentWithoutBody() throws IOException {
        // Prepare request without body
        HttpRequest request = createMockRequest("POST", "/myindex/_doc/doc1", null);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 400
        assertEquals(400, response.getStatus());
    }

    @Test
    void testPutDocument() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("title", "Updated Document");
        HttpRequest request = createMockRequest("PUT", "/myindex/_doc/doc1", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("id", "doc1");
        when(vespaClient.insert(eq("myindex"), eq("doc"), eq("doc1"), anyMap())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(201, response.getStatus());
    }

    @Test
    void testCreateDocumentWithVespaException() throws IOException {
        // Prepare request
        Map<String, Object> requestBody = Map.of("title", "Test");
        HttpRequest request = createMockRequest("POST", "/myindex/_doc/doc1", requestBody);

        // Mock exception
        when(vespaClient.insert(anyString(), anyString(), anyString(), anyMap()))
                .thenThrow(new VespaClientException("Vespa connection failed"));

        // Execute
        HttpResponse response = action.execute(request);

        // Verify - should return 500
        assertEquals(500, response.getStatus());
    }

    @Test
    void testCreateDocumentWithSpecialCharacters() throws IOException {
        // Prepare request with special characters
        Map<String, Object> requestBody = Map.of(
                "title", "Test \"quoted\" & <special> chars",
                "content", "Content with \n newlines \t tabs");
        HttpRequest request = createMockRequest("POST", "/myindex/_doc/doc-1", requestBody);

        // Mock response
        Map<String, Object> vespaResponse = Map.of("id", "doc-1");
        when(vespaClient.insert(anyString(), anyString(), anyString(), anyMap())).thenReturn(vespaResponse);

        // Execute
        HttpResponse response = action.execute(request);

        // Verify
        assertEquals(201, response.getStatus());
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
