package org.codelibs.vespa.opensearch.action;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.function.Function;

import org.codelibs.curl.CurlException;
import org.codelibs.curl.CurlResponse;
import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;

public abstract class HttpAction {

    protected RestApiProxyHandler handler;

    protected HttpAction(final RestApiProxyHandler handler) {
        this.handler = handler;
    }

    public abstract boolean isTarget(String contentType, String path);

    public abstract HttpResponse execute(HttpRequest httpRequest);

    protected HttpResponse createResponse(final HttpRequest httpRequest, final int status, final Map<String, Object> result) {
        return new HttpResponse(status) {
            @Override
            public void render(final OutputStream stream) throws IOException {
                try (XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, stream)) {
                    builder.value(result);
                }
            }
        };
    }

    protected static final Function<CurlResponse, Map<String, Object>> PARSER = response -> {
        try (InputStream is = response.getContentAsStream()) {
            return JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, is).map();
        } catch (final Exception e) {
            throw new CurlException("Failed to access the content.", e);
        }
    };
}
