package org.codelibs.vespa.opensearch.action;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.jdisc.http.HttpRequest.Method;

public abstract class HttpAction {

    protected RestApiProxyHandler handler;

    protected HttpAction(final RestApiProxyHandler handler) {
        this.handler = handler;
    }

    public abstract boolean isTarget(Method method, String[] paths);

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

}
