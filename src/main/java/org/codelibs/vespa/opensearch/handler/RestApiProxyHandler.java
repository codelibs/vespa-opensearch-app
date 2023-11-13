package org.codelibs.vespa.opensearch.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.logging.Level;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.vespa.opensearch.action.HttpAction;
import org.codelibs.vespa.opensearch.action.RootAction;
import org.codelibs.vespa.opensearch.config.ProxyHandlerConfig;
import org.codelibs.vespa.opensearch.exception.IncorrectHttpMethodException;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.container.jdisc.ThreadedHttpRequestHandler;
import com.yahoo.jdisc.Metric;
import com.yahoo.jdisc.http.HttpRequest.Method;

// https://github.com/vespa-engine/sample-apps/blob/master/text-search/src/main/java/ai/vespa/example/text_search/site/data/SimpleHttpClient.java

public class RestApiProxyHandler extends ThreadedHttpRequestHandler {

    private final String vespaHostName;
    private final String vespaHostPort;
    private final String pathPrefix;

    private final Map<Method, HttpAction[]> actions;

    @Inject
    public RestApiProxyHandler(final Executor executor, final Metric metric, final ProxyHandlerConfig config) {
        super(executor, metric);
        this.vespaHostName = config.vespaHostName();
        this.vespaHostPort = String.valueOf(config.vespaHostPort());
        this.pathPrefix = config.pathPrefix();

        actions = ImmutableMap.<Method, HttpAction[]> builder()//
                .put(Method.GET, new HttpAction[] { new RootAction(this) })//
                .build();
    }

    @Override
    public HttpResponse handle(final HttpRequest httpRequest) {
        log.log(Level.INFO, () -> "Method:         " + httpRequest.getMethod());
        log.log(Level.INFO, () -> "URI:            " + httpRequest.getUri());
        log.log(Level.INFO, () -> "Path:           " + httpRequest.getUri().getPath());
        log.log(Level.INFO, () -> "Content-Type:   " + httpRequest.getHeader("Content-Type"));

        final String path = getPath(httpRequest);
        final HttpAction[] httpActions = actions.get(httpRequest.getMethod());
        if (httpActions != null) {
            final String contentType = httpRequest.getHeader("Content-Type");
            for (final HttpAction action : httpActions) {
                if (action.isTarget(contentType, path)) {
                    return action.execute(httpRequest);
                }
            }
        }
        return handleException(httpRequest, 405, new IncorrectHttpMethodException(
                "Incorrect HTTP method for uri [" + path + "] and method [" + httpRequest.getMethod() + "]"));
    }

    private String getPath(final HttpRequest httpRequest) {
        final String path = httpRequest.getUri().getPath();
        if (path == null || path.length() <= pathPrefix.length()) {
            return "/";
        }
        return path.substring(pathPrefix.length());
    }

    private HttpResponse handleException(final HttpRequest httpRequest, final int status, final Exception e) {
        return new HttpAction(this) {

            @Override
            public boolean isTarget(final String contentType, final String path) {
                return false;
            }

            @Override
            public HttpResponse execute(final HttpRequest httpRequest) {
                /*
                {
                  "error": {
                    "root_cause": [
                      {
                        "type": "index_not_found_exception",
                        "reason": "no such index [xxx]",
                        "resource.type": "index_or_alias",
                        "resource.id": "xxx",
                        "index_uuid": "_na_",
                        "index": "xxx"
                      }
                    ],
                    "type": "index_not_found_exception",
                    "reason": "no such index [xxx]",
                    "resource.type": "index_or_alias",
                    "resource.id": "xxx",
                    "index_uuid": "_na_",
                    "index": "xxx"
                  },
                  "status": 404
                }
                 */
                final Map<String, Object> result = new HashMap<>();
                result.put("status", status);
                final Map<String, Object> error = new HashMap<>();
                result.put("error", error);
                error.put("type", e == null ? "_na_" : StringUtil.decamelize(e.getClass().getSimpleName()).toLowerCase());
                error.put("reason", e == null ? "_na_" : e.getMessage());
                // error.put("resource.type", "_na_");
                // error.put("resource.id", "_na_");
                error.put("index_uuid", "_na_");
                error.put("index", "_na_");
                final List<Map<String, Object>> rootCauses = new ArrayList<>();
                error.put("root_cause", rootCauses);
                return createResponse(httpRequest, status, result);
            }
        }.execute(httpRequest);
    }

}
