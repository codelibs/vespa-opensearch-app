package org.codelibs.vespa.opensearch.action;

import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.jdisc.http.HttpRequest.Method;

public class IndexAction extends HttpAction {

    public IndexAction(RestApiProxyHandler handler) {
        super(handler);
    }

    @Override
    public boolean isTarget(final Method method, final String[] paths) {
        return getNamespace(method, paths) != null;
    }

    private String getNamespace(final Method method, final String[] paths) {
        // POST /<target>/_doc/
        // POST /<target>/_create/<_id>
        // PUT /<target>/_doc/<_id>
        // PUT /<target>/_create/<_id>
        switch (method) {
        case POST:
            if (paths.length == 3) {
                if ("_doc".equals(paths[2])) {
                    return paths[1];
                }
            } else if (paths.length == 4) {
                if ("_doc".equals(paths[2]) && paths[3].length() == 0) {
                    return paths[1];
                }
                if ("_create".equals(paths[2]) && paths[3].length() > 0) {
                    return paths[1];
                }
            }
            break;
        case PUT:
            if (paths.length == 4) {
                if ("_doc".equals(paths[2]) && paths[3].length() > 0) {
                    return paths[1];
                }
                if ("_create".equals(paths[2]) && paths[3].length() > 0) {
                    return paths[1];
                }
            }
            break;
        default:
            break;
        }
        return null;
    }

    @Override
    public HttpResponse execute(HttpRequest httpRequest) {
        // TODO Auto-generated method stub
        return null;
    }

}
