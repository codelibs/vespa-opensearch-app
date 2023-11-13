package org.codelibs.vespa.opensearch.action;

import java.util.HashMap;
import java.util.Map;

import org.codelibs.vespa.opensearch.handler.RestApiProxyHandler;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;

public class RootAction extends HttpAction {

    public RootAction(final RestApiProxyHandler handler) {
        super(handler);
    }

    @Override
    public boolean isTarget(final String contentType, final String path) {
        return "/".equals(path);
    }

    @Override
    public HttpResponse execute(final HttpRequest httpRequest) {
        /*
        {
          "name" : "es01",
          "cluster_name" : "codelibs-es",
          "cluster_uuid" : "xHKpajYgQ4Ofemgup2G5yg",
          "version" : {
            "number" : "8.11.0",
            "build_flavor" : "default",
            "build_type" : "docker",
            "build_hash" : "d9ec3fa628c7b0ba3d25692e277ba26814820b20",
            "build_date" : "2023-11-04T10:04:57.184859352Z",
            "build_snapshot" : false,
            "lucene_version" : "9.8.0",
            "minimum_wire_compatibility_version" : "7.17.0",
            "minimum_index_compatibility_version" : "7.0.0"
          },
          "tagline" : "You Know, for Search"
        }
         */
        final Map<String, Object> result = new HashMap<>();
        result.put("name", "_na_");
        result.put("cluster_name", "_na_");
        result.put("cluster_uuid", "_na_");
        result.put("tagline", "You Know, for Search");
        final Map<String, Object> version = new HashMap<>();
        result.put("version", version);
        version.put("number", "_na_");
        version.put("build_flavor", "_na_");
        version.put("build_type", "_na_");
        version.put("build_hash", "_na_");
        version.put("build_snapshot", false);
        version.put("lucene_version", false);
        version.put("minimum_wire_compatibility_version", "7.17.0");
        version.put("minimum_index_compatibility_version", "7.0.0");
        return createResponse(httpRequest, 200, result);
    }

}
