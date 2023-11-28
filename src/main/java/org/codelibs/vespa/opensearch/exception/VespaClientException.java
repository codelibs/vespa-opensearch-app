package org.codelibs.vespa.opensearch.exception;

public class VespaClientException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public VespaClientException(final String msg, final Exception e) {
        super(msg, e);
    }

    public VespaClientException(final String msg) {
        super(msg);
    }

}
