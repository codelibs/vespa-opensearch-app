package org.codelibs.vespa.opensearch.exception;

public class IncorrectHttpMethodException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IncorrectHttpMethodException(final String msg) {
        super(msg, null, false, false);
    }

}
