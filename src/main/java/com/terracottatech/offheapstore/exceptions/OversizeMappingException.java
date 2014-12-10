/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.exceptions;

/**
 *
 * @author Chris Dennis
 */
public class OversizeMappingException extends IllegalArgumentException {

    private static final long serialVersionUID = 3918022751469816074L;

    public OversizeMappingException() {
        super();
    }

    public OversizeMappingException(String s) {
        super(s);
    }

    public OversizeMappingException(String message, Throwable cause) {
        super(message, cause);
    }

    public OversizeMappingException(Throwable cause) {
        super(cause);
    }
}
