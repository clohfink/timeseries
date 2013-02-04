package com.digi.data.timeseries;

import java.io.IOException;

public class DataServiceException extends IOException {
    private static final long serialVersionUID = 3644338652664769099L;

    public DataServiceException() {
        super();
    }

    public DataServiceException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    public DataServiceException(String arg0) {
        super(arg0);
    }

    public DataServiceException(Throwable arg0) {
        super(arg0);
    }
}
