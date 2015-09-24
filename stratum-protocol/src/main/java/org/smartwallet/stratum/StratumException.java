package org.smartwallet.stratum;

/**
 * Created by devrandom on 2015-Sep-04.
 */
public class StratumException extends RuntimeException {
    public StratumException(String error) {
        super(error);
    }
}
