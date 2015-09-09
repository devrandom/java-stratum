package org.smartwallet.multi;

import com.google.protobuf.ByteString;

import java.util.Set;

/**
 * Created by devrandom on 2015-09-09.
 */
public interface AddressableKeyChain {
    public Set<ByteString> getP2SHHashes(); 
}
