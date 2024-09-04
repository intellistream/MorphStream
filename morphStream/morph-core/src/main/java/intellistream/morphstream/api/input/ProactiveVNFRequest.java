package intellistream.morphstream.api.input;

import intellistream.morphstream.transNFV.common.VNFRequest;

/**
 * A class wrapper to embed input VNFRequest in a MorphStream-based transactional event.
 */

public class ProactiveVNFRequest extends TransactionalEvent {
    private VNFRequest vnfRequest;

    public ProactiveVNFRequest(VNFRequest vnfRequest) {
        super(vnfRequest.getCreateTime());
        this.vnfRequest = vnfRequest;
    }

    public ProactiveVNFRequest(long bid) {
        super(bid);
    }

    public VNFRequest getVnfRequest() {
        return this.vnfRequest;
    }
}
