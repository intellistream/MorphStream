package intellistream.morphstream.common.io.Rdma.Msg;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class WWRegionTokenGroup {
    private final List<RegionToken> regionTokens;
    public WWRegionTokenGroup() {
        this.regionTokens = new ArrayList<>();
    }
    public void addRegionToken(RegionToken regionToken) {
        this.regionTokens.add(regionToken);
    }
    public void addRegionTokens(List<RegionToken> regionTokens) {
        this.regionTokens.addAll(regionTokens);
    }
    public int size() {
        return regionTokens.size();
    }
    public RegionToken getRemoteOperationToken() {
        return regionTokens.get(regionTokens.size() - 1);
    }
}
