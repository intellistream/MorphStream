package intellistream.morphstream.common.io.Rdma.Msg;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class DWRegionTokenGroup {
    private final List<RegionToken> regionTokens;
    public DWRegionTokenGroup() {
        this.regionTokens = new ArrayList<>();
    }
    public void addRegionToken(RegionToken regionTokens) {
        this.regionTokens.add(regionTokens);
    }
    public void addRegionTokens(List<RegionToken> regionTokens) {
        this.regionTokens.addAll(regionTokens);
    }
    public int size() {
        return regionTokens.size();
    }
    public RegionToken getResultsToken() {
        return regionTokens.get(0);
    }
}
