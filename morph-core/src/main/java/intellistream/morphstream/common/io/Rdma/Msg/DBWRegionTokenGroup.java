package intellistream.morphstream.common.io.Rdma.Msg;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class DBWRegionTokenGroup {
    private final List<RegionToken> regionTokens;
    public DBWRegionTokenGroup() {
        this.regionTokens = new ArrayList<>();
    }
    public void addRegionToken(RegionToken regionToken) {
        this.regionTokens.add(regionToken);
    }

    public void addRegionTokens(List<RegionToken> remoteRegionToken) {
        this.regionTokens.addAll(remoteRegionToken);
    }
}
