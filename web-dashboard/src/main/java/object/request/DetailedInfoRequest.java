package object.request;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class DetailedInfoRequest extends AbstractRequest {
    private String appId;

    public DetailedInfoRequest() {}

    public DetailedInfoRequest(String appId) {
        this.appId = appId;
    }
}
