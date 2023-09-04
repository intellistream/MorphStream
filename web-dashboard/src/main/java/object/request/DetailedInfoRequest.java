package object.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DetailedInfoRequest extends AbstractRequest {
    private String appId;

    public DetailedInfoRequest() {}

    public DetailedInfoRequest(String appId) {
        this.appId = appId;
    }
}
