package intellistream.morphstream.web.common.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class BasicInfoRequest extends AbstractRequest {
    private String appId;

    public BasicInfoRequest() {}

    public BasicInfoRequest(String appId) {
        this.appId = appId;
    }
}
