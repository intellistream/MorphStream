package runtimeweb.common.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SignalResponse extends AbstractResponse {
    private String appId;
    private boolean jobStart;

    public SignalResponse(String appId, boolean jobStart) {
        this.appId = appId;
        this.jobStart = jobStart;
    }
}
