package intellistream.morphstream.web.common.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SignalRequest extends AbstractRequest {
    private String appId;
    private String signal;
    public SignalRequest() {}
    public SignalRequest(String appId, String signal) {
        this.appId = appId;
        this.signal = signal;
    }
}
