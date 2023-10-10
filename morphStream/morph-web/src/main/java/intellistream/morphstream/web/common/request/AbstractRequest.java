package intellistream.morphstream.web.common.request;

import lombok.Data;

import java.io.Serializable;

@Data
public abstract class AbstractRequest implements Serializable {
    private String correlationId;
}
