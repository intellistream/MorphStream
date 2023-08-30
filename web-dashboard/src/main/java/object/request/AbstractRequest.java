package object.request;

import lombok.Data;

import java.io.Serializable;

@Data
public abstract class AbstractRequest implements Serializable {
    private String correlationId;
}
