package object.response;

import lombok.Data;

import java.io.Serializable;

@Data
public class AbstractResponse implements Serializable {
    private String type = "response";
    private String correlationId;
}
