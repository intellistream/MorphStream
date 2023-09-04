package object;

import lombok.Data;
import lombok.ToString;

@Data
@ToString(callSuper = true)
public class Response<T> {
    private String type = "response";
    private String correlationId;
    private T data;
}
