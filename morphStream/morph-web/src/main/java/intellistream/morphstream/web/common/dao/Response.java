package intellistream.morphstream.web.common.dao;

import lombok.Data;
import lombok.ToString;

@Data
@ToString(callSuper = true)
public class Response<T> {
    private String type = "response";
    private String correlationId;
    private T data;
}
