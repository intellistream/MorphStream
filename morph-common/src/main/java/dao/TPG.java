package dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TPG {
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, ConcurrentHashMap<TPGNode, List<TPGEdge>>>> tpgMap;
}
