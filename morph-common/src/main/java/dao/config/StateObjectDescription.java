package dao.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StateObjectDescription {
    private String name;
    private String accessType;
    private String tableName;
    private String keyName;
    private String valueName;
    private int keyIndex;
}
