package dao.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StateAccessDescription {
    private String name;
    private String accessType;
    private List<StateObjectDescription> stateObjectDescription;
    private String valueName;
}
