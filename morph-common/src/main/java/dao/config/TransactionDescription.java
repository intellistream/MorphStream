package dao.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionDescription {
    private String name;
    private List<StateAccessDescription> stateAccessDescription;
}
