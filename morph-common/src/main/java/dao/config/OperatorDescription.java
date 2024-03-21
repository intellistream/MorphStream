package dao.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OperatorDescription {
    private String name;
    private List<TransactionDescription> transactionDescription;
}
