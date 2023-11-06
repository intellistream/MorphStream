package communication.dao;

import lombok.Data;

@Data
public class JobSubmit {
    private String job;
    private int parallelism;
    private boolean startNow;
    private String code;
}
