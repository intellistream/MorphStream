package runtimeweb.service;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractService {
    public final String PATH = "data/jobs";
    public final ObjectMapper objectMapper = new ObjectMapper();

}
