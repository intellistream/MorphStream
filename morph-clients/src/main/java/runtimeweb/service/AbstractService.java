package runtimeweb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

public abstract class AbstractService {
    public final String PATH = MorphStreamEnv.get().configuration().getString("dataPath", "data/jobs");
    public final ObjectMapper objectMapper = new ObjectMapper();

}
