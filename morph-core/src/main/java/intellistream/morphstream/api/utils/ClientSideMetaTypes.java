package intellistream.morphstream.api.utils;

public interface ClientSideMetaTypes {
    enum AccessType {
        READ,
        WRITE,
        WINDOW_READ,
        WINDOW_WRITE,
        NON_DETER_READ,
        NON_DETER_WRITE
    }
}
