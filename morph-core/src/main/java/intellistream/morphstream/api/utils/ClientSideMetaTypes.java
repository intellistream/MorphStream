package intellistream.morphstream.api.utils;

public interface ClientSideMetaTypes {
    enum AccessType {
        READ,
        WRITE,
        MODIFY,
        WRITE_READ,
        MODIFY_READ,
        WINDOWED_READ,
        NON_READ_WRITE_COND_READN
    }
}
