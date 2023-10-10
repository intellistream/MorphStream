package intellistream.morphstream.api.input;

public abstract class InputEvent {
    public String toString(int iterationNumber, int totalTransaction) {
        throw new UnsupportedOperationException("unsupported by abstract class");
    }
}
