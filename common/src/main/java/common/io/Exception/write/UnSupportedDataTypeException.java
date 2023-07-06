package common.io.Exception.write;

public class UnSupportedDataTypeException extends RuntimeException{
    public UnSupportedDataTypeException(String dataTypeName) {
        super("Unsupported dataType: " + dataTypeName);
    }
}
