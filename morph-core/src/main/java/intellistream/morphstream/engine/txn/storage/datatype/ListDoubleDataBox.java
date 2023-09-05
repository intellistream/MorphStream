package intellistream.morphstream.engine.txn.storage.datatype;

import java.util.LinkedList;
import java.util.List;

public class ListDoubleDataBox extends DataBox {
    private final int movingAverageWindow;
    private final LinkedList<Double> list;

    /**
     * Construct an empty StringDataBox.
     *
     * @param movingAverageWindow
     */
    public ListDoubleDataBox(int movingAverageWindow) {
        this.movingAverageWindow = movingAverageWindow;
        this.list = new LinkedList<>();
    }

    public int getSize() throws DataBoxException {
        return 8 * list.size();
    }

    @Override
    public DataBox clone() {
        ListDoubleDataBox dataBox = new ListDoubleDataBox(movingAverageWindow);
        for (Double item : this.getDoubleList()) {
            dataBox.addItem(item);
        }
        return dataBox;
    }

    @Override
    public List<Double> getDoubleList() {
        return this.list;
    }

    @Override
    public double addItem(Double nextDouble) {
        double valueToRemove = 0;
        if (list.size() > movingAverageWindow - 1) {
            valueToRemove = list.removeFirst();
        }
        list.addLast(nextDouble);
        return valueToRemove;
    }

    @Override
    public Types type() {
        return Types.STRING;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (this == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        ListDoubleDataBox other = (ListDoubleDataBox) obj;
        return this.getString().equals(other.getString());
    }

    @Override
    public int hashCode() {
        return this.getString().hashCode();
    }

    public double size() {
        return list.size();
    }

    @Override
    public String toString() throws DataBoxException {
        return list.toString();
    }
}
