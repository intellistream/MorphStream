package storage.datatype;

import org.apache.commons.lang.ArrayUtils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ListStringDataBox extends DataBox {
    private final int movingAverageWindow;
    private LinkedList<String> list;

    /**
     * Construct an empty StringDataBox.
     *
     * @param movingAverageWindow
     */
    public ListStringDataBox(int movingAverageWindow) {
        this.movingAverageWindow = movingAverageWindow;
        this.list = new LinkedList<>();
    }

    public ListStringDataBox(String[] list) {
        this.movingAverageWindow = 100; //TODO: Check this
        this.list = new LinkedList<>(Arrays.asList(list));
    }

    public ListStringDataBox() {
        this.movingAverageWindow = 100; //TODO: Check this
        this.list = new LinkedList<>();
    }

    public int getSize() throws DataBoxException {
        return 8 * list.size();
    }

    @Override
    public List<String> getStringList() {
        return this.list;
    }

    @Override
    public void setStringList(List<String> list) {
        this.list = new LinkedList<>(list);
    }

    @Override
    public DataBox clone() {
        ListStringDataBox dataBox = new ListStringDataBox(movingAverageWindow);
        for (String item : this.getStringList()) {
            dataBox.addItem(item);
        }
        return dataBox;
    }

    @Override
    public String addItem(String nextString) {
        String valueToRemove = "";
        if (list.size() > movingAverageWindow - 1) {
            valueToRemove = list.removeFirst();
        }
        list.addLast(nextString);
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
        ListStringDataBox other = (ListStringDataBox) obj;
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
