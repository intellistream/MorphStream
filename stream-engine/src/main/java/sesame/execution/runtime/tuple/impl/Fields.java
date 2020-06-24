package sesame.execution.runtime.tuple.impl;

import application.util.datatypes.StreamValues;

import java.io.Serializable;
import java.util.*;

/**
 * Created by shuhaozhang on 12/7/16.
 */
public class Fields implements Iterable<String>, Serializable {
    private static final long serialVersionUID = -3144488700411923598L;

    private final List<String> _fields;
    private final Map<String, Integer> _index = new HashMap<>();

    public Fields(String... fields) {
        this(Arrays.asList(fields));
    }

    public Fields(List<String> fields) {
        _fields = new ArrayList<>(fields.size());
        for (String field : fields) {
            if (_fields.contains(field)) {
                throw new IllegalArgumentException(
                        String.format("duplicate field '%s'", field)
                );
            }
            _fields.add(field);
        }
        index();
    }

    private void index() {
        for (int i = 0; i < _fields.size(); i++) {
            _index.put(_fields.get(i), i);
        }
    }

    @Override
    public Iterator<String> iterator() {
        return _fields.iterator();
    }


    public List<Object> select(Fields input_fields, Object... tuple) {

        List<Object> ret = new ArrayList<>(input_fields._fields.size());
        for (String s : input_fields._fields) {
            ret.add(tuple[(_index.get(s))]);
        }
        return ret;
    }

    public List<Object> select(Fields input_fields, StreamValues tuple) {

        List<Object> ret = new ArrayList<>(input_fields._fields.size());
        for (String s : input_fields._fields) {
            ret.add(tuple.get((_index.get(s))));
        }
        return ret;
    }

    public int fieldIndex(String field) {
        Integer ret = _index.get(field);
        if (ret == null) {
            throw new IllegalArgumentException(field + " does not exist");
        }
        return ret;
    }

    public List<String> toList() {
        return new ArrayList<>(_fields);
    }

    public String get(int i) {
        return _fields.get(i);
    }

    public int size() {
        return _fields.size();
    }
}
