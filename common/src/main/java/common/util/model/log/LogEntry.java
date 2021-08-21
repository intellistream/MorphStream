package common.util.model.log;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static common.CONTROL.enable_log;

/**
 * Class that encapsulates the data and parsing logic for a partition log record (json) in a log file
 * Log record json structure sent from logstash:
 * {
 * "@source":"file://PATH",
 * "@tags":[],
 * "@fields":{},
 * "@timestamp":"yyyy-MM-ddThh:mm:ss.SSS",
 * "@source_host":"hostname",
 * "@source_path":"path",
 * "@message":"Syslog log line contents",
 * "@type":"syslog"
 * }
 */
public class LogEntry {
    public static final Logger LOG = LoggerFactory.getLogger(LogEntry.class);
    private static final String[] FORMATS = new String[]{
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy.MM.dd G 'at' HH:mm:ss z",
            "yyyyy.MMMMM.dd GGG hh:mm aaa",
            "EEE, d MMM yyyy HH:mm:ss Z",
            "yyMMddHHmmssZ"
    };
    private final List<String> tags = new ArrayList<>();
    private final Map<String, String> fields = new HashMap<>();
    private String source;
    private String type;
    private Date timestamp;
    private String sourceHost;
    private String sourcePath;
    private String message = "";
    private boolean filter = false;
    private NotificationDetails notifyAbout = null;

    public LogEntry(JSONObject json) {
        source = (String) json.get("@source");
        timestamp = parseDate((String) json.get("@timestamp"));
        sourceHost = (String) json.get("@source_host");
        sourcePath = (String) json.get("@source_path");
        message = (String) json.get("@message");
        type = (String) json.get("@type");
        JSONArray array = (JSONArray) json.get("@tags");
        tags.addAll(array);
        JSONObject fieldsTmp = (JSONObject) json.get("@fields");
        fields.putAll(fieldsTmp);
    }

    /*
     * The date format can vary from log entry to entry, therefore parseDate() provides a best effort approach to
     * parsing the date
     */
    public final Date parseDate(String value) {
        for (String format : FORMATS) {
            SimpleDateFormat dateFormat = new SimpleDateFormat(format);
            Date temp;
            try {
                temp = dateFormat.parse(value);
                if (temp != null)
                    return temp;
            } catch (ParseException ignored) {
            }
        }
        if (enable_log) LOG.error("Could not parse timestamp for log");
        return null;
    }

    public JSONObject toJSON() {
        JSONObject json = new JSONObject();
        json.put("@source", source);
        json.put("@timestamp", DateFormat.getDateInstance().format(timestamp));
        json.put("@source_host", sourceHost);
        json.put("@source_path", sourcePath);
        json.put("@message", message);
        json.put("@type", type);
        JSONArray temp = new JSONArray();
        temp.addAll(tags);
        json.put("@tags", temp);
        JSONObject fieldTemp = new JSONObject();
        fieldTemp.putAll(fields);
        json.put("@fields", fieldTemp);
        return json;
    }

    public boolean isFilter() {
        return filter;
    }

    public void setFilter(boolean filter) {
        this.filter = filter;
    }

    public NotificationDetails getNotificationDetails() {
        return notifyAbout;
    }

    public void notifyAbout(NotificationDetails notifyAbout) {
        this.notifyAbout = notifyAbout;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public List<String> getTags() {
        return tags;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public void addField(String name, String value) {
        fields.put(name, value);
    }

    public void addTag(String tag) {
        tags.add(tag);
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getSourceHost() {
        return sourceHost;
    }

    public void setSourceHost(String sourceHost) {
        this.sourceHost = sourceHost;
    }

    public String getSourcePath() {
        //sourcePath.contains("");
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public String getMessage() {
        return message;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + (filter ? 1231 : 1237);
        result = prime * result + ((message == null) ? 0 : message.hashCode());
        result = prime * result + ((source == null) ? 0 : source.hashCode());
        result = prime * result
                + ((sourceHost == null) ? 0 : sourceHost.hashCode());
        result = prime * result
                + ((sourcePath == null) ? 0 : sourcePath.hashCode());
        result = prime * result + ((tags == null) ? 0 : tags.hashCode());
        result = prime * result
                + ((timestamp == null) ? 0 : timestamp.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LogEntry other = (LogEntry) obj;
        if (fields == null) {
            if (other.fields != null)
                return false;
        } else if (!fields.equals(other.fields))
            return false;
        if (filter != other.filter)
            return false;
        if (message == null) {
            if (other.message != null)
                return false;
        } else if (!message.equals(other.message))
            return false;
        if (source == null) {
            if (other.source != null)
                return false;
        } else if (!source.equals(other.source))
            return false;
        if (sourceHost == null) {
            if (other.sourceHost != null)
                return false;
        } else if (!sourceHost.equals(other.sourceHost))
            return false;
        if (sourcePath == null) {
            if (other.sourcePath != null)
                return false;
        } else if (!sourcePath.equals(other.sourcePath))
            return false;
        if (tags == null) {
            if (other.tags != null)
                return false;
        } else if (!tags.equals(other.tags))
            return false;
        if (timestamp == null) {
            if (other.timestamp != null)
                return false;
        } else if (!timestamp.equals(other.timestamp))
            return false;
        if (type == null) {
            return other.type == null;
        } else {
            return type.equals(other.type);
        }
    }
}
