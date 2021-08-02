package datatypes;


import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;

public class Event implements Comparable<Event>, Serializable {
    private static final long serialVersionUID = -5885564359171527196L;
    public final String key;
    public final long timestamp;

    public Event() {
        this.key = "";
        this.timestamp = 0;
    }

    public Event(String key, long timestamp) {
        this.key = key;
        this.timestamp = timestamp;
    }

    public String getKey() {
        return key;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Event{" + "key='" + key + '\'' + ", timestamp=" + timestamp + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        Event event = (Event) o;
        return timestamp == event.timestamp && Objects.equals(key, event.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, timestamp);
    }

    @Override
    public int compareTo(@Nullable Event other) {
        if (other == null) {
            return 1;
        }

        int compareKeys = this.getKey().compareTo(other.getKey());

        return compareKeys == 0 ?
               Long.compare(this.getTimestamp(), other.getTimestamp()) :
               compareKeys;
    }
}
