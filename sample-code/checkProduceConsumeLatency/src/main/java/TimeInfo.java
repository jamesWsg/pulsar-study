
import org.apache.pulsar.shade.com.google.common.base.MoreObjects;

import java.io.Serializable;

public class TimeInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    String id;
    long createTime = 0;

    public TimeInfo() {
    }

    public TimeInfo(String id, long createTime) {
        this.id = id;
        this.createTime = createTime;
    }

    public String getId() {
        return id;
    }

    public long getCreateTime() {
        return createTime;
    }



    public void setId(String id) {
        this.id = id;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }



    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("createTime", createTime)
                .toString();
    }
}