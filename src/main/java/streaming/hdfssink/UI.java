package streaming.hdfssink;

public class UI {
	private String province;
	private String id;
	private long timestamp;
	private String date;
	private long count;

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "UI [province=" + province + ", id=" + id + ", timestamp=" + timestamp + ", date=" + date + ", count="
				+ count + "]";
	}

}