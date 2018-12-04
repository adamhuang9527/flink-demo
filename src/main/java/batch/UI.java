package batch;

public class UI {
	private Long datestamp;
	private String id;
	private String province;
	private String city;
	private String page;
	private String biz;
	private String cspot;
	private Integer day;

	public Long getDatestamp() {
		return datestamp;
	}

	public void setDatestamp(Long datestamp) {
		this.datestamp = datestamp;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getPage() {
		return page;
	}

	public void setPage(String page) {
		this.page = page;
	}

	public String getBiz() {
		return biz;
	}

	public void setBiz(String biz) {
		this.biz = biz;
	}

	public String getCspot() {
		return cspot;
	}

	public void setCspot(String cspot) {
		this.cspot = cspot;
	}

	public Integer getDay() {
		return day;
	}

	public void setDay(Integer day) {
		this.day = day;
	}

	@Override
	public String toString() {
		return "UI [datestamp=" + datestamp + ", id=" + id + ", province=" + province + ", city=" + city + ", page="
				+ page + ", biz=" + biz + ", cspot=" + cspot + ", day=" + day + "]";
	}

}
