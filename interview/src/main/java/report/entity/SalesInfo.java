package report.entity;

import java.io.Serializable;

public class SalesInfo implements Serializable {
	private long timestamp;
	private int customerId;
	private long salesPrice;

	public  static final String[] COLUMNS = {"timestamp","customerId","salesPrice"};

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public int getCustomerId() {
		return customerId;
	}

	public void setCustomerId(int customerId) {
		this.customerId = customerId;
	}

	public long getSalesPrice() {
		return salesPrice;
	}

	public void setSalesPrice(long salesPrice) {
		this.salesPrice = salesPrice;
	}
}
