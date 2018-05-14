package report.entity;

import java.io.Serializable;

public class SalesInfo implements Serializable {

	private static final long serialVersionUID = -2682131318382696366L;

	private long timestamp;
	private int customerId;
	private long salesPrice;

	public  static final String[] COLUMNS = {"timestamp","customerId","salesPrice"};

	public SalesInfo() {

	}

	public SalesInfo(String input) {
		this();
		fillFields(input);
	}

	private void fillFields(String input) {
		String[] fields = input.split("#");
		timestamp = Long.parseLong(fields[0]);
		customerId = Integer.parseInt(fields[1]);
		salesPrice = Long.parseLong(fields[2]);
	}

	public static boolean isValid(String input) {
		try {
			SalesInfo salesInfo = new SalesInfo();
			salesInfo.fillFields(input);
			return true;
		} catch(Exception e) {
			System.out.printf("Exception while handling sales input %s\n",input);
			e.printStackTrace();
		}
		return false;
	}

	public long getTimestamp() {
		return timestamp * 1000;
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
