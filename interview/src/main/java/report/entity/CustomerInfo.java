package report.entity;

import java.io.Serializable;

public class CustomerInfo implements Serializable {

	private static final long serialVersionUID = -2621344218382696366L;

	private int customerId;
	private String name;
	private String street;
	private String city;
	private String state;
	private int zipcode;

	public CustomerInfo() {

	}

	public CustomerInfo(String input) {
		this();
		fillFields(input);
	}

	private void fillFields(String input) {
		String[] fields = input.split("#");
		customerId = Integer.parseInt(fields[0]);
		name = fields[1];
		street = fields[2];
		city = fields[3];
		state = fields[4];
		zipcode = Integer.parseInt(fields[5]);
	}

	public static boolean isValid(String input) {
		try {
			CustomerInfo customerInfo = new CustomerInfo();
			customerInfo.fillFields(input);
			return true;
		} catch(Exception e) {
			System.out.printf("Exception while handling customer input %s\n",input);
			e.printStackTrace();
		}
		return false;
	}


	public  static final String[] COLUMNS = {"customerId","name","street","city","state","zipcode"};


	public int getCustomerId() {
		return customerId;
	}

	public void setCustomerId(int customerId) {
		this.customerId = customerId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public int getZipcode() {
		return zipcode;
	}

	public void setZipcode(int zipcode) {
		this.zipcode = zipcode;
	}



}
