package report.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class CustomerInfoIterator implements Iterator, Serializable {
	private Queue<String> customerInfos;
	private static final long serialVersionUID = -5468544421231396366L;

	public CustomerInfoIterator() {
		customerInfos = new LinkedList<>();
	}

	public CustomerInfoIterator addCustomerInfo(String input) {
		if(CustomerInfo.isValid(input)) {
			customerInfos.add(input);
		}
		return this;
	}

	@Override
	public boolean hasNext() {
		return customerInfos.size() > 0;
	}

	@Override
	public CustomerInfo next() {
		return new CustomerInfo(customerInfos.poll());
	}
}
