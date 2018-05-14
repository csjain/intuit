package report.entity;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;

public class SalesInfoIterator implements Iterator, Serializable {
	private LinkedList<String> salesInfos;
	private static final long serialVersionUID = -348544421231396366L;

	public SalesInfoIterator() {
		salesInfos = new LinkedList<>();
	}

	public SalesInfoIterator addSalesInfo(String input) {
		if(SalesInfo.isValid(input)) {
			salesInfos.push(input);
		}
		return this;
	}

	@Override
	public boolean hasNext() {
		return salesInfos.size() > 0;
	}

	@Override
	public SalesInfo next() {
		return new SalesInfo(salesInfos.poll());
	}
}
