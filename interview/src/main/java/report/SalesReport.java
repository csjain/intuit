package report;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import report.entity.CustomerInfo;
import report.entity.CustomerInfoIterator;
import report.entity.SalesInfo;
import report.entity.SalesInfoIterator;
import scala.Serializable;
import scala.Tuple2;

public class SalesReport implements Serializable {

	private static String[] HEADERS = {"State", "Year", "Month", "Day", "Hour", "Sales"};
	private static String REPORT_NAME = "SalesReport";

	private static final long serialVersionUID = -268544421231396366L;

	private String inputCustomerDataFilePath;
	private String inputSalesDataFilePath;
	private String outputSalesReportPath;
	private String sparkDriverHost;
	private String delimiter;

	public static void main(String[] args) {
		SalesReport report = new SalesReport();
		report.setConfiguration(args);
		report.process();
	}

	private void process() {
		JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(getSparkSession());
		JavaRDD<CustomerInfo> customerInfoDataset = getCustomerInfoDS(sparkContext);
		JavaRDD<SalesInfo> salesInfoDataset = getSalesInfoDS(sparkContext);

		JavaPairRDD<Integer, String> result = customerInfoDataset.mapToPair(customerInfo -> new Tuple2<>(customerInfo.getCustomerId(), customerInfo.getState()));
		Map<Integer, String> customerStateMap = result.collectAsMap();
		// TODO Ask JD if collectAsMap is right approach as per their needs
		/* 3. We encourage you to consider all possible cases of datasets like
			number of states are small(finitely known set) OR huge(unknown set)
			and come with appropriate solutions.
		*/

		JavaPairRDD<String, Long> salesReportEntries = salesInfoDataset.flatMapToPair(salesInfo -> getListOfTuples(salesInfo, customerStateMap).listIterator()).reduceByKey((x, y) -> x + y).sortByKey();
		// TODO Ask JD why union is not working here
		//getHeader(sparkContext).union(pairs).map(stringLongTuple2 -> stringLongTuple2._1() + stringLongTuple2._2()).saveAsTextFile(outputSalesReportPath);
		salesReportEntries.map(stringLongTuple2 -> stringLongTuple2._1() + stringLongTuple2._2()).saveAsTextFile(outputSalesReportPath);
	}

	private JavaRDD<CustomerInfo> getCustomerInfoDS(JavaSparkContext sparkContext) {
		CustomerInfoIterator customerInfoIterator = new CustomerInfoIterator();
		JavaRDD<String> javaRDD = sparkContext.textFile(inputCustomerDataFilePath);
		return javaRDD.flatMap(s -> customerInfoIterator.addCustomerInfo(s));
	}

	private JavaRDD<SalesInfo> getSalesInfoDS(JavaSparkContext sparkContext) {
		SalesInfoIterator salesInfoIterator = new SalesInfoIterator();
		JavaRDD<String> javaRDD = sparkContext.textFile(inputSalesDataFilePath);
		return javaRDD.flatMap(s -> salesInfoIterator.addSalesInfo(s));
	}

	private SparkContext getSparkSession() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName(REPORT_NAME);
		sparkConf.setMaster("local");
		sparkConf.set("spark.driver.host", sparkDriverHost);
		return new SparkContext(sparkConf);
	}

	private List<Tuple2<String, Long>> getListOfTuples(SalesInfo salesInfo, Map<Integer, String> customerStateMap) {
		List<Tuple2<String, Long>> tuple2s = new ArrayList<>();
		for (int level = 4; level >= 0 ; level--) {
			tuple2s.add(new Tuple2<>(getFilledKey(salesInfo, level, customerStateMap), salesInfo.getSalesPrice()));
		}
		return tuple2s;
	}

	private void setConfiguration(String[] args) {
		sparkDriverHost = args[0];
		inputCustomerDataFilePath = args[1];
		inputSalesDataFilePath = args[2];
		outputSalesReportPath = args[3];
		delimiter = args[4];
	}

	private JavaPairRDD<String, Long> getHeader(JavaSparkContext sparkContext) {
		String filledHeader = String.format(getKeyTemplate(Arrays.asList(HEADERS)), HEADERS);
		List<Tuple2<String, Long>> headerTuple = Arrays.asList(new Tuple2<>(filledHeader, 0l));
		JavaRDD rdd = sparkContext.parallelize(headerTuple, 1);
		return JavaPairRDD.fromJavaRDD(rdd);
	}

	private String getKeyTemplate(List<Object> args) {
		StringBuilder keyTemplate = new StringBuilder();
		args.forEach(o -> keyTemplate.append("%s").append(delimiter));
		return keyTemplate.toString();
	}

	private String getFilledKey(SalesInfo salesInfo, int level, Map<Integer, String> customerStateMap) {
		DateTime dateTime = new DateTime(salesInfo.getTimestamp(), DateTimeZone.UTC);
		List<Object> args = new ArrayList<>();
		args.add(customerStateMap.get(salesInfo.getCustomerId()));
		args.add(level > 0 ? String.valueOf(dateTime.getYear()) : "");
		args.add(level > 1 ? String.valueOf(dateTime.getMonthOfYear()) : "");
		args.add(level > 2 ? String.valueOf(dateTime.getDayOfMonth()) : "");
		args.add(level > 3 ? String.valueOf(dateTime.getHourOfDay()) : "");
		return String.format(getKeyTemplate(args), args.toArray());
	}
}
