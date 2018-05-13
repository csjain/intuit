package report;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import report.entity.CustomerInfo;
import report.entity.SalesInfo;
import scala.Serializable;
import scala.Tuple2;

public class SalesReport implements Serializable{

	private static String[] HEADERS = {"State","Year","Month","Day","Hour","Sales"};

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
		SparkSession sparkSession = getSparkSession();
		Dataset<CustomerInfo> customerInfoDataset = getCustomerInfoDS(sparkSession);
		Dataset<SalesInfo> salesInfoDataset = getSalesInfoDS(sparkSession);

		JavaPairRDD<Integer, String> result = customerInfoDataset.toJavaRDD().mapToPair(customerInfo -> new Tuple2<>(customerInfo.getCustomerId(), customerInfo.getState()));
		Map<Integer, String> customerStateMap = result.collectAsMap();
		// TODO Ask JD if collectAsMap is right approach as per their needs
		/* 3. We encourage you to consider all possible cases of datasets like
			number of states are small(finitely known set) OR huge(unknown set)
			and come with appropriate solutions.
		*/


		JavaPairRDD<String, Long> pairs = salesInfoDataset.toJavaRDD().flatMapToPair(salesInfo -> getListOfTuples(salesInfo, customerStateMap).listIterator()).reduceByKey((x, y) -> x + y);
		// TODO Ask JD why union is not working here
		//getHeader(sparkSession).union(pairs).map(stringLongTuple2 -> stringLongTuple2._1() + stringLongTuple2._2()).saveAsTextFile(outputSalesReportPath);
		pairs.map(stringLongTuple2 -> stringLongTuple2._1() + stringLongTuple2._2()).saveAsTextFile(outputSalesReportPath);
	}

	private Dataset<CustomerInfo> getCustomerInfoDS(SparkSession sparkSession) {
		return sparkSession.read().option("delimiter", delimiter).option("inferSchema", "true").csv(inputCustomerDataFilePath).toDF(CustomerInfo.COLUMNS).as(Encoders.bean(CustomerInfo.class));
	}

	private Dataset<SalesInfo> getSalesInfoDS(SparkSession sparkSession) {
		return sparkSession.read().option("delimiter", delimiter).option("inferSchema", "true").csv(inputSalesDataFilePath).toDF(SalesInfo.COLUMNS).as(Encoders.bean(SalesInfo.class));
	}

	private SparkSession getSparkSession() {
		return SparkSession.builder().
				master("local")
				.appName("SalesReport")
				.config("spark.driver.host", sparkDriverHost)
				.getOrCreate();
	}

	private List<Tuple2<String, Long>> getListOfTuples(SalesInfo salesInfo, Map<Integer, String> customerStateMap) {
		List<Tuple2<String, Long>> tuple2s = new ArrayList<>();
		for (int level = 0; level <= 4; level++) {
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

	private JavaPairRDD<String,Long> getHeader(SparkSession sparkSession) {
		String filledHeader = String.format(getKeyTemplate(Arrays.asList(HEADERS)), HEADERS);
		List<Tuple2<String, Long>> headerTuple = Arrays.asList(new Tuple2<>(filledHeader, 0l));
		JavaRDD rdd = new JavaSparkContext(sparkSession.sparkContext()).parallelize(headerTuple, 1);
		return JavaPairRDD.fromJavaRDD(rdd);
	}

	private String getKeyTemplate(List<Object> args) {
		StringBuilder keyTemplate = new StringBuilder();
		args.forEach(o -> keyTemplate.append("%s").append(delimiter));
		return keyTemplate.toString();
	}

	private  String getFilledKey(SalesInfo salesInfo, int level, Map<Integer, String> customerStateMap) {
		DateTime dateTime = new DateTime(salesInfo.getTimestamp() * 1000, DateTimeZone.UTC);
		List<Object> args = new ArrayList<>();
		args.add(customerStateMap.get(salesInfo.getCustomerId()));
		args.add(level > 0 ? String.valueOf(dateTime.getYear()) : "");
		args.add(level > 1 ? String.valueOf(dateTime.getMonthOfYear()) : "");
		args.add(level > 2 ? String.valueOf(dateTime.getDayOfMonth()) : "");
		args.add(level > 3 ? String.valueOf(dateTime.getHourOfDay()) : "");
		return String.format(getKeyTemplate(args), args.toArray());
	}
}
