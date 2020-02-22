package formatfa.bigdata.gegeCore.etl.source;

import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import formatfa.bigdata.gegeCore.etl.SourceComponent;

public class SourceCsv extends SourceComponent {



	public SourceCsv(HashMap<String, String> conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Dataset<Row> readSource(SparkSession spark) {
		String path = this.getConf("path");
		String sep = this.getConf("separator");
		System.out.println("source获取path:"+path);
		
		Dataset<Row > data = spark.read().option("sep", sep).option("header","true").csv(path);
		return data;
	}

}
