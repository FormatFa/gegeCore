package formatfa.bigdata.gegeCore.etl;

import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class SinkComponent extends Component {

	/*
	 * sink 应该 返回什么呢
	 */
	


	public SinkComponent(String compid, HashMap<String, String> conf) {
		super(compid, conf);
		// TODO Auto-generated constructor stub
	}

	public   abstract  boolean writeSink(Dataset<Row> item);
}
