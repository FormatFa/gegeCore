package formatfa.bigdata.gegeCore.etl;

import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract  class SourceComponent extends Component {

	public SourceComponent(HashMap<String, String> conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	Dataset<Row> source;
	/*
	 * spark的抽象方法
	 */

//	读取数据源，子组件提供
	public abstract Dataset<Row> readSource(SparkSession spark);
	
	//获取数据源
	public  Dataset<Row> getSource(SparkSession spark)
	{
		if(source==null)
		{
			source = readSource(spark);
		}
		return source;
	}
	
	

	

}
