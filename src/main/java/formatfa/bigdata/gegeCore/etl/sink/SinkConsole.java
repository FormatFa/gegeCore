package formatfa.bigdata.gegeCore.etl.sink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import formatfa.bigdata.gegeCore.etl.SinkComponent;
import scala.Function1;
import scala.runtime.BoxedUnit;

public class SinkConsole extends SinkComponent {

	

	public SinkConsole(String compid, HashMap<String, String> conf) {
		super(compid, conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean writeSink(Dataset<Row> item) {
		// TODO Auto-generated method stub
		int limit = Integer.parseInt(getConf("limit"));
		System.out.println("-----控制台输出数据-----");
		Dataset<Row> temp = item.limit(limit);
		StructType types =  temp.schema();
//		获取所有字段名字
		String [] columns = types.fieldNames();
		System.out.println(Arrays.toString(columns));
		for(Row row: temp.collectAsList())
		{
			System.out.println(row);
		}
//		打印列名
		return true;
	}

}
