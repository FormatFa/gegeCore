package formatfa.bigdata.gegeCore.etl.sink;

import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import formatfa.bigdata.gegeCore.etl.SinkComponent;
import formatfa.bigdata.gegeCore.etl.SourceComponent;

public class SinkCsv extends SinkComponent {



	public SinkCsv(String compid, HashMap<String, String> conf) {
		super(compid, conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean writeSink(Dataset<Row> item) {
		// TODO Auto-generated method stub
		System.out.println("sink写入数据:");
		String path = this.getConf("path");
		String sep = this.getConf("separator");
		System.out.println("source获取path:"+path);
		System.out.println(item);
		 item.write().option("sep", sep).csv(path);
		return true;
		
	}



	
	

}
