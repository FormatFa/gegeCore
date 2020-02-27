package formatfa.bigdata.gegeCore.etl.transform;

import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import formatfa.bigdata.gegeCore.etl.TransformComponent;

public class RowLimit extends TransformComponent {



	public RowLimit(String compid, HashMap<String, String> conf) {
		super(compid, conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Dataset<Row> process(Dataset<Row> row) {
		// TODO Auto-generated method stub
		int num = Integer.parseInt( this.getConf("num"));
		return row.limit(num);
		
	}

}
