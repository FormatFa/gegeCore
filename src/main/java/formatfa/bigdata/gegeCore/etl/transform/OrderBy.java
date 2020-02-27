package formatfa.bigdata.gegeCore.etl.transform;

import java.util.HashMap;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import formatfa.bigdata.gegeCore.etl.TransformComponent;

public class OrderBy extends TransformComponent {

	public OrderBy(String compid, HashMap<String, String> conf) {
		super(compid, conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Dataset<Row> process(Dataset<Row> row) {
		// 
		String column = this.getConf("column");
		String order = this.getConf("order");
		if("desc".equals(order))
		{
			return row.orderBy(new Column(column).desc());
		}
		else if("asc".equals(order))
		{
			return row.orderBy(new Column(column).asc());
		}
		else
		return null;
	}

}
