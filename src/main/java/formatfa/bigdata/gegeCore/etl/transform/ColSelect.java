package formatfa.bigdata.gegeCore.etl.transform;

import java.util.HashMap;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import formatfa.bigdata.gegeCore.etl.SourceComponent;
import formatfa.bigdata.gegeCore.etl.TransformComponent;

public class ColSelect extends TransformComponent {




	public ColSelect(String compid, HashMap<String, String> conf) {
		super(compid, conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Dataset<Row> process(Dataset<Row> row) {
		// TODO Auto-generated method stub
		String[] columns = getConf("columns").split(",");
		Column [] temp = new Column[columns.length];
		for(int i =0;i<columns.length;i+=1)
		{
			temp[i] = new Column(columns[i]);
		}
		return row.select(temp);
		
	}

}
