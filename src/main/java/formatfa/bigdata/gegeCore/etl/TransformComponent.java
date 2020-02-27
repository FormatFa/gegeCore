package formatfa.bigdata.gegeCore.etl;

import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class TransformComponent extends Component {

	


	public TransformComponent(String compid, HashMap<String, String> conf) {
		super(compid, conf);
		// TODO Auto-generated constructor stub
	}

	//	传入一个Dataset,使用组件处理
	public  abstract  Dataset<Row> process(Dataset<Row> row);

}
