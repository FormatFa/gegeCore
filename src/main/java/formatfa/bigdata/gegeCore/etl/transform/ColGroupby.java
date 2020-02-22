package formatfa.bigdata.gegeCore.etl.transform;

import java.util.HashMap;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;

import formatfa.bigdata.gegeCore.etl.SparkUtils;
import formatfa.bigdata.gegeCore.etl.TransformComponent;
import static org.apache.spark.sql.functions.*;;
public class ColGroupby extends TransformComponent {

	public ColGroupby(HashMap<String, String> conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Dataset<Row> process(Dataset<Row> row) {
		// TODO Auto-generated method stub
		Column[] cols = SparkUtils.str2col(this.getConf("columns").split(","));
		Column aggColumn = new Column(this.getConf("aggColumn"));
		String operation = this.getConf("operation");
		String alias = this.getConf("alias");
		Dataset<Row> result = null;
		if(operation.equals("avg"))
		{
			aggColumn = avg(aggColumn);
		}else if(operation.equals("sum"))
		{
			aggColumn = sum(aggColumn);
		}
		else if("count".equals("count"))
		{
			aggColumn = count(aggColumn);
		}
		aggColumn =  aggColumn.alias(alias);
		result = row.groupBy(cols).agg(aggColumn);
		return result;
	}

}
