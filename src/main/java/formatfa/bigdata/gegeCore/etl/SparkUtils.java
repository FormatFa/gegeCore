package formatfa.bigdata.gegeCore.etl;

import org.apache.spark.sql.Column;

public class SparkUtils {
		public static Column[] str2col(String[] cols)
		{
			Column[] result = new Column[cols.length];
			for(int i =0;i<cols.length;i+=1)
			{
				result[i] = new Column(cols[i]);
			}
			return result;
		}
}
