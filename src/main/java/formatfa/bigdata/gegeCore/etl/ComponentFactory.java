package formatfa.bigdata.gegeCore.etl;

import java.lang.reflect.Constructor;
import java.util.HashMap;

import formatfa.bigdata.gegeCore.etl.sink.SinkCsv;
import formatfa.bigdata.gegeCore.etl.source.SourceCsv;
import formatfa.bigdata.gegeCore.etl.transform.ColGroupby;
import formatfa.bigdata.gegeCore.etl.transform.ColSelect;

public class ComponentFactory {
	
	
	public static final HashMap<String,Class> idMap = new HashMap<String,Class>();
	static {
		idMap.put("sink-csv",SinkCsv.class);
		idMap.put("col-select", ColSelect.class);
		idMap.put("source-csv", SourceCsv.class);
		idMap.put("col-groupby", ColGroupby.class);
	}
//	根据id 返回不同的组件实例
//	抽象工厂模式
		public static Component getComponent(String id,HashMap<String,String> conf) throws Exception
		{
			if(idMap.containsKey(id)) {
				Class clz = idMap.get(id);
				Constructor con =  clz.getConstructor(HashMap.class);
				return (Component) con.newInstance(conf);
			}
			else
				throw new Exception("id错误:"+id);
			
		}
		
	
}
