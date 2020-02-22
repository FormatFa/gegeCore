package formatfa.bigdata.gegeCore.etl;

import java.util.HashMap;

public abstract class Component {

//	原始数据
	private String data;

//	组件名字
	private String name;
//	组件id
	private String compid;
//	组件的配置
	private HashMap<String, String> conf;

//	默认使用data来构造组件，由组件解析成hashmap

	public String getConf(String key) {
		if(conf==null)
		{
			return null;
		}
		if(conf.containsKey(key)) {
			return conf.get(key);
		}
		return null;

	}


	public Component(HashMap<String, String> conf) {
		super();
		this.conf = conf;
	}

}
