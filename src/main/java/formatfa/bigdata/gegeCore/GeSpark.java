package formatfa.bigdata.gegeCore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import cn.hutool.core.io.resource.ClassPathResource;
import cn.hutool.core.io.resource.NoResourceException;
import formatfa.bigdata.gegeCore.etl.Component;
import formatfa.bigdata.gegeCore.etl.ComponentFactory;
import formatfa.bigdata.gegeCore.etl.SinkComponent;
import formatfa.bigdata.gegeCore.etl.SourceComponent;
import formatfa.bigdata.gegeCore.etl.TransformComponent;

public class GeSpark {
	/*
	 * 解析json,执行ETL操作
	 */
	
	
//	spark session
	private SparkSession session;
	private List<Component> components ;
	
public GeSpark(SparkSession session, String data) {
		super();
		this.session = session;
		this.data = data;
	}

	//	json数据
	private String data;
	
	
	
	public GeSpark( String data) {
		super();
		this.data = data;
	}

//	加载组件
	public void loadComponents() throws Exception
	{
		
		components = new ArrayList<Component>();
		JSONArray array  =  JSONObject.parseArray(data);
		for(int i =0;i<array.size();i+=1)
		{
//			System.out.println(array.get(i));
			HashMap<String,String> conf = new HashMap<String,String>();
			JSONObject jsonConf = array.getJSONObject(i).getJSONObject("conf");
			for(String key: jsonConf.keySet())
			{
				String value = jsonConf.getString(key);
				conf.put(key, value);
			}
			Component c =  ComponentFactory.getComponent(array.getJSONObject(i).getString("compid"), conf);
			components.add(c);
		}
		System.out.println("加载处理组件完成,组件个数:"+this.components.size());
	}
	
//	处理
	public void process() throws Exception
	{
		this.loadComponents();
//		判断组件是否符合
		if(this.components.size()<2)
		{
			throw new Exception("组件至少要3个");
		}
		
		Dataset<Row> inputdata =null;
		for(int i=0;i<this.components.size();i+=1)
		{
			Component now = this.components.get(i);
			System.out.println("***处理组件:"+this.components.get(i).getCompid());
			
			if( now instanceof SourceComponent)
			{
				SourceComponent source = (SourceComponent)this.components.get(i);
				inputdata = source.readSource(session);
			}
			else if( now  instanceof TransformComponent) {
				if(inputdata==null)
				{
					System.out.println("遇到了转换组件，但是输入数据为空!!");
					continue;
				}
				 TransformComponent trans =  (TransformComponent) this.components.get(i);
				 inputdata = trans.process(inputdata);
			}
			else if(now instanceof SinkComponent) {
				if(inputdata==null)
				{
					System.out.println("遇到了保存组件，但是输入数据为空!!");
					continue;
				}
				SinkComponent sink = (SinkComponent) this.components.get(i);
				boolean sinkResult = sink.writeSink(inputdata);
				System.out.println(sink.getCompid()+"sink结果:"+sinkResult);
			}
			
		}
	
		
		
		System.out.println("-----处理完成!-----");
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
	
		
//		Component c =  ComponentFactory.getComponent("sink-csv", null);
//		System.out.println(c);
		
		SparkSession session = SparkSession.builder().appName("test").master("local[2]").getOrCreate();
		session.sparkContext().setLogLevel("WARN");
		
		String json = null;
		if(args.length>0)
		{
			json=args[0];
		}
		else
		{
//			不是从命令行传参数，就使用测试数据
			ClassPathResource resource = new ClassPathResource("comps.json");
			json = IOUtils.toString(resource.getStream());
		}
		
		GeSpark gspark = new GeSpark(session,json);
		gspark.process();
//		System.out.println(json);
//		System.out.println(resource.getStream());
//		JSONArray object = JSONObject.parseArray(json);
//		System.out.println(object);
		
	}
	

}
