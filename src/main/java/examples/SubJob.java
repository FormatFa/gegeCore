package examples;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

public class SubJob {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			try {
				LivyClient client = new LivyClientBuilder().setURI(new URI("http://192.168.4.101:8998")).build();
				Future result =  client.uploadJar(new File("C:\\Users\\root\\Documents\\stsWorkSpace\\gegeCore\\target\\a.jar"));
				System.out.println( result.get() );
				double pi = client.submit(new PiJob(100)).get();
				
				System.out.println("Pi is roughly: "+pi);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

}
