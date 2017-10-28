import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class airPollution {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("filterOnline").setMaster("local[2]");
		JavaStreamingContext jsc = new  JavaStreamingContext(conf,Durations.seconds(5));
		
		JavaReceiverInputDStream<String>  lines = jsc.socketTextStream("localhost", 10080);
		
		JavaDStream<String> items =  lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
	        public Iterator<String> call(String line) throws Exception {
				String[] lists = line.split(",");
				String[] result = new String[6];
				result[0] = lists[0] + "|" + lists[1] + "|" + "PM2.5" + "|" + lists[2];
				result[1] = lists[0] + "|" + lists[1] + "|" + "PM10" + "|" + lists[3];
				result[2] = lists[0] + "|" + lists[1] + "|" + "SO2" + "|" + lists[4];
				result[3] = lists[0] + "|" + lists[1] + "|" + "CO" + "|" + lists[5];
				result[4] = lists[0] + "|" + lists[1] + "|" + "NO2" + "|" + lists[6];
				result[5] = lists[0] + "|" + lists[1] + "|" + "O3" + "|" + lists[7];
	            return Arrays.asList(result).iterator();
	        }
	    });
		
		JavaDStream<String> alert = items.filter(new Function<String,Boolean>(){
	          public Boolean call(String s){
	        	  String[] temp = s.split("[|]");
		        	  switch(temp[2]) {
		        	  	case "PM2.5":
		        	  		return Integer.parseInt(temp[3]) >= 150;
		        	  	case "PM10":
		        	  		return Integer.parseInt(temp[3]) >= 350;
		        	  	case "SO2":
		        	  		return Integer.parseInt(temp[3]) >= 800;
		        	  	case "CO":
		        	  		return Integer.parseInt(temp[3]) >= 60;
		        	  	case "NO2":
		        	  		return Integer.parseInt(temp[3]) >= 1200;
		        	  	case "O3":
		        	  		return Integer.parseInt(temp[3]) >= 400;
		        	  	default:
		        	  		return true;
		        	  } 
	          }
	    });

	    try {
	    		alert.print();
	    		alert.dstream().repartition(1).saveAsTextFiles("stream/output/log","txt");
	    		
		    jsc.start();
			jsc.awaitTermination();
			jsc.close();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
