package edu.asu.cse512;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.util.*;

/**
 * Hello world!
 *
 */
public class RangeQuery 
{
	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation
	 * @param outputLocation
	 * 
	*/
    public static void main( String[] args )
    {
        //Initialize, need to remove existing in output file location.
    	SparkConf conf = new SparkConf().setAppName("My App").setMaster("local");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	//Implement 
    	//String input1 = "/home/ramkumar/Desktop/testing";
    	//String input2 = "/home/ramkumar/Desktop/testing2";
    	
    	
        String input1 = args[0];
    	//String input1 = "/home/ramkumar/Downloads/Test Case/RangeQueryTestData.csv";
        String input2 = args[1];
    	//String input2 = "/home/ramkumar/Downloads/Test Case/RangeQueryRectangle.csv";
        //"/home/ramkumar/Downloads/Test Case/RangeQueryRectangle.csv"
    	//System.out.	println("Hello");
    	JavaRDD<String> inputfile1 = sc.textFile(input1);
		JavaRDD<String> windowfile2 = sc.textFile(input2);
		
		JavaRDD<List<String>> inputmapper = inputfile1.map(new Function<String, List<String>>(){
			public List<String> call (String line)
			{
				List<String> obj =  new LinkedList<String>(Arrays.asList(line.split(",")));
				//obj.remove(0);
				return obj;
			}
		});
		
		JavaRDD<List<String>> windowmapper = windowfile2.map(new Function<String, List<String>>(){
			public List<String> call (String line)
			{
				List<String> obj =  new LinkedList<String>(Arrays.asList(line.split(",")));
				return obj;
			}
		});
		
		
		final Broadcast<List<String>> windowlist = sc.broadcast(windowmapper.first());
		//Double x_1 = Double.parseDouble((windowlist).getValue().get(0));
		//Double y_1 = Double.parseDouble((windowlist).getValue().get(1));
		//System.out.println(x_1);
		//System.out.println(y_1);
		JavaRDD<List<String>> result = inputmapper
				.filter(new Function<List<String>, Boolean>() {
					public Boolean call(List<String> inputList) {
					
					Double x1 = Double.parseDouble(inputList.get(1));
					Double y1 = Double.parseDouble(inputList.get(2));
					//Double x2 = Double.parseDouble(inputList.get(3));	
					//Double y2 = Double.parseDouble(inputList.get(4));
					Double x_1 = Double.parseDouble((windowlist).getValue().get(0));
					Double y_1 = Double.parseDouble((windowlist).getValue().get(1));
					Double x_2 = Double.parseDouble((windowlist).getValue().get(2));
					Double y_2 = Double.parseDouble((windowlist).getValue().get(3));
					
					
					if((x1-x_1) * (x1-x_2)<=0 && (y1-y_1) * (y1-y_2)<=0)
					{
						return true;
					}
					else return false;
					}
				});
		
JavaRDD<String> result2 = result.map(new Function<List<String>,String>(){
	public String call(List<String> input){
		
		String out = input.get(0);
		return out;
	}
	
	
});


		//System.out.println("King  " + result2.count() + "First " + result2.take(2));
		List<String> finalList = result2.take((int) result2.count());
		
		/*for(int i=0;i<finalList.size();i++)
		{
			finalList.get(i).remove(0);
		}*/
		String output = "/home/ramkumar/Desktop/myout";
		//System.out.println(finalList.get(0));
		result2.coalesce(1).saveAsTextFile(args[2]);
		//result.coalesce(1).saveAsTextFile(output);
		
		/*for (String line: file1.take(3)) {
			  System.out.println(line);
		}*/
		//System.out.println("COunt" + file1.count());
    	//Output your result, you need to sort your result!!!
    	//And,Don't add a additional clean up step delete the new generated file...
    }
}
