package edu.asu.cse512;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.List;
import java.util.ArrayList;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class Join 
{
	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation 1
	 * @param inputLocation 2
	 * @param outputLocation
	 * @param inputType 
	 * 
	*/
    public static void main( String[] args )
    {
        System.out.println("hello");
 
        //Initialize, need to remove existing in output file location.
    	//String outputLocation = args[2];
    	
    	
    	//Implement 
    	String input1 = "/home/system/Desktop/TestData/JoinQueryTestData1.csv";  //args[0];
    	String input2 = "/home/system/Desktop/TestData/JoinQueryTestData2.csv";  //args[1];
    	SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	JavaRDD<String> input1RDD = sc.textFile(input1);
    	JavaRDD<String> input2RDD = sc.textFile(input2);
    	
    	System.out.println(input1RDD.count());
    	System.out.println(input2RDD.count());
    	
    	
    	//new Funtion(inputType, returnType)
    	JavaRDD<Geometry> input2Polygons = input2RDD.map(new Function<String,Geometry>(){
    		public Geometry call(String coOrdinates){  Geometry g = getGeometry(coOrdinates); 
    		return g;}
    	});
    		
    	
    	
    	String inputType = "rectangle";//args[3];
    	
    	if(inputType.equals("point")){
    		
    	}
    	else if(inputType.equals("rectangle")){
    		JavaRDD<Geometry> input1Polygons = input1RDD.map(new Function<String,Geometry>(){
        		public Geometry call(String coOrdinates){  Geometry g = getGeometry(coOrdinates); 
        		return g;}
        	});
    		
    		
    		JavaRDD<List<Double>> output = input2Polygons.Map(new Function<Geometry ,List<Double>>{
    			public List<Double> call(Geometry onePolygon){
    				List<Double> eachLine = new ArrayList<Double>();
    				
    				
    				return eachLine;
    			}
    		});
    		
    	}
    
    	
    	//Output your result, you need to sort your result!!!
    	//And,Don't add a additional clean up step delete the new generated file...
    	
    }
    
    
    
    public static Geometry getGeometry(String twocoOrdinates){
    	String coOrdinates[] = twocoOrdinates.split(","); 
		double x1=Double.parseDouble(coOrdinates[1]);
		double y1=Double.parseDouble(coOrdinates[2]);
		double x2=Double.parseDouble(coOrdinates[3]);
		double y2=Double.parseDouble(coOrdinates[4]);
		Polygon rect = new Polygon();
		List<Double> x3y3x4y4 = rect.CalcultePolygon(x1, y1, x2, y2);
		WKTReader reader = new WKTReader();
		Geometry g = null;
		try{
			g=reader.read(String.format("POLYGON ((%f %f,%f %f,%f %f,%f %f,%f %f))",x1,y1,x3y3x4y4.get(0),x3y3x4y4.get(1),
					x2,y2,x3y3x4y4.get(2),x3y3x4y4.get(3),x1,y1));
			
		}catch(ParseException e){
			e.printStackTrace();    				
		}
		
		return g;

    }

}

