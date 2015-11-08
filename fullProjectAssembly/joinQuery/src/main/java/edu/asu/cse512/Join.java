package edu.asu.cse512;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class Join implements Serializable
{
	
	private static final long serialVersionUID = 1L;



	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation 1
	 * @param inputLocation 2
	 * @param outputLocation
	 * @param inputType 
	 * 
	*/
    @SuppressWarnings("serial")
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
    	JavaRDD<Id_Polygon> input2Polygons = input2RDD.map(new Function<String,Id_Polygon>(){
		

			public Id_Polygon call(String coOrdinates){  Id_Polygon idp = getGeometry(coOrdinates); 
    		return idp;}
    	});
    		
    	System.out.println(input2Polygons.count());
    	System.out.println(input2Polygons.first().id);
    	System.out.println(input2Polygons.first().g.getCoordinate());
    	String inputType = "rectangle";//args[3];
    	
    	if(inputType.equals("point")){
    		//if((x1-x_1) * (x1-x_2)<=0 && (y1-y_1) * (y1-y_2)<=0)
    	}
    	else if(inputType.equals("rectangle")){
    		JavaRDD<String> output;
    		JavaRDD<Id_Polygon> input1Polygons = input1RDD.map(new Function<String,Id_Polygon>(){
    			
		
				public Id_Polygon call(String coOrdinates){  Id_Polygon idp = getGeometry(coOrdinates); 
	    		return idp;}
        	});
    		
    		final Broadcast<JavaRDD<Id_Polygon>> input1Broadcast = sc.broadcast(input1Polygons);
    		output = input2Polygons.map(new Function<Id_Polygon,String>(){
    			public String call(final Id_Polygon onePolygon){
    				String eachResult =""; //glom    				
    				 JavaRDD<Id_Polygon> inputPolygons= input1Broadcast.getValue();
    				 long tuplesInInput1 = inputPolygons.count(); //have to check later     				 
    				 
    				 
    				 JavaRDD<Id_Polygon> filteredInput = inputPolygons.filter(new Function<Id_Polygon,Boolean>(){
    					 public Boolean call(Id_Polygon input1Polygon){
    						 if(input1Polygon.g.intersects(onePolygon.g)){ //contains
    							 return true;
    						 }
    						 else
    							 return false;    								
    					 }
    				 });
    				 
    				 JavaRDD<String> filteredMapInput = inputPolygons.map(new Function<Id_Polygon,String>(){
    					 public String call(Id_Polygon input1Polygon){
    						 		return input1Polygon.id;
    					 }
    				 });
    				 
    				 String result= filteredMapInput.reduce(new Function2<String,String,String>(){
    					 public String call(String a, String b){
    						 return a+","+b;
    					 }
    				 });
    				
    				 eachResult = eachResult+onePolygon.id+result;
    				 return eachResult;    				
    			}
    		});
    		
    		System.out.println(output.count());
    		output.saveAsTextFile("/home/system/Desktop/joinOut.txt");
    		
    		
    		
    	}
    
    	
    	
    	//Output your result, you need to sort your result!!!
    	//And,Don't add a additional clean up step delete the new generated file...
    	sc.close();
    }
    
	 // List<Id_Polygon> input1List = inputPolygons.collect(); //take() method is only returning integer number of tuples but here we need to collect Double number of records    
    
    public static Id_Polygon getGeometry(String twocoOrdinates){
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
		Id_Polygon newRecord = new Id_Polygon(coOrdinates[0], g);
		
		return newRecord;

    }

}

