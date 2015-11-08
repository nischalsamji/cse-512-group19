package edu.asu.cse512;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

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
			private static final long serialVersionUID = 1L;

			public Id_Polygon call(String coOrdinates){  Id_Polygon idp = getGeometry(coOrdinates); 
    		return idp;}
    	});
    		
    	
    	
    	String inputType = "rectangle";//args[3];
    	
    	if(inputType.equals("point")){
    		
    	}
    	else if(inputType.equals("rectangle")){
    		JavaRDD<List<String>> output;
    		JavaRDD<Id_Polygon> input1Polygons = input1RDD.map(new Function<String,Id_Polygon>(){
    			
				private static final long serialVersionUID = 1L;
				public Id_Polygon call(String coOrdinates){  Id_Polygon idp = getGeometry(coOrdinates); 
	    		return idp;}
        	});
    		
    		final Broadcast<JavaRDD<Id_Polygon>> input1Broadcast = sc.broadcast(input1Polygons);
    		output = input2Polygons.map(new Function<Id_Polygon,List<String>>(){
    			public List<String> call(Id_Polygon onePolygon){
    				List<String> eachResult = new ArrayList<String>(); //glom
    				
    				 JavaRDD<Id_Polygon> inputPolygons= input1Broadcast.getValue();
    				 int tuplesInInput1 = (int)inputPolygons.count(); //have to check later 
    					List<Id_Polygon> input1List = inputPolygons.collect(); //take() method is only returning integer number of tuples but here we need to collect Double number of records
    					int count=0;
    					for(int i=0; i< tuplesInInput1; i++){
    						if(input1List.get(i).g.intersects(onePolygon.g))
    						{
    							if(count == 0)
    								eachResult.add(onePolygon.id);
    							eachResult.add(input1List.get(i).id);
    						}
    					}
    				return eachResult;    				
    			}
    		});
    		
    		System.out.println(output.first());
    		
    		
    		
    	}
    
    	
    	
    	//Output your result, you need to sort your result!!!
    	//And,Don't add a additional clean up step delete the new generated file...
    	sc.close();
    }
    
    
    
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

