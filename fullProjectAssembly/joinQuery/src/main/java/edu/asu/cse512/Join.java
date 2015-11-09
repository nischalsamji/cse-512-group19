package edu.asu.cse512;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;

import scala.Array;
import scala.Tuple2;

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
    	String input1 = "/home/system/Desktop/TestData/JoinQueryInput3.csv";  //args[0];
    	String input2 = "/home/system/Desktop/TestData/JoinQueryInput2.csv";  //args[1];
    	SparkConf conf = new SparkConf().setMaster("local").setAppName("Grp19-JoinQuery");
    	final JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	JavaRDD<String> input1RDD = sc.textFile(input1);
    	JavaRDD<String> input2RDD = sc.textFile(input2);
    	   	
     	String inputType = "point";//args[3];
    	
    	if(inputType.equals("point")){
    		
    		//new Function(inputType, returnType)
        	final JavaRDD<Id_PolygonPoints> input2Polygons = input2RDD.map(new Function<String,Id_PolygonPoints>(){
    			public Id_PolygonPoints call(String coOrdinates){ Id_PolygonPoints idp = getIDPoints(coOrdinates); 
    			return idp;}
        	});
    		
    		final JavaRDD<Id_Point> input1Points= input1RDD.map(new Function<String,Id_Point>(){
    		 public Id_Point call(String oneCoOrdinates){Id_Point ip= getPoints(oneCoOrdinates);
    		 return ip;}
    		});
    		System.out.println(input1Points.count());
    		System.out.println(input2Polygons.count());
    		System.out.println(input1Points.first().id);
    	
 
    		
    		

    		final Broadcast<List<Id_Point>> input1Broadcast = sc.broadcast(input1Points.collect());
   		     
		    	    		 				
    		JavaRDD<String> output = input2Polygons.map(new Function<Id_PolygonPoints,String>(){
    			public String call(final Id_PolygonPoints onePolygon){    				    				
    				final List<Id_Point> inputBPoints= input1Broadcast.getValue();    				      			
    				String result="";
    			     		 
    			     Double x_1 = onePolygon.x_1;
    			     Double y_1 = onePolygon.y_1;
    			     Double x_2 = onePolygon.x_2;
    			     Double y_2 = onePolygon.y_2;
    			     for(int i=0; i<inputBPoints.size(); i++){
    			    	Double x1 = inputBPoints.get(i).x1;
    			    	Double y1 = inputBPoints.get(i).y1;
    			    	 //if((x1-x_1) * (x1-x_2)<=0 && (y1-y_1) * (y1-y_2)<=0)	    	 
    			    	 if((x1-x_1) * (x1-x_2)<=0 && (y1-y_1) * (y1-y_2)<=0){
    			    		 if(result.equals(""))
    			    		 result = result + inputBPoints.get(i).id;
    			    		 else
    			    			 result=result +","+inputBPoints.get(i).id;
    			    	 }
    			     }
    				 if(result.equals(""))
    					 result = "NULL";
    				 return onePolygon.id+","+result;    				
    			}
    		}); 
    		
       		
       		//need to remove existing in output file location.
       		
       		output.saveAsTextFile("/home/system/Desktop/joinOut.txt"); //args[2]
    	}		
    	else if(inputType.equals("rectangle")){
    	
    		//new Function(inputType, returnType)
        	final JavaRDD<Id_Polygon> input2Polygons = input2RDD.map(new Function<String,Id_Polygon>(){
    			public Id_Polygon call(String coOrdinates){ Id_Polygon idp = getGeometry(coOrdinates); 
    			return idp;}
        	});
        	
    		JavaRDD<Id_Polygon> input1Polygons = input1RDD.map(new Function<String,Id_Polygon>(){
				public Id_Polygon call(String coOrdinates){  Id_Polygon idp = getGeometry(coOrdinates); 
				return idp; }
        	});
    		
    		final Broadcast<List<Id_Polygon>> input1Broadcast = sc.broadcast(input1Polygons.collect());
   		     
		    	    		 				
    		JavaRDD<String> output = input2Polygons.map(new Function<Id_Polygon,String>(){
    			public String call(final Id_Polygon onePolygon){    				    				
    				final List<Id_Polygon> inputBPolygons= input1Broadcast.getValue();    				      			
    				String result="";
    			     		 
    			     
    			     for(int i=0; i<inputBPolygons.size(); i++){
    			    	 if(inputBPolygons.get(i).g.intersects(onePolygon.g)){ //contains
    			    		 if(result.equals(""))
    			    		 result = result + inputBPolygons.get(i).id;
    			    		 else
    			    			 result=result +","+inputBPolygons.get(i).id;
    			    	 }
    			     }
    				 if(result.equals(""))
    					 result = "NULL";
    				 return onePolygon.id+","+result;    				
    			}
    		}); 
    		
       		
       		//need to remove existing in output file location.
       		
      
       		output.saveAsTextFile("/home/system/Desktop/joinOut.txt");//args[2]
       		
    		
    	}
    
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
    
    public static Id_Point getPoints(String oneCoOrdinate){
    	String coOrdinates[] = oneCoOrdinate.split(",");
    	Double x1 = Double.parseDouble(coOrdinates[1]);
    	Double y1 = Double.parseDouble(coOrdinates[2]);
    	Id_Point ip = new Id_Point(coOrdinates[0],x1,y1);
    	return ip;
    }
    
    public static Id_PolygonPoints getIDPoints(String twoCoOrdinates){
    	String coOrdinates[] = twoCoOrdinates.split(",");
    	Double x_1= Double.parseDouble(coOrdinates[1]);
    	Double y_1= Double.parseDouble(coOrdinates[2]);
    	Double x_2 = Double.parseDouble(coOrdinates[3]);
    	Double y_2 = Double.parseDouble(coOrdinates[4]);
    	Id_PolygonPoints pp = new Id_PolygonPoints(coOrdinates[0],x_1, y_1, x_2, y_2);
    	return pp;
    	
    }

}

