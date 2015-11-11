package edu.asu.cse512;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;

import scala.tools.nsc.util.ClassPath.JavaContext;;

public class Geounion {
	private double x1;
	private double x2;
	private double y1;
	private double y2;
	Geometry g = null;
	int i=0;
	final HashMap<Integer,Geometry> mappy=new HashMap();
	/* refered from Learning Spark-Lightening-Fast-Big-Data-Analysis */
	SparkConf conf = new SparkConf().setMaster("local").setAppName("Grp19GeoMetricUnion");
	JavaSparkContext sc = new JavaSparkContext(conf);
	public void readip(){
	BufferedReader bufferedreader = null;
	//using spark 
	
	Polygon Rect = new Polygon();
	
	try {

		String currentLine;
		bufferedreader = new BufferedReader(new FileReader("C:\\Users\\bhara\\Desktop\\geoUnion.txt"));

		while ((currentLine = bufferedreader.readLine()) != null) {
		System.out.println(currentLine);
		List<String> coordinate = Arrays.asList(currentLine.split("\t"));
			x1=Double.parseDouble(coordinate.get(0));
			y1=Double.parseDouble(coordinate.get(1));
			x2=Double.parseDouble(coordinate.get(2));
			y2 =Double.parseDouble(coordinate.get(3));
			List<Double> x3y3=Rect.CalcultePolygon(x1,y1,x2,y2);
			WKTReader reader=new WKTReader();
			
		// wkt reader example referenced from http://openlayers.org/en/master/examples/wkt.html
			
	  /*reader.read("POLYGON coordinate.get(0) coordinate.get(1), coordinate.get(2)coordinate.get(3) ,String.valueOf(x3y3.get(0)) String.valueOf(x3y3.get(0)),"+
			  "String.valueOf(x3y3.get(0)) String.valueOf(x3y3.get(0)");*/
			
			
	
	try {
		
		g = reader.read(String.format("POLYGON ((%f %f,%f %f,%f %f,%f %f,%f %f))",x1,y1,x3y3.get(0),x3y3.get(1),
				x2,y2,x3y3.get(2),x3y3.get(3),x1,y1));
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
			
			mappy.put(i, g);
			System.out.println("id"+i+"value"+mappy.get(i));
			i++;
			
		
			
		}
	
		
	} catch (IOException e) {
		e.printStackTrace();
		
		
	}
	
	}
	public void PerformUnion(){
	Geometry fianlg;
	fianlg=CascadedPolygonUnion.union(mappy.values());
		Coordinate c[]=fianlg.getCoordinates();
		for(int j=0;j<c.length;j++)
		System.out.println("final"+c[j]);
	}
	
	
	
}