package com.geospark.Geospatialoperations;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;

import scala.Tuple2;
import scala.reflect.reify.phases.Calculate;
public class GeoUnionusingSpark implements java.io.Serializable{
	
	@SuppressWarnings("unused")
	public static void main(String[] args){
		
		Geometry g = null;
		int i=0;
		final HashMap<Integer,Geometry> mappy=new HashMap();
		/* referred from Learning Spark-Lightening-Fast-Big-Data-Analysis */
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Grp19GeoMetricUnion");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//BufferedReader bufferedreader = null;
		//Polygon Rect=new Polygon();
		//using spark 
		JavaRDD<String> polygonip = sc.textFile("C:\\Users\\bhara\\Desktop\\geoUnion.txt");
		System.out.println("string"+polygonip.count());
		 JavaRDD<Geometry> input1Polygons = polygonip.map(new Function<String,Geometry>(){
			private static final long serialVersionUID = 1L;
			public Geometry call(String coOrdinates){  Geometry idp = calculatePoly(coOrdinates); 
			System.out.print("here");
    		return idp;}
    	});
		 
	/*Geometry finalgeo= CascadedPolygonUnion.union(input1Polygons.take((int) input1Polygons.count()));
		WKTWriter writer=new WKTWriter();*/
		 
		 List<Geometry> geo=input1Polygons.take((int) input1Polygons.count());
		 
		 Geometry finalop=input1Polygons.reduce(new Function2<Geometry,Geometry,Geometry>(){
			private static final long serialVersionUID = 8741785958217170398L;

			public Geometry call(Geometry a, Geometry b) throws Exception {
				return a.union(b);
				
			}});
		 
		 ArrayList<String> op=done(finalop);
		 JavaRDD<String> out=sc.parallelize(op);
		 out.saveAsTextFile("C:\\Users\\bhara\\Desktop\\geoUnion1.csv");

	}
	public static ArrayList<String> done(Geometry g){
		 Coordinate[] getcoord = g.getCoordinates();
			ArrayList<String> array = new ArrayList<String>();
			for (int i = 0; i < getcoord.length - 1; i++) {
				Coordinate coo = getcoord[i];
				array.add(coo.x + "," + coo.y);
				System.out.println(coo.x+","+coo.y);
			}
			return array;
		}
	public  static Geometry calculatePoly(String lines){
			String coordinate[]= lines.split(",");
			double x1=Double.parseDouble(coordinate[0]);
			double y1=Double.parseDouble(coordinate[1]);
			double x2=Double.parseDouble(coordinate[2]);
			double y2 =Double.parseDouble(coordinate[3]);
			Polygon p=new Polygon();
			p.CalcultePolygon(x1, y1, x2, y2);
			WKTReader reader=new WKTReader();
			Geometry g = null;
			try {
				
				 g = reader.read(String.format("POLYGON ((%f %f,%f %f,%f %f,%f %f,%f %f))",x1,y1,p.getX3(),p.getY3(),
						x2,y2,p.getX4(),p.getY4(),x1,y1));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				return g;}
	}