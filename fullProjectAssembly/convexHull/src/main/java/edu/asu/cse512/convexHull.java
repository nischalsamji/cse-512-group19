package edu.asu.cse512;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Coordinate;

class Point
{
	private double x;
	private double y;
	public double getX() {
		return x;
	}
	public void setX(double x) {
		this.x = x;
	}
	public double getY() {
		return y;
	}
	public void setY(double y) {
		this.y = y;
	}
	
	}
class calculateConvexHull implements FlatMapFunction<Iterator<String>, Coordinate>, Serializable
{
	private static final long serialVersionUID = 1L;

	public Iterable<Coordinate> call(Iterator<String> pointText)
	{
		
		
		List<Coordinate> initPoints = new ArrayList<Coordinate>();
		try{
			
			while(pointText.hasNext())
			{
				String eachPoint = pointText.next();
				String[] CoordList = eachPoint.split(",");
				Double x1 = Double.parseDouble(CoordList[0]);
				Double y1 = Double.parseDouble(CoordList[1]);
				Coordinate coord = new Coordinate(x1,y1);
				initPoints.add(coord);
			}}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		Coordinate[] ptsArray = initPoints.toArray(new Coordinate[initPoints.size()]);
		
		ConvexHull ch = new ConvexHull(ptsArray,  new GeometryFactory());
		Geometry convHullGeo=ch.getConvexHull();
		Coordinate[] coordArray= convHullGeo.getCoordinates();
		return Arrays.asList(coordArray);
	}
}


public class convexHull
{
	public static void main(String[] args) throws ClassNotFoundException
	{
		SparkConf sparkConfig = new SparkConf().setAppName("Group19FarthestPair");
		JavaSparkContext sc = new JavaSparkContext(sparkConfig);
		JavaRDD<String> pointStrings = sc.textFile(args[0]);
		JavaRDD<Coordinate> localHull = pointStrings.mapPartitions(new calculateConvexHull());		
		JavaRDD<Coordinate> localHullList = localHull.repartition(1);
		List<Coordinate> globalHull =  localHullList.collect();
		List<String> cList = new ArrayList<String>();
		for (Coordinate c : globalHull) {
			StringBuffer sb = new StringBuffer();
			sb.append(c.x);
			sb.append(",");
			sb.append(c.y);
			sb.append("\n");
			cList.add(sb.toString());
		}
		JavaRDD<String> globalCoords = sc.parallelize(cList);
		JavaRDD<Coordinate> FinalList = globalCoords.mapPartitions(new calculateConvexHull());
		List<Coordinate> gConvexHull = FinalList.collect();
		List<String> cgList = new ArrayList<String>();
		for (Coordinate c : gConvexHull) {
			StringBuffer sb = new StringBuffer();
			sb.append(c.x);
			sb.append(",");
			sb.append(c.y);
			sb.append("\n");
			cgList.add(sb.toString());
		}
		JavaRDD<String> globalConvexHull = sc.parallelize(cgList);
		JavaRDD<Coordinate> GlobalFinalList = globalConvexHull.mapPartitions(new calculateConvexHull());
		List<Coordinate> accConvexHull = GlobalFinalList.collect();
		Set<Coordinate> po=new TreeSet<Coordinate>();
		
		for(Coordinate g:accConvexHull){
		po.add(g);
		}
	List<String> finalOutput = new ArrayList();
	for(Coordinate g:po){
		finalOutput.add(Double.toString(g.x)+","+Double.toString(g.y));
		}
	JavaRDD<String> finalFile = sc.parallelize(finalOutput);
	finalFile.coalesce(1).saveAsTextFile(args[1]);
	}
}