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

class Point{
	private Double x;
	private Double y;
	public Double getX() {
		return x;
	}
	public void setX(Double x) {
		this.x = x;
	}
	public Double getY() {
		return y;
	}
	public void setY(Double y) {
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


public class farthestPair
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
		Double maxDistance = 0.0;
		List<Point> finalPoints= new ArrayList<Point>();
		Point p1 = new Point();
		Point p2 = new Point();
		p1.setX(0.0);
		p1.setY(0.0);
		p2.setX(0.0);
		p2.setY(0.0);
		finalPoints.add(p1);
		finalPoints.add(p2);
		for(int i=0; i<gConvexHull.size(); i++){
			for(int j = i+1; j<gConvexHull.size();j++){
				Double x1 = gConvexHull.get(i).x;
				Double y1 = gConvexHull.get(i).y;
				Double x2 = gConvexHull.get(j).x;
				Double y2 = gConvexHull.get(j).y;
				double distance = ((x2 - x1)*(x2 - x1)) + ((y2 - y1))*((y2 - y1));
				   if(distance > maxDistance){
				   	maxDistance = distance;
				   	finalPoints.get(0).setX(x1);
				   	finalPoints.get(0).setY(y1);
				   	finalPoints.get(1).setX(x2);
				   	finalPoints.get(1).setY(y2);
				   }
			}
		}
	
	List<String> finalString = new ArrayList<String>();	
	
		String x1 = finalPoints.get(0).getX().toString();
		String y1 = finalPoints.get(0).getY().toString();
		String x2 = finalPoints.get(1).getX().toString();
		String y2 = finalPoints.get(1).getY().toString();
		String comma = ",";
		finalString.add(x1+comma+y1);
		finalString.add(x2+comma+y2);

	JavaRDD<String> finalFile = sc.parallelize(finalString);
	finalFile.saveAsTextFile(args[1]);
	}
}