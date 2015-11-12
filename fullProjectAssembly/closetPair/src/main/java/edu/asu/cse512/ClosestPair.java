package edu.asu.cse512;

import java.awt.Point;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Hello world!
 * 
 */

public class ClosestPair {
	/*
	 * Main function, take two parameter as input, output
	 * 
	 * @param inputLocation
	 * 
	 * @param outputLocation
	 */
	
	public static MultiMap multiMap = new MultiValueMap();
	public static class Point {
		public double x, y;

		public Point(double x, double y) {
			this.x = x;
			this.y = y;
		}

		public int compareTo(Point other) {
			if (this.y == other.y) {
				return (this.x < other.x) ? -1 : ((this.x == other.x) ? 0 : 1);
			} else {
				return (this.y < other.y) ? -1 : 1;
			}
		}

	}

	public static class Pair {
		public Point point1 = null;
		public Point point2 = null;
		public double distance = 0.0;

		public Pair(Point point1, Point point2) {
			this.point1 = point1;
			this.point2 = point2;
			calcDistance();
		}

		public void update(Point point1, Point point2, double distance) {
			this.point1 = point1;
			this.point2 = point2;
			this.distance = distance;
		}

		public void calcDistance() {
			this.distance = distance(point1, point2);
		}

		public String toString() {
			return point1 + "-" + point2 + " : " + distance;
		}

		public static double distance(Point p1, Point p2) {

			return Math.sqrt(Math.pow(p2.x - p1.x, 2)
					+ Math.pow(p2.y - p1.y, 2));
		}

		public static Pair bruteForce(List<? extends Point> points) {
			int numPoints = points.size();
			if (numPoints < 2)
				return null;
			Pair pair = new Pair(points.get(0), points.get(1));
			if (numPoints > 2) {
				for (int i = 0; i < numPoints - 1; i++) {
					Point point1 = points.get(i);
					for (int j = i + 1; j < numPoints; j++) {
						Point point2 = points.get(j);
						double distance = distance(point1, point2);
						if (distance < pair.distance){
							pair.update(point1, point2, distance);
							multiMap.put(pair.distance,pair);
						}
					}
				}
			}
			return pair;
		}

		private static List<Point> Ysort(List<? extends Point> points) {
			int size_list = points.size();

			for (int i = 0; i < size_list; i++) {
				Point a = points.get(i);
				if (size_list - 1 != 0) {
					for (int j = i + 1; j < size_list; j++) {
						Point b = points.get(j);
						a.compareTo(b);

					}
				}
			}

			return (List<Point>) points;

		}

		private static Pair divideAndConquer(
				List<? extends Point> pointsSortedByX,
				List<? extends Point> pointsSortedByY) {
			Pair closestPair;
			int numPoints = pointsSortedByX.size();
			if (numPoints <= 3)
				return bruteForce(pointsSortedByX);

			int Index_divide = numPoints / 2;

			List<? extends Point> left_Data = pointsSortedByX.subList(0,
					Index_divide);
			
		
			
			List<? extends Point> right_Data = pointsSortedByX.subList(
					Index_divide, numPoints);
			List<Point> tempList = new ArrayList<Point>(left_Data);
			Ysort(tempList);
			Pair close_right = divideAndConquer(left_Data, tempList);
			tempList.clear();
			
			tempList = new ArrayList<Point>(right_Data);
			Ysort(tempList);
			Pair close_left = divideAndConquer(right_Data, tempList);
			tempList.clear();
			closestPair = (close_right.distance < close_left.distance) ? close_right
					: close_left;
			multiMap.put(closestPair.distance,closestPair);
			double strip = closestPair.distance;
			
			List<Point> strip_Points = new ArrayList<Point>();
			Point center = right_Data.get(0);
			for (Point p : pointsSortedByX) {
				if ((Math.abs(p.x - center.x) < strip)
						|| (Math.abs(center.x - p.x) < strip))
					strip_Points.add(p);

			}
			for (int i = 0; i < strip_Points.size() - 1; i++) {
				Point p = strip_Points.get(i);
				for (int j = i + 1; j < strip_Points.size(); j++) {
					Point q = strip_Points.get(j);
					double min_dist = distance(p, q);
					if (min_dist < strip) {
						strip = min_dist;
						closestPair.update(p, q, strip);
						multiMap.put(strip,closestPair);
					}

				}

			}

			return closestPair;

		}

	}
	
	

	public static void main(String[] args) {
		
		String inputFile = args[0];
		//String inputFile = "/home/kranthipc/Downloads/Test Case/ClosestPairTestData.csv";
		// Initializing Spark
		SparkConf conf = new SparkConf()
				.setAppName(
				"closestPair");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(inputFile);
		final List<Point> pointsSortedByX = new ArrayList<Point>();
		final List<Point> pointsSortedByY = new ArrayList<Point>();
		
		// Reading Input Data
		JavaRDD<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String s) {
						return Arrays.asList(s.split(" "));
					}
				});
		// JavaRDD<String> input = sc.textFile(inputFile);
		/*
		 * JavaPairRDD<String, Integer> pairs = words.mapToPair(new
		 * PairFunction<String, String, Integer>() { public Tuple2<String,
		 * Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
		 * });
		 */
		System.out.println(words.first());
		// Sorting by X Coordinate
		JavaPairRDD<Double, Double> pairsX = words
				.mapToPair(new PairFunction<String, Double, Double>() {
					public Tuple2<Double, Double> call(String s) {

						String[] parts = s.split(",");
						Double key = Double.parseDouble(parts[0]);
						Double value = Double.parseDouble(parts[1]);

						return new Tuple2<Double, Double>(key, value);
					}
				});

		JavaPairRDD<Double, Double> sortedByX = pairsX.sortByKey();
		List<Tuple2<Double, Double>> obj = sortedByX.take((int) sortedByX
				.count());

		int count = obj.size();
		for (int i = 0; i < count; i++) {
			Double pointX = obj.get(i)._1;
			Double pointY = obj.get(i)._2;
			pointsSortedByX.add(new Point(pointX, pointY));
		}

		//System.out.println("pointsSortedByX:" + pointsSortedByX);

		// Sorting by Y Coordinate
		JavaPairRDD<Double, Double> pairsY = words
				.mapToPair(new PairFunction<String, Double, Double>() {
					public Tuple2<Double, Double> call(String s) {

						String[] parts = s.split(",");
						Double key = Double.parseDouble(parts[1]);
						Double value = Double.parseDouble(parts[0]);

						return new Tuple2<Double, Double>(key, value);
					}
				});

		//System.out.println(pairsY.first());
		JavaPairRDD<Double, Double> sortedByY = pairsY.sortByKey();
		//System.out.println(sortedByY.first());

		List<Tuple2<Double, Double>> obj1 = sortedByY.take((int) sortedByY
				.count());

		count = obj1.size();
		for (int i = 0; i < count; i++) {
			Double pointY = obj1.get(i)._1;
			Double pointX = obj1.get(i)._2;

			pointsSortedByY.add(new Point(pointX, pointY));
		}

	//	System.out.println("pointsSortedByX:" + pointsSortedByX);
		//System.out.println("pointsSortedByY:" + pointsSortedByY);

		// Initialize, need to remove existing in output file location.

		long numPoints = sortedByY.count();
		if (numPoints == 0) {
			System.out.println("No Points Found in input");
			return;
		}
		//System.out.println("Generated " + numPoints + " random points");
		

		Pair divConq_ClosestPair = Pair.divideAndConquer(pointsSortedByX,pointsSortedByY);
		/*
		System.out.println(divConq_ClosestPair.point1.x+" "+divConq_ClosestPair.point1.y);
		System.out.println(divConq_ClosestPair.point2.x+" "+divConq_ClosestPair.point2.y);
		System.out.println(divConq_ClosestPair.distance);
		
		System.out.println("From Hash values:"+multiMap.values());
		System.out.println("From Hash keys:"+multiMap.keySet());
		*/
		
		List<String> outputString = new ArrayList<String>();
		outputString.add(divConq_ClosestPair.point1.x+","+divConq_ClosestPair.point1.y);
		outputString.add(divConq_ClosestPair.point2.x+","+divConq_ClosestPair.point2.y);
		
		
		JavaRDD<String> distData = sc.parallelize(outputString);
		distData.saveAsTextFile(args[1]);
		//distData.saveAsTextFile("/home/kranthipc/Downloads/Test Case/ClosestPairResult2.csv");
		

	}
}