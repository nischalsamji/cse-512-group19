package edu.asu.cse512;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.operation.overlay.snap.GeometrySnapper;
import com.vividsolutions.jts.util.GeometricShapeFactory;

public class Polygon implements Serializable{
	
	/* create rectangle reference 
	 * http://math.stackexchange.com/questions/506785/given-two-diagonally-opposite
	 * -points-on-a-square-how-to-calculate-the-other-two */
	
	private double x1;
	private double x2;
	private double y1;
	private double y2;
	private double xcentre;
	private double ycentre;
	private double xdia;
	private double ydia;
	private double   x3;
	private double   y3;
	private double   x4;
	private double   y4;
	public List<Double>  CalcultePolygon(double x1,double y1,double x2,double y2){
		this.x1=x1;this.x2=x2;this.y1=y1;this.y2=y2;
		xcentre=(x1+x2)/2;
		ycentre=(y1+y2)/2;
		xdia=(x1-x2)/2;
		ydia=(y1-y2)/2;
		x3=x2;
		y3=y1;
		x4=x1;
		y4=y2;
		
		System.out.println("x"+x3+"y"+y3+"x"+x4+"y"+y4);
		List<Double> coordinate=Arrays.asList(x3,y3,x4,y4);
		return coordinate;
		
	}
	public double getX1() {
		return x1;
	}
	public void setX1(double x1) {
		this.x1 = x1;
	}
	public double getX2() {
		return x2;
	}
	public void setX2(double x2) {
		this.x2 = x2;
	}
	public double getY1() {
		return y1;
	}
	public void setY1(double y1) {
		this.y1 = y1;
	}
	public double getY2() {
		return y2;
	}
	public void setY2(double y2) {
		this.y2 = y2;
	}
	public double getXcentre() {
		return xcentre;
	}
	public void setXcentre(double xcentre) {
		this.xcentre = xcentre;
	}
	public double getYcentre() {
		return ycentre;
	}
	public void setYcentre(double ycentre) {
		this.ycentre = ycentre;
	}
	public double getXdia() {
		return xdia;
	}
	public void setXdia(double xdia) {
		this.xdia = xdia;
	}
	public double getYdia() {
		return ydia;
	}
	public void setYdia(double ydia) {
		this.ydia = ydia;
	}
	public double getX3() {
		return x3;
	}
	public void setX3(double x3) {
		this.x3 = x3;
	}
	public double getY3() {
		return y3;
	}
	public void setY3(double y3) {
		this.y3 = y3;
	}
	public double getX4() {
		return x4;
	}
	public void setX4(double x4) {
		this.x4 = x4;
	}
	public double getY4() {
		return y4;
	}
	public void setY4(double y4) {
		this.y4 = y4;
	}
}