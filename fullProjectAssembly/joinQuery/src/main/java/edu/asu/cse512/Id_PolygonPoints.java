package edu.asu.cse512;

import java.io.Serializable;


public class Id_PolygonPoints implements Serializable {
	private static final long serialVersionUID = 1L;
	public String id;
	public double x_1;
	public double y_1;
	public double x_2;
	public double y_2;
	public Id_PolygonPoints(String id,double x_1, double y_1, double x_2, double y_2) {
		super();
		this.id = id;
		this.x_1 = x_1;
		this.y_1 = y_1;
		this.x_2 = x_2;
		this.y_2 = y_2;
	}
	


}
