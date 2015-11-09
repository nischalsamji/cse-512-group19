package edu.asu.cse512;

import java.io.Serializable;


public class Id_Point implements Serializable {
	private static final long serialVersionUID = 1L;
	public String id;
	public double x1;
	public double y1;
	public Id_Point(String id,double x1, double y1) {
		super();
		this.id = id;
		this.x1 = x1;
		this.y1 = y1;
	}
	
	


}