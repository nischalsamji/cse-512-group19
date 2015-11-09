package edu.asu.cse512;
import java.io.Serializable;

import com.vividsolutions.jts.geom.Geometry;


public class Id_Polygon implements Serializable {


	private static final long serialVersionUID = 1L;
	public String id;
	public Geometry g;
	
	public Id_Polygon(String id, Geometry g) {
		this.id = id;
		this.g = g;
	}

	
}
