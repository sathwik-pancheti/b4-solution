package com.stackroute.datamunger.query;

//this class contains the data type definitions
public class DataTypeDefinitions {

	/*
	 * this class should contain a member variable which is a String array, to hold
	 * the data type for all columns for all data types and should override toString() method as well.
	 */	
	  String[] dataTypes;
		public String[] getDataTypes() {
			return dataTypes;
		}
		
		public void setDataTypes(String[] dataTypes) {
	        this.dataTypes = dataTypes;
	    } 
	     
}
