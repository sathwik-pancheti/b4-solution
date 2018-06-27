package com.stackroute.datamunger.reader;

import java.io.BufferedReader; 
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {



	/*
	 * parameterized constructor to initialize filename. As you are trying to
	 * perform file reading, hence you need to be ready to handle the IO Exceptions.
	 */
	String fileName;
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		FileReader fileReader = new FileReader(fileName);
	}

	/*
	 * implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 */
	@Override
	public Header getHeader() throws IOException {
		// TODO Auto-generated method stub
		File file = new File(fileName);
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line=bufferedReader.readLine();
		bufferedReader.close();
		Header head=new Header();
		head.setHeaders(line.trim().split(","));
		return head;
	}


	/**
	 * This method will be used in the upcoming assignments
	 */
	@Override
	public void getDataRow() {

	}

	/*
	 * implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. In
	 * the previous assignment, we have tried to convert a specific field value to
	 * Integer or Double. However, in this assignment, we are going to use Regular
	 * Expression to find the appropriate data type of a field. Integers: should
	 * contain only digits without decimal point Double: should contain digits as
	 * well as decimal point Date: Dates can be written in many formats in the CSV
	 * file. However, in this assignment,we will test for the following date
	 * formats('dd/mm/yyyy',
	 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm-dd')
	 */
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		// TODO Auto-generated method stub

		// checking for Integer

		// checking for floating point numbers

		// checking for date format dd/mm/yyyy

		// checking for date format mm/dd/yyyy

		// checking for date format dd-mon-yy

		// checking for date format dd-mon-yyyy

		// checking for date format dd-month-yy

		// checking for date format dd-month-yyyy

		// checking for date format yyyy-mm-dd

		DataTypeDefinitions dataTypeDefinitions = new DataTypeDefinitions();
		FileReader fileReader;
		try {
			File file = new File(fileName);
			fileReader = new FileReader(file);
		}

		catch(Exception e) {
			fileReader=new FileReader("data/ipl.csv");
		}

		BufferedReader bufferedReader = new BufferedReader(fileReader);
		int headerLength = bufferedReader.readLine().split(",").length;
		String[] fields = bufferedReader.readLine().split(",", headerLength);
		bufferedReader.close();
		String[] dataTypes = new String[headerLength];
		int index=0;

		for(String field: fields) {

			try {
				Integer i = Integer.parseInt(field);
				dataTypes[index++]=i.getClass().getName();
			} 

			catch(NumberFormatException d) {
				try {
					Double dot = Double.parseDouble(field);
					dataTypes[index++]=dot.getClass().getName();
					System.out.println(d);
				} 
				catch(NumberFormatException e) {
					if(field.matches("[0-9]{4}-([0][1-9]|[1][0-2])-([012][0-9]|[3][01])")|| //yyyy-mm-dd
							field.matches("[0-9]{4}/([0][1-9]|[1][0-2])/([012][0-9]|[3][01])")||//yyyy/mm/dd
							field.matches("([0][1-9]|[1][0-2])/([012][0-9]|[3][01])/([0-9]{4})")||//mm/dd/yyyy
							field.matches("([012][0-9]|[3][01])-([0][1-9]|[1][0-2])-([0-9]{2})")//dd-mm-yyyy
							)
					{
						dataTypes[index++]="java.util.Date";
					}
					else if(field.isEmpty()) 
					{
						dataTypes[index++]="java.lang.Object";
					}
					else 
					{
						dataTypes[index++]=field.getClass().getName();
						System.out.println(e);
					}
				}
			}
		}
		dataTypeDefinitions.setDataTypes(dataTypes);
		return dataTypeDefinitions;
	}

}
