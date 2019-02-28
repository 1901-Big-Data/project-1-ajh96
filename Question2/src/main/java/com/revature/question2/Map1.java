package com.revature.question2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Map1 extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException{
		String line = text.toString();
		line = line.replace(", ", " ");
		String[] rows = line.split(",");
		String CountryName = rows[0];
		String IndicatorCode = rows[3];
		int year = 1960;
		switch(IndicatorCode) {
		case "SE.ADT.1524.LT.FE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && CountryName.equals("United States")) {
						context.write(new Text("Female Literacy Rate (15-24) " + year), new IntWritable(-1));
					} else if (rows[x].length() != 0 && CountryName.equals("United States")){
						Integer Data = Integer.parseInt(rows[x]);
						context.write(new Text("Female Literacy Rate (15-24)"), new IntWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SE.ADT.LITR.FE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && CountryName.equals("United States")) {
						context.write(new Text("Female Literacy Rate (15+) " + year), new IntWritable(-1));
					} else if (rows[x].length() != 0 && CountryName.equals("United States")){
						Integer Data = Integer.parseInt(rows[x]);
						context.write(new Text("Female Literacy Rate (15+) " + year), new IntWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SE.PRM.ENRR.FE":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && CountryName.equals("United States")) {
						context.write(new Text("Percent of Females Enrolled in Primary Education " + year), new IntWritable(-1));
					} else if (rows[x].length() != 0 && CountryName.equals("United States")){
						Integer Data = Integer.parseInt(rows[x]);
						context.write(new Text("Percent of Females Enrolled in Primary Education " + year), new IntWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SE.SEC.ENRR.FE":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && CountryName.equals("United States")) {
						context.write(new Text("Percent of Females Enrolled in Secondary Education " + year), new IntWritable(-1));
					} else if (rows[x].length() != 0 && CountryName.equals("United States")){
						Integer Data = Integer.parseInt(rows[x]);
						context.write(new Text("Percent of Females Enrolled in Secondary Education " + year), new IntWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SE.SCH.LIFE.FE":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && CountryName.equals("United States")) {
						context.write(new Text("Expected Years of Schooling for Females " + year), new IntWritable(-1));
					} else if (rows[x].length() != 0 && CountryName.equals("United States")){
						Integer Data = Integer.parseInt(rows[x]);
						context.write(new Text("Expected Years of Schooling for Females " + year), new IntWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		default:
			break;
		}
	}
}
