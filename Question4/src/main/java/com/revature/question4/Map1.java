package com.revature.question4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Map1 extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	@Override
	public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException{
		String line = text.toString();
		line = line.replace(", ", " ");
		line = line.replace("\"", "");
		String[] rows = line.split(",");
		String CountryName = rows[0];
		String IndicatorCode = rows[3];
		int year = 1960;
		switch(IndicatorCode) {
		case "SL.AGR.EMPL.FE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("Female Employment % in Agriculture: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Female Employment % in Agriculture: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.EMP.MPYR.FE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("% of Female Employers: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("% of Female Employers: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.EMP.OWAC.FE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("% of Own-Account Female Workers: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("% of Own-Account Female Workers: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.EMP.SELF.FE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("% of Self-Employed Female Workers: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("% of Self-Employed Female Workers: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.EMP.WORK.FE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("% of Wage/Salaried Female Workers: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("% of Wage/Salaried Female Workers: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.IND.EMPL.FE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("Female Employment % in Industry: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Female Employment % in Industry: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.SRV.EMPL.FE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("Female Employment % in Services: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Female Employment % in Services: " + CountryName + ", " + year), new DoubleWritable(Data));
					}
					year++;
				} catch (ArrayIndexOutOfBoundsException e) {
					return;
				} catch (NumberFormatException e) {
					return;
				}
			}
			break;
		case "SL.EMP.TOTL.SP.FE.NE.ZS":
			for(int x = 4; x < rows.length; x++) {
				try {
					if(rows[x].length() == 0 && year >= 2000) {
						context.write(new Text("Employment to Population Ratio, Female: " + CountryName + ", " + year), new DoubleWritable(-1));
					} else if (rows[x].length() != 0 && year >= 2000){
						Double Data = Double.parseDouble(rows[x]);
						context.write(new Text("Employment to Population Ratio, Female: " + CountryName + ", " + year), new DoubleWritable(Data));
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
