package com.eng;

import java.io.IOException;
import java.util.regex.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeblogMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String reExpression = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
		String hostAddr,prodLink,statusCode,primCategory,seconCategory,hostLink,temp,temp1,line;
		Pattern pat = Pattern.compile(reExpression);
		Matcher matcher = pat.matcher(value.toString());
		if (!matcher.matches() ) {
			System.err.println("error");
			return;
		}
		hostAddr = matcher.group(1);
		prodLink= matcher.group(5);
		statusCode= matcher.group(6);
		temp = prodLink.replaceAll("/", ",");
		temp1= temp.replace(".", ",");
		line = temp1.toString();
		String[] parts= line.split(",");
		primCategory= parts[1];
		seconCategory= parts[2];
		hostLink=parts[3];
		String result = hostAddr +"\t"+primCategory +"\t"+seconCategory+"\t"+hostLink+ "\t" + statusCode;
		context.write(NullWritable.get(), new Text(result));
	}
}
