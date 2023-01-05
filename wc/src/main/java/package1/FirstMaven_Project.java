package package1;

import java.io.File;
import java.io.StreamTokenizer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.Tuple2;

public class FirstMaven_Project 
{
	public static void main(String[] args) throws Exception
	{
		//setup the stream execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		// setting up the environment for global parameters
		env.getConfig().setGlobalJobParameters(params);
		DataSet<String> text = env.readTextFile(params.get("input"));
		
		//filter name starting with B
		DataSet<String> filtered_Name = text.filter(new FilterFunction<String>() {
			
			public boolean filter(String value) throws Exception {
				
				return value.startsWith("B");
			}
		});
		
		DataSet<Tuple2<String ,Integer>> tokenized = filtered_Name.map(new Tokenizer());
		DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
			
		if (params.has("output"))
		{
			counts.writeAsCsv(params.get("output"), "\n", " ");
			//counts.print();
			env.execute("TEST!!!");
		}// if
		
	}// main method
	
	public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>>
	{
		public Tuple2<String, Integer> map (String value)
		{
			return new Tuple2<String, Integer>(value,1);
		}
	}//
	
}
