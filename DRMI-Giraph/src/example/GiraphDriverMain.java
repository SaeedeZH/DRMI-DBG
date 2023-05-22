package example;

import org.apache.giraph.GiraphRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GiraphDriverMain {
	public static void main(String[] args) throws Exception {
		
//		System.out.println("Number of Command Line Argument = "+args.length);
//		for(int i = 0; i< args.length; i++) {
//			System.out.println(String.format("Command Line Argument %d is %s", i, args[i]));
//		}
		
//		GiraphRunner.main(args);
		
//		String inputPath = "/home/myuser/SparkProjects/DBGConstructionwithSpark/DBGoutput_1000_200_filter3/*";
//		String inputPath = "./InputDir3/InputData_new.csv";
//		String inputPath = "~/SparkProjects/DBG/ConstructionwithSpark/Karect_Rhodo/*";
//		String inputPath = "/home/myuser/SparkProjects/DBGConstructionwithSpark/parquet_test.parquet/*";
//		String inputPath = "/home/myuser/SparkProjects/DBGConstructionwithSpark/Rhodo_102000_Graph1/*";
		String inputPath = "/home/myuser/SparkProjects/DBGConstructionwithSpark/Rhodo_Graph8/*";
		
//		String outputPath = "OutputDir/output_new_9";
//		String outputPath = "OutputDir/Rhodo_102000_Output2";
		String outputPath = "OutputDir/Rhodo_1";
		
		// create Giraph Configuration object
		GiraphConfiguration giraphConf = new GiraphConfiguration();
		
		// set Computation class
		giraphConf.setMasterComputeClass(DBGProcessMasterCompute.class);
		giraphConf.setComputationClass(DBGProcessComputation_ListRankingByPPA.class);
		
		// set Input Format class
		giraphConf.setVertexInputFormatClass(DBGProcessTextVertexInputFormat.class);
		
		// set Input Path
		GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(inputPath));
		
		// set other properties
		giraphConf.setLocalTestMode(false);
		giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
		giraphConf.setWorkerConfiguration(1, 1, 100);
		
		
		// set Output Format class
//		giraphConf.setVertexOutputFormatClass(DBGProcessTextVertexOutputFormat.class);
		giraphConf.setVertexOutputFormatClass(DBGProcessContigFastaOutputFormat.class);
		
		// create Giraph Job object
		GiraphJob giraphJob = new GiraphJob(giraphConf, "GiraphDriverMain");
		
		// set OutputPath		
		FileOutputFormat.setOutputPath(giraphJob.getInternalJob() , new Path(outputPath));
		
		// run Giraph Job	
		giraphJob.run(true);
		
		
//		try {
//			giraphJob.run(true);
//		} catch (Exception e) {
//			// TODO: handle exception
//			System.out.println(e.toString());
//		}
		
		
	}
}


