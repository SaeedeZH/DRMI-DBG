# DRMI-DBG

DRMI-DBG is a prototype of a distributed iterative de Bruijn Graph assembler over a Hadoop cluster, using Apache Spark (a batch processing engine) and Apache Giraph (a distributed big graph processing system) frameworks. DRMI-DBG is based on the RMI-DBG algorithm[1] and is a distributed solution for assembling multi-cell and single-cell datasets.

## Requirements
This program have been run on a Hadoop 2.10.1 cluster including Spark 3.1.1 and Giraph 1.3.0 frameworks. We use Spark for building the graph(written by Python) and Giraph for processing the graph(written by Java), at each iteration of the iterative DBG algorithm. 

This program is the core of an iterative DBG genome assembler, so except the main stages, we have used some tools to fulfil the pipeline of an assembler. These tools include:

Karect[3] as an Error corrector to preprocess the input sequences. For better quality, it's recommended using an error corrector before running the program.

BWA[4] as a Sequence Aligner to align the output contigs with the input data at each iteration. It'll be installed by the dependecies package. 

BBmap[5] as an Aligner to remove similar and containment contigs. Please download BBmap and put it in this path: DRMI-Pyspark/tools/bbmap/

SGA[6] as a string graph-based sequence assembler to use in the last iteration of the assembly. Please download and install from [here](https://github.com/jts/sga).

Quast[7] as a quality assessment tool to evaluate the final output of the assembly. It'll be installed by the dependecies package.


## Packaging project Dependencies
The libraries that are used in the program are lised in requirements.txt. To install them follow the instruction:
```
pip install -r requirements.txt --target=dependencies
cd dependencies
zip -r dependencies.zip *
mv dependencies.zip ../ 
```

## Run command
After preparing the environment includes Hadoop, Spark and Giraph, for running the program use the following command. We used Stand alone mode for Spark and pseudo-distributed mode for Hadoop.

```
spark-submit --deploy-mode client --driver-memory [x] --total-executor-cores [x] --executor-memory [x] --executor-cores [x] --py-files dependencies.zip Distributed_RMI_v4.py --kmin 20 --kmax 91 --step 10 -c [cut filter] --dsname [Data Set] --ref [ref file path] --inputdir [input dir path] --workers [number of workers] --giraph_jarfile=DBG-Giraph_26.jar --bwa_threads [number of thresds]
```

All required files including the jar file of Giraph code are located in DRMI-Pyspark folder. The java source code of the related jar file is in DRMI-Giraph folder. The input dataset includes two pair files(fastq files) located in a directory must put into the HDFS and specify in the run command. 


The Datasets that are used in the paper can be retrieved from the following[2]: 
http://bix.ucsd.edu/singlecell/


## References
[1] Hosseini, Z. Z., Rahimi, S. K., Forouzan, E., & Baraani, A. (2021). RMI-DBG algorithm: A more agile iterative de Bruijn graph algorithm in short read genome assembly. Journal of Bioinformatics and Computational Biology, 19(2), 2150005.
[2] Chitsaz, H., Yee-Greenbaum, J., Tesler, G., et al. (2011). Efficient de novo assembly of single-cell bacterial genomes from short-read data sets. Nat. Biotechnol. 29, 915–921.

[3] Allam, A., Kalnis, P., & Solovyev, V. (2015). Karect: Accurate correction of substitution, insertion and deletion errors for next-generation sequencing data. Bioinformatics, 31(21), 3421–3428. https://doi.org/10.1093/bioinformatics/btv415
[4] Li, H., & Durbin, R. (2009). Fast and accurate short read alignment with Burrows-Wheeler transform. Bioinformatics, 25(14), 1754–1760. https://doi.org/10.1093/bioinformatics/btp324
[5] Work, R. (2014). BBMap : A Fast, Accurate, Splice-Aware Aligner. In Lawrence Berkeley National Laboratory (pp. 3–5). https://escholarship.org/uc/item/1h3515gn

[6] Simpson, J. T., & Durbin, R. (2012). Efficient de novo assembly of large genomes using compressed data structures. Genome research, 22(3), 549–556. https://doi.org/10.1101/gr.126953.111

[7] Gurevich, A., Saveliev, V., Vyahhi, N., & Tesler, G. (2013). QUAST: Quality assessment tool for genome assemblies. Bioinformatics, 29(8), 1072–1075. https://doi.org/10.1093/bioinformatics/btt086



