import getopt
import os
import sys
from datetime import datetime
from subprocess import Popen, PIPE

from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import concat_ws, col, lit, row_number, broadcast
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.window import *


class CreateGraph:
    def __init__(self, sc, spark, k_value: int, input_contigs: str, out_dir_graph: str):
        self.spark = spark
        self.sc = sc
        self.out_dir_graph = out_dir_graph
        self.k_value = k_value
        self.input_contigs = input_contigs
        # self.log_str = log


    def time_duration(self, start: datetime, jname: str, log: bool = False):
        global log_str
        duration = datetime.now() - start
        print(f"@:: '{jname}' command time duration: {duration}\n")
        if log:
            log_str += f"\t{duration}"


    def create_dbg(self, loop_count: int, max_loop_count: int, input_dir: str, bwa_output_list: list,
                   remain_reads: str, cut_filter: int):
        global log_str
        k_value = self.k_value

    ########################functions for maps############################

        def add_reverse_comp(read):
            """
            add reverse complement of reads
            """
            rc_read = str(Seq(read).reverse_complement())

            return read, rc_read


        ### Construct DBG Graph (extract Kmers from read) with output edges
        def compositionWithDBGCons2(read):
            """

            """
            kmerList = []

            for ch in range(len(read) - k_value + 1):
                kmer = read[ch:ch + k_value]
                kmer = kmer.replace("N", "A")
                kmerList.append((kmer, 1))

                # ### add reverse complement of kmer
                # kmerList.append((str(Seq(kmer).reverse_complement()), 1))

            return kmerList


        def createDBG(item):
            """

            """
            k = len(item[0])
            outputEdgeList = []
            prefix = item[0][0:k - 1]
            suffix = item[0][1:]
            if prefix != suffix:
                outputEdgeList.append((prefix, [(suffix, item[1])]))
                outputEdgeList.append((suffix, []))
            return outputEdgeList


        def sumFreq(item):
            """

            """
            sumfreq = 0
            outnodelist = []
            for outnode in item[1]:
                sumfreq += outnode[1]
                outnodelist.append(outnode[0])

            for i in range(4 - len(outnodelist)):
                outnodelist.append(None)

            return (item[0], sumfreq, outnodelist[0], outnodelist[1], outnodelist[2], outnodelist[3])


    ######################################################################
        print(self.sc.getConf().getAll())
        print("loop_count:", loop_count)
        if loop_count == 1:
            print("input_dir:", input_dir)
            start_time = datetime.now()
            print("-> self.sc.textFile(input_dir, )")
            rdd1 = self.sc.textFile(input_dir + "/*")
            print(f"-> {rdd1.getNumPartitions()=}")
            log_str += f"\t{rdd1.getNumPartitions()}"
            # print(f"==>> {rdd1.first()=}")
            # self.time_duration(start_time, "sc.textFile")
            start_time = datetime.now()
            print("-> rdd1.zipWithIndex()")
            rdd1 = rdd1.zipWithIndex()# set an index for each line of fastq file
            self.time_duration(start_time, "rdd1.zipWithIndex", False)

            ### extract the Sequence line from indexed fastq file
            start_time = datetime.now()
            print("-> rdd1.filter().map()")
            seqListRDD = rdd1.filter(lambda x: x[1] & 3 == 1).map(lambda x: x[0])
            # print(f"==>> {seqListRDD.first()=}")
            # print(f"==>> {seqListRDD.count()=}")
            # self.time_duration(start_time, "rdd1.filter().map()")
        else:
            if loop_count == len(k_list):
                part_num = 10
            else:
                part_num = 10
            # print("- input_dir:", bwa_output_list)
            start_time = datetime.now()
            print(f"-> Reading bwa_output_list: sc.parallelize(bwa_output_list, {part_num})")
            print(f"{sys.getsizeof(bwa_output_list)=}")
            seqListRDD = self.sc.parallelize(bwa_output_list, part_num)
            print(f"-> {seqListRDD.getNumPartitions()=}")
            log_str += f"\t{seqListRDD.getNumPartitions()}"

            self.time_duration(start_time, "sc.parallelize", False)

        # start_time = datetime.now()
        print("-> start add_reverse_comp ...")
        seqListRDD_withrc = seqListRDD.flatMap(add_reverse_comp)
        # seqListRDD_withrc = seqListRDD.flatMap(lambda r: (r, str(Seq(r).reverse_complement())))

        # print(f"==>> {seqListRDD_withrc.first()=}")
        # print(f"==>> {seqListRDD_withrc.count()=}")
        # self.time_duration(start_time, "seqListRDD.flatMap(add_reverse_comp)")

        # start_time = datetime.now()
        print("-> start compositionWithDBGCons2 on seqListRDD_withrc ...")
        # print("-> start compositionWithDBGCons2 on seqListRDD ...")
        graphRDD1 = seqListRDD_withrc.flatMap(compositionWithDBGCons2)
        # graphRDD1 = seqListRDD.flatMap(compositionWithDBGCons2)
        print(f"-> {graphRDD1.getNumPartitions()=}")
        # print(f"==>> {graphRDD1.first()=}")
        # print(f"==>> {graphRDD1.count()=}")
        # self.time_duration(start_time, "flatMap(compositionWithDBGCons2)")

        # start_time = datetime.now()
        # if loop_count == 1:
        #     print("-> start partitionBy(700) ...")
        #     graphRDD1 = graphRDD1.partitionBy(700)
        #     print(f"==>> {graphRDD1.first()=}")
        #     self.time_duration(start_time, "partitionBy(700)")
        # else:
        #     print("-> start partitionBy(40) ...")
        #     graphRDD1 = graphRDD1.partitionBy(40)
        #     print(f"==>> {graphRDD1.first()=}")
        #     self.time_duration(start_time, "partitionBy(40)")
        # print(f"-> {graphRDD1.getNumPartitions()=}")

        # start_time = datetime.now()
        print("-> start reduceByKey() ...")
        graphRDD1 = graphRDD1.reduceByKey(lambda a, b: a + b)
        # print(f"==>> {graphRDD1.first()=}")
        # print(f"==>> {graphRDD1.count()=}")
        # self.time_duration(start_time, "reduceByKey(lambda a, b: a + b)")
        # print(f"-> {graphRDD1.getNumPartitions()=}")
        # start_time = datetime.now()
        # print("graphRDD1.count():", graphRDD1.count())
        # self.time_duration(start_time, "graphRDD1.count()")

        # start_time = datetime.now()
        print("-> start  graphRDD1.filter...")
        graphRDD1 = graphRDD1.filter(lambda x: x[1] >= cut_filter)
        # print(f"==>> {graphRDD1.first()=}")
        # self.time_duration(start_time, f"graphRDD1.filter(lambda x: x[1] >= {cut_filter})")

        # start_time = datetime.now()
        # print("graphRDD1.count():", graphRDD1.count())
        # time_duration(start_time, "graphRDD1.count()2")

        # start_time = datetime.now()
        # print("-> start  graphRDD1.coalesce(270)...")
        # graphRDD1 = graphRDD1.coalesce(270)
        # print(f"==>> {graphRDD1.first()=}")
        # print(f"-> {graphRDD1.getNumPartitions()=}")
        # self.time_duration(start_time, "graphRDD1.coalesce()")

        # start_time = datetime.now()
        print("-> start  graphRDD1.flatMap(createDBG)....")
        graphRDD2 = graphRDD1.flatMap(createDBG)
        # print(f"==>> {graphRDD2.first()=}")
        # print(f"-> {graphRDD2.getNumPartitions()=}")
        # self.time_duration(start_time, "graphRDD1.flatMap(createDBG)")

        # graphRDD2.cache()

        # start_time = datetime.now()
        # print("-> start partitionBy(110) ...")
        # graphRDD2 = graphRDD2.partitionBy(110)
        # print(f"==>> {graphRDD2.first()=}")
        # self.time_duration(start_time, "partitionBy(110)")
        # print(f"-> {graphRDD2.getNumPartitions()=}")

        # start_time = datetime.now()
        # print("-> start  graphRDD2.coalesce(350)...")
        # graphRDD2 = graphRDD2.coalesce(350)
        # print(f"==>> {graphRDD2.first()=}")
        # print(f"-> {graphRDD2.getNumPartitions()=}")
        # self.time_duration(start_time, "graphRDD2.coalesce(110)")

        start_time = datetime.now()
        print("-> start  reduceByKey()...")
        graphRDD2 = graphRDD2.reduceByKey(lambda a, b: a + b)
        # print(f"==>> {graphRDD2.first()=}")
        self.time_duration(start_time, "reduceByKey(lambda a, b: a + b)")
        # print(f"-> {graphRDD2.getNumPartitions()=}")

        # start_time = datetime.now()
        #print("graphRDD2.count():", graphRDD2.count())
        #self.time_duration(start_time, "graphRDD2.count()")

        # start_time = datetime.now()
        # print("-> start  graphRDD2.coalesce(150)...")
        # graphRDD2 = graphRDD2.coalesce(150)
        # print(f"==>> {graphRDD2.first()=}")
        # print(f"-> {graphRDD2.getNumPartitions()=}")
        # self.time_duration(start_time, "graphRDD2.coalesce()")

        # start_time = datetime.now()
        print("-> start graphRDD2.map(sumFreq)")
        graphRDD3 = graphRDD2.map(sumFreq)
        # print(f"==>> {graphRDD3.first()=}")
        # self.time_duration(start_time, "graphRDD2.map(sumFreq)")

        # graphRDD3.persist()
        graphRDD3.cache()

        schema = StructType([StructField('id', StringType(), True),
                             StructField('kmer', StringType(), True), StructField('freq', LongType(), True),
                             StructField('o1', StringType(), True), StructField('o2', StringType(), True),
                             StructField('o3', StringType(), True), StructField('o4', StringType(), True)])
        print("-> graphRDD3.zipWithUniqueId().map().toDF(schema)")
        start_time = datetime.now()
        df2 = graphRDD3.zipWithUniqueId()\
            .map(lambda x: (x[1], x[0][0], x[0][1], x[0][2], x[0][3], x[0][4], x[0][5])).toDF(schema)
        # print(f"-> {df2.rdd.getNumPartitions()=}")
        # print(f"==>> {df2.first()=}")
        self.time_duration(start_time, "graphRDD3.zipWithUniqueId().map().toDF(schema)", False)

        df1 = df2.select("id", "kmer")
        print(f"-> {df1.rdd.getNumPartitions()=}")

        # start_time = datetime.now()
        print("-> df2.join(df1).join(df1).join(df1).join(df1).join(df1)")
        df_dbg = df2.alias('graph').join(df1.alias('mapId1'), col("o1") == col("mapId1.kmer"), "left") \
            .select(col('graph.id').alias('kmer_id'), col('graph.kmer').alias('node_kmer'), 'freq',
                    col('mapId1.id').alias('o1_id'), 'graph.o2', 'graph.o3', 'graph.o4') \
            .join(df1.alias('mapId2'), col("o2") == col("mapId2.kmer"), "left") \
            .select('kmer_id', 'node_kmer', 'freq', 'o1_id', col('mapId2.id').alias('o2_id'), 'graph.o3', 'graph.o4') \
            .join(df1.alias('mapId3'), col("o3") == col("mapId3.kmer"), "left") \
            .select('kmer_id', 'node_kmer', 'freq', 'o1_id', 'o2_id', col('mapId3.id').alias('o3_id'), 'graph.o4') \
            .join(df1.alias('mapId4'), col("o4") == col("mapId4.kmer"), "left") \
            .select('kmer_id', 'node_kmer', 'freq', 'o1_id', 'o2_id', 'o3_id', col('mapId4.id').alias('o4_id'))

        # print(f"==>> {df_dbg.count()=}")
        # print("df_dbg.count() before string graph:", df_dbg.count())
        # print(f"==>> {df_dbg.first()=}")
        self.time_duration(start_time, "df2.join(df1).join(df1).join(df1).join(df1)", False)
        # self.time_duration(start_time, "above commands", False)

        if (loop_count > 1) and (loop_count < max_loop_count):
            start_time = datetime.now()
            df_dbg = self.create_string_graph(df_dbg)
            self.time_duration(start_time, "create_string_graph", True)
        else:
            log_str += "\t\t\t\t"

        # print("df_dbg.count() after string graph:", df_dbg.count())
        start_time = datetime.now()
        print(f"-> {df_dbg.rdd.getNumPartitions()=}")
        print("-> df_dbg.coalesce(1).write.csv")
        df_dbg.coalesce(1).write.mode("append").csv(self.out_dir_graph)#+"/df_dbg")
        self.time_duration(start_time, "df_dbg.coalesce(1).write.csv", True)


    def create_string_graph(self, df_dbg):
        """

        :return:
        """
        global log_str
        print("input_contigs:", self.input_contigs)

        ### read contigs file from bbmap output ----> read file from master disck(fs)
        ### bbmap output file splits contigs to 70 character line by line
        print("-> start process fasta file (input_contigs)")
        start_time = datetime.now()
        read_list = []
        with open(input_contigs) as handle:
            for seq_record in SeqIO.parse(handle, "fasta"):
                read = str(seq_record.seq)
                read_list.append(read)
        self.time_duration(start_time, "process fasta file with SeqIO", False)
        print(f"{len(read_list)=}")
        log_str += f"\t{len(read_list)}"
        print(f"{sys.getsizeof(read_list)=}")
        log_str += f"\t{sys.getsizeof(read_list)}"

        print("-> start sc.parallelize(read_list, ).flatMap(addReverse).map().toDF()")
        start_time = datetime.now()
        schema_cng = StructType([StructField("contig", StringType(), True)])
        df_contigs = self.sc.parallelize(read_list).flatMap(lambda r: (r, str(Seq(r).reverse_complement())))\
            .map(lambda x: (x, )).toDF(schema_cng)
        self.time_duration(start_time, "sc.parallelize(read_list, ).flatMap(addReverse).map().toDF()", False)
        print(f"-> {df_contigs.rdd.getNumPartitions()=}")
        log_str += f"\t{df_contigs.rdd.getNumPartitions()}"

        ###
        # df_contigs = self.spark.read.options(inferSchema="true").csv(input_contigs)
        print("-> start adding id to contigs by window and row_number() over 100000000")
        win = Window.orderBy(col('contig'))
        df_contigs = df_contigs.withColumn('contig_id', row_number().over(win) + lit(100000000))

        print("-> df_dbg.join(df_contigs)")
        start_time = datetime.now()
        # df_suff = df_dbg.select("kmer_id", "node_kmer") \
        #     .join(df_contigs, col("node_kmer")[-(self.k_value - 2):self.k_value - 2] == col("contig")[1:self.k_value - 2])
        df_suff = df_dbg.join(broadcast(df_contigs),
                              col("node_kmer")[-(self.k_value - 2):self.k_value - 2] == col("contig")[1:self.k_value - 2], "left")

        df_dbg1 = df_suff.groupBy("kmer_id", "node_kmer", "freq", "o1_id", "o2_id", "o3_id", "o4_id")\
            .agg(F.collect_list("contig_id").alias("out_list"))\
            .select("kmer_id", "node_kmer", "freq", "o1_id", "o2_id", "o3_id", "o4_id",
                    concat_ws(",", col("out_list")).alias("out"))
        self.time_duration(start_time, "df_suff.groupBy().agg(F.collect_list())", False)

        print("-> df_contigs.join(df_dbg).rdd.map.reduceByKey.toDF(schema)")
        start_time = datetime.now()
        df_pref = broadcast(df_contigs)\
            .join(df_dbg.select("kmer_id", "node_kmer"),
                  col("node_kmer")[1:self.k_value - 2] == col("contig")[-(self.k_value - 2):self.k_value - 2], "left")

        df_contigs = df_pref.groupBy("contig_id", "contig").agg(F.collect_list("kmer_id").alias("out_list"))\
            .select("contig_id", "contig", concat_ws(",", col("out_list")).alias("out"))
        # print("==>> df_contigs.first()")
        # df_contigs.first()
        self.time_duration(start_time, "df_dbg.join(df_contigs).groupBy().agg(F.collect_list())", False)

        print(f"-> {df_contigs.rdd.getNumPartitions()=}")
        print(f"-> {df_dbg.rdd.getNumPartitions()=}")
        print(f"-> {df_dbg1.rdd.getNumPartitions()=}")

        print("-> df_contigs.coalesce(1).write.csv")
        start_time = datetime.now()
        df_contigs.coalesce(1).write.mode("append").csv(self.out_dir_graph)#+"/df_contigs")
        self.time_duration(start_time, "df_contigs.coalesce(1).write.csv", False)

        # print("df_dbg1.count():", df_dbg1.count())
        print("->> Finish Creating String Graph")
        return df_dbg1

# end of class


def run_cmd(cmd: str, jname: str, log: bool):
    """

    """
    global log_str
    start_datetime = datetime.now()
    # start_time = time.time()
    print("-" * 50)
    print(f'{jname} command line is:')
    print(cmd)

    cmd_process = Popen(cmd, shell=True)
    cmd_process.wait()
    exitSTAT = cmd_process.poll()
    stdout, stderr = cmd_process.communicate()
    if exitSTAT == 0:
        print(f'{jname} was done successfully')
        time_duration = datetime.now() - start_datetime
        print(f"{jname} command time duration: {time_duration}")
        if log:
            log_str += f"\t{time_duration}"
        print("-" * 50)
        return 0
    else:
        sys.stderr.write(f'{jname} encountered an ERROR!\n')
        sys.stderr.write(str(stderr) + '\n')
        print("!" * 50)
        sys.exit(2)


def get_args(argv):
    global input_file1, input_file2, input_dir, dsname, ref_file, kmin, kmax, kstep, cut_filter_list\
        , workers_num, giraph_jarfile, sparkbwa_jarfile, bwa_threads, k_list

    try:
        opts, args = getopt.getopt(argv, "c:", ["klist=", "kmin=", "kmax=", "step=", "ifile1=", "ifile2=", "inputdir=", "dsname=",
                                                "ref=", "workers=", "giraph_jarfile=", "sparkbwa_jarfile=", "bwa_threads="])
    except getopt.GetoptError:
        print(
            'test.py -c <> --kmin=<> --kmax=<> --step=<> --ifile1=<> --ifile2=<> --inputdir=<> --dsname=<> --ref=<> '
            '--workers=<> --giraph_jarfile=<> --sparkbwa_jarfile=<> bwa_threads=<>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("--klist"):
            k_list = arg.split(",")
            print(k_list)
        elif opt in ("--ifile1"):
            input_file1 = arg
            print(input_file1)
        elif opt in ("--ifile2"):
            input_file2 = arg
            print(input_file2)
        elif opt in ("--inputdir"):
            input_dir = arg
            print(input_dir)
        elif opt in ("--dsname"):
            dsname = arg
            print(dsname)
        elif opt in ("--ref"):
            ref_file = arg
            print(ref_file)
        elif opt in ("--kmin"):
            kmin = int(arg)
            print(kmin)
        elif opt in ("--kmax"):
            kmax = int(arg)
            print(kmax)
        elif opt in ("--step"):
            kstep = int(arg)
            print(kstep)
        elif opt in ("-c"):
            cut_filter_list = arg.split(",")
            print(cut_filter_list)
        elif opt in ("--workers"):
            workers_num = int(arg)
            print(workers_num)
        elif opt in ("--sparkbwa_jarfile"):
            sparkbwa_jarfile = arg
            print(sparkbwa_jarfile)
        elif opt in ("--giraph_jarfile"):
            giraph_jarfile = arg
            print(giraph_jarfile)
        elif opt in ("--bwa_threads"):
            bwa_threads = int(arg)
            print(bwa_threads)

def correction_with_karect():
    karect_output_dir = "KarectedFiles"
    karect_cmd = f"./tools/karect -correct -inputfile={input_file1} -inputfile={input_file2} " \
                 f"-resultdir={karect_output_dir} -matchtype=hamming -celltype=haploid"

    cwd = os.getcwd()
    print(cwd)
    if not os.path.exists(karect_output_dir):
        os.mkdir(karect_output)

    run_cmd(karect_cmd, "Karect")

    return karect_output_dir


def set_cut_filter(loop_cnt: int, filter_list: list, k_list: list):
    """
    define cut_filter base on counter of loop
    :param loop_cnt: current counter of loop
    :param filter_list: list of cut_filters
    :param k_list: list of k_values
    :return: a cut_filter for current k_value
    """

    if loop_cnt == 1:
        cut_filter = filter_list[0]
    elif (loop_cnt < len(k_list) / 3):
        cut_filter = filter_list[1]
    elif (loop_cnt < len(k_list)):
        cut_filter = filter_list[2]
    elif (loop_cnt == len(k_list)):
        cut_filter = filter_list[3]

    return int(cut_filter)


def set_k_list(start: int, end: int, step: int):
    """
        create list of k_values
        :return: list of k_values (k_list)
    """
    global k_list
    for i in range(end, start, -step):
        k_list.append(i)
    print(k_list)


def start_spark(conf):
    """

    """
    # conf = SparkConf().getAll()
    sc = SparkContext(conf=conf).getOrCreate()
    # sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel("ERROR") # Stop INFO & DEBUG message logging to console
    # sc._conf.get('spark.driver.memory')
    return sc, spark


def main(inp_file1: str, inp_file2: str):
    """

    """
    global k_list, cut_filter_list, output_dir, dsname, output_graph, input_contigs
    global workers_num, giraph_jarfile, sparkbwa_jarfile, log_str, bwa_threads
    # karect_output = correction_with_karect()

    loop_count: int = 0
    max_loop_count = len(k_list)

    bbmap_output = ""
    input_contigs = "null"
    remain_reads = "null"
    # bwa_output_on_hdfs = "null"
    bwa_output_list: list = []
    log_str += f"K\tcut\tseqList_Partitions\tinput_contigs\tsizeof(input_contigs)\tcontigs_Partitions\t" \
               f"Create Graph1\tCreate Graph2\tCreate Graph\tGiraph\tBWA mem\t" \
               f"all_reads\tremain_reads\tsizeof(remain_reads_list)\tloop_time\n"

    for k in k_list:
        k_value = int(k)
        start_loop_time = datetime.now()
        loop_count += 1
        cut_filter = set_cut_filter(loop_count, cut_filter_list, k_list)
        print("#"*60)
        print("loop_count:", loop_count)
        print("k_value:", k_value)
        print("cut_filter:", cut_filter)
        print("-"*60)
        log_str += f"K{k_value}\t{cut_filter}"
        # print(f"{log_str=}")

        output_sub_dir = output_dir + "/" + dsname + "_K" + str(k_value)
        cmd = f"hdfs dfs -mkdir -p {output_sub_dir}"
        run_cmd(cmd, "create output sub directory", False)

        output_graph = output_sub_dir + "/output_graph"

        if loop_count == 1:
            spark_config = SparkConf()

        sc, spark = start_spark(spark_config)

        print(">>>> start creating Graph in spark <<<<")
        dbg_object = CreateGraph(sc, spark, k_value, input_contigs, output_graph)
        start_time = datetime.now()
        dbg_object.create_dbg(loop_count, max_loop_count, input_dir, bwa_output_list, remain_reads, cut_filter)
        duration = datetime.now() - start_time
        print(f">>>>  Creating Graph k{k_value} in spark finished in {duration} <<<<")
        log_str += f"\t{duration}"

        print("-> ...sparkContext stopped")
        sc.stop()

        giraph_input = output_graph
        giraph_output = f"{output_sub_dir}/giraph_output_k{k_value}"
        cmd = f"giraph {giraph_jarfile} " \
              f"example.DBGProcessComputation_ListRankingByPPA " \
              f"-mc example.DBGProcessMasterCompute " \
              f"-vif example.DBGProcessTextVertexInputFormat -vip {giraph_input} " \
              f"-vof example.DBGProcessContigFastaOutputFormat -op {giraph_output} -w {workers_num} " \
              f"-ca k_value={k_value - 1}"
        if workers_num == 1:
            cmd += " -ca giraph.SplitMasterWorker=false"

        run_cmd(cmd, "giraph", True)

        dir_path_on_fs = f"/tmp/{output_dir}"
        cmd = f"mkdir -v -p {dir_path_on_fs}"
        run_cmd(cmd, "mkdir output_dir on master fs", False)

        bbmap_output =  "/tmp/" + output_dir.split("/")[1] + f"_index_k{k_value}/bbmap_out_k{k_value}.fa"
        cmd = f"mkdir -v -p {os.path.dirname(bbmap_output)}"
        run_cmd(cmd, "mkdir indexes on master fs", False)

        bbmap_input = f"{dir_path_on_fs}/giraph_out_k{k_value}.fa"
        cmd = f"hdfs dfs -getmerge {giraph_output}/p* {bbmap_input}"
        run_cmd(cmd, "copy from hdfs", False)

        cmd = f"tools/bbmap/dedupe.sh in={bbmap_input} out={bbmap_output}"
        run_cmd(cmd, "bbmap", False)

        #
        # bbmap_fq_output = f"{dir_path_on_fs}/bbmap_out_k{k_value}.fq"
        # # cmd = f"~/PycharmProjects/Tools/bbmap/reformreformat.sh in={bbmap_output} out={bbmap_fq_output}"
        # cmd = f"tools/bbmap/reformat.sh in={bbmap_output} out={bbmap_fq_output}"
        # run_cmd(cmd, "reformat")

        # cmd = f"hdfs dfs -put {bbmap_fq_output} {output_sub_dir}"
        # cmd = f"hdfs dfs -put {bbmap_output} {output_sub_dir}"
        # run_cmd(cmd, "copy to hdfs")

        # if loop_count == 1:
        #     cmd = "hdfs dfs -ls " + input_dir + " | awk '{print $8}'"
        #     proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        #     s_output, s_err = proc.communicate()
        #     input_file_list = s_output.split()  # stores list of files and sub-directories in 'path'
        #     input_file1 = str(input_file_list[0], "utf-8")
        #     input_file2 = str(input_file_list[1], "utf-8")

        if loop_count < max_loop_count:

            cmd = f"bwa index {bbmap_output}"
            run_cmd(cmd, "bwa index", False)

            # for i in range(1, 4, 1):
            #     cmd = f"scp -r {os.path.dirname(bbmap_output)} slave{i}:/tmp/"
            #     run_cmd(cmd, f"scp index to slave{i}")

            # bwa_output_file = "bwa_output.sam"
            # bwa_output_on_fs = f"{dir_path_on_fs}/{bwa_output_file}"
            if loop_count == 1:
                cmd = f"bwa mem -T 0 -t {bwa_threads} {bbmap_output} {inp_file1} {inp_file2}"
            else:
                merged_read_file = f"{dir_path_on_fs}/merged_read_file_k{k_value}.fa"
                cmd = f"cat {inp_file1} {inp_file2} > {merged_read_file}"
                run_cmd(cmd, "cat and merge file", False)
                cmd = f"bwa mem -T 0 -t {bwa_threads} {bbmap_output} {merged_read_file}"

            print(f"bwa mem command: {cmd}")
            start_time = datetime.now()
            bwa_output_tmp = os.popen(cmd).readlines()
            duration = datetime.now() - start_time
            print(f"bwa mem finished in: {duration}")
            log_str += f"\t{duration}"
            print(f"{len(bwa_output_tmp)=}")
            print(f"{sys.getsizeof(bwa_output_tmp)=}")

            bwa_output_list = []
            # remain_reads = dir_path_on_fs + f"/remain_reads_k{k_value}.fa"
            remain_reads = dir_path_on_fs + "/remain_reads.fa"
            start_time = datetime.now()
            id_counter = 0
            all_reads_counter = 0
            with open(remain_reads, "w") as bwa_file:
                for item in bwa_output_tmp:
                    if not item.startswith("@"):
                        myline = item.split()
                        cutoff = len(myline[9])
                        if (myline[11].split(":")[0] == "AS" and int(myline[11].split(":")[2]) < cutoff) or \
                                (myline[13].split(":")[0] == "AS" and int(myline[13].split(":")[2]) < cutoff):
                            bwa_output_list.append(myline[9])
                            bwa_file.write(f">{id_counter}\n{myline[9]}\n")
                            id_counter += 1
                        all_reads_counter += 1
            bwa_file.close()
            bwa_output_tmp = []
            print(f"find remain reads: {datetime.now() - start_time}")
            print(f"{len(bwa_output_list)=}")
            print(f"{sys.getsizeof(bwa_output_list)=}")
            log_str += f"\t{all_reads_counter}\t{len(bwa_output_list)}\t{sys.getsizeof(bwa_output_list)}"

        else:
            log_str += "\t\t\t\t"

        input_contigs = bbmap_output

        inp_file1 = remain_reads
        inp_file2 = bbmap_output

        loop_time = datetime.now() - start_loop_time
        print(f"loop count {loop_count} time duration: {loop_time}")
        log_str += f"\t{loop_time}\n"


def sga_quast(file1, file2, data_dir, refGenFileAddress, in_f1, in_f2):
    m = 0

    sgadirname = data_dir
    if not os.path.exists(sgadirname):
        os.mkdir(sgadirname)
    outputfile = "last-graph.fasta"
    handle1 = open(file1)
    handle2 = open(file2)
    output_handle = open(sgadirname +"/"+ outputfile, "w")
    cnt_file1 = 0
    cnt_file2 = 0
    for seq_record in SeqIO.parse(handle1, "fasta"):
        # seq1 = seq_record.seq
        SeqIO.write(SeqRecord(seq_record.seq, id=str(m)), output_handle, "fasta")
        cnt_file1 += 1
        m += 1
    for seq_record in SeqIO.parse(handle2, "fasta"):
        SeqIO.write(SeqRecord(seq_record.seq, id=str(m)), output_handle, "fasta")
        cnt_file2 += 1
        m += 1
    print('cnt_file1', cnt_file1)
    print('cnt_file2', cnt_file2)
    output_handle.close()
    handle1.close()
    handle2.close()

    str_precommand = "cd " + sgadirname + " \r\n"
    strCommand = str_precommand + " sga index -t 4 " + outputfile
    print(strCommand)
    os.system(strCommand)
    rmdupfile = outputfile.split('.')[0] + "_rmdup.fa"
    strCommand = str_precommand + " sga rmdup " + outputfile + " -o " + rmdupfile
    print(strCommand)
    os.system(strCommand)
    strCommand = str_precommand + " sga overlap -m 20 -t 4 " + rmdupfile
    run_cmd(strCommand, "sga overlap", False)
    strCommand = str_precommand + " sga assemble " + rmdupfile.split('.')[0] + ".asqg.gz -o assembled"
    run_cmd(strCommand, "sga assemble", False)
    strCommand = f"cp  {sgadirname}/assembled-contigs.fa {sgadirname}/contigs"
    print(strCommand)
    os.system(strCommand)
    assembledfile = "assembled-contigs.fa"
    if refGenFileAddress == "null":
        strCommand = f"~/tools/quast/quast.py {data_dir}/contigs/assembled-contigs.fa -1 {in_f1} -2 {in_f2} -o {data_dir}/QuastResult -m 500"
    else:
        strCommand = "~/tools/quast/quast.py " + data_dir + "/contigs/* -r " + refGenFileAddress + " -o " + data_dir + "/QuastResult -m 500"
        run_cmd(strCommand, "Quast", False)
    return (sgadirname + "/" + assembledfile)


kmin: int = 0
kmax: int = 0
kstep: int = 0
k_list: list = []

workers_num: int = 0

cut_filter_list = []

bwa_threads = 0

giraph_jarfile = ""
sparkbwa_jarfile = ""

ref_file = ""
dsname = ""
input_dir = ""
output_dir = ""
input_contigs = ""
karect_output = ""
output_dbg = ""
output_graph = ""

log_str = ""


if __name__ == '__main__':
    start_time_total = datetime.now()

    get_args(sys.argv[1:])
    if k_list == []:
        set_k_list(kmin, kmax, kstep)
    output_dir = f"outputs/output_{str(datetime.now().date())}_{str(datetime.now().time().hour)}-" \
                 f"{str(datetime.now().time().minute)}_{dsname}"
    cmd = f"hdfs dfs -mkdir -p {output_dir}"
    run_cmd(cmd, "create output directory", False)
    log_str = f"Start:\t{start_time_total}\t{dsname}\t{ref_file}\n"

    cmd = "hdfs dfs -ls " + input_dir + " | awk '{print $8}'"
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    s_output, s_err = proc.communicate()
    input_file_list = s_output.split()  # stores list of files and sub-directories in 'path'
    input_file1 = str(input_file_list[0], "utf-8")
    input_file2 = str(input_file_list[1], "utf-8")

    log_str += f"\t{input_file1}\t{input_file2}\n"
    log_str += f"\t{k_list}\t{cut_filter_list}\n"

    main(input_file1, input_file2)

    print("="*50)

    contigs_dir = output_dir + "/contigs"
    os.makedirs(contigs_dir, exist_ok=True)

    cmd = "cp /tmp/" + output_dir.split("/")[1] + f"_index_k*/bbmap_out_k*.fa  {contigs_dir}"
    run_cmd(cmd, "copy contigs to output folder", False)

    f1 = f"{contigs_dir}/bbmap_out_k{k_list[len(k_list)-1]}.fa"
    f2 = f"{contigs_dir}/bbmap_out_k{k_list[len(k_list)-2]}.fa"
    sga_quast(f1, f2, output_dir, ref_file, input_file1, input_file2)

    print("="*50)

    duration = datetime.now() - start_time_total
    print(f"TOTAL TIME DURATION: {duration}")
    log_str += f"\n\tTotal Time Duraion:\t{duration}"

    cmp_file = open(output_dir + "/" + dsname + "_" + output_dir.split("_")[1] + "_" + output_dir.split("_")[2] + "_CompareFile.tsv", "w")
    cmp_file.write(log_str)
    cmp_file.close()