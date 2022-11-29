import os
import io, zipfile
import pyspark
from pyspark.sql import SQLContext


class spark_init(object):
    def __init__(self, params, name='ehr'):
        self._setup_spark(**params)

        self.sc, self.sqlContext = self._init_spark(name=name)

    def _setup_spark(self, pyspark_env, temp, memory='300g', excutors='4', exe_mem='50g', result_size='80g', offHeap='16g'):
        """ set up pyspark enviroment for data preprocessing
            pyspark_env is the python evironment for spark, for example, pip install pyspark, need to properly
            configutre the enviroment for pyspark

            Args:
            pyspark_env: pyspark environment, for example '/home/yikuan/anaconda/envs/py3/bin/python3.7'
            temp: dir for saving temporary results for spark, please remember to delete after


        """

        os.environ["PYSPARK_PYTHON"] = pyspark_env

        pyspark_submit_args = ' --driver-memory ' + memory + ' --num-executors ' + excutors + \
                              ' --executor-memory ' + exe_mem + \
                              ' --conf spark.driver.maxResultSize={} --conf spark.memory.offHeap.size={} ' \
                              '--conf spark.local.dir={}'.format(result_size, offHeap, temp) + ' pyspark-shell'

        os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

    def _init_spark(self, name='ehr'):
        sc = pyspark.SparkContext(appName=name)
        sqlContext = SQLContext(sc)
        sqlContext.sql("SET spark.sql.parquet.binaryAsString=true")
        sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")

        return sc, sqlContext



def read_txtzip(sc, sqlContext, path):
    """read from txt to pyspark dataframe"""

    def zip_extract(x):
        in_memory_data = io.BytesIO(x[1])
        file_obj = zipfile.ZipFile(in_memory_data, "r")
        files = [i for i in file_obj.namelist()]
        sl = [(file_obj.open(file).read()).decode("utf-8") for file in files]

        flat_list = "".join(sl)
        flat_list = flat_list.split('\r\n')
        return flat_list

    zips = sc.binaryFiles(path)
    files_data = zips.map(zip_extract).flatMap(lambda xs: xs)
    head = files_data.first()
    content = files_data.filter(lambda line: (line != head) and (line != "")).map(lambda k: k.split('\t'))
    df = sqlContext.createDataFrame(content, schema=head.split('\t'))

    return df
def read_txt(sc, sqlContext, path):
    """read from txt to pyspark dataframe"""
    file = sc.textFile(path)
    head = file.first()
    content = file.filter(lambda line: line != head).map(lambda k: k.split('\t'))
    df = sqlContext.createDataFrame(content, schema=head.split('\t'))
    return df


def read_parquet(sqlContext, path):
    """read from parquet to pyspark dataframe"""
    return sqlContext.read.parquet(path)


def read_csv(sqlContext, path):
    """read from parquet to pyspark dataframe"""
    return sqlContext.read.csv(path, header=True)