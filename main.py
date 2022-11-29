from utils.yaml_act import yaml_load
from utils.arg_parse import arg_paser
from CPRD.config.spark import spark_init
from task import *


def main():
    args = arg_paser()
    params = yaml_load(args.params)
    params.update({'save_path': args.save_path})
    print(params)

    # init pyspark
    spark_params = params['pyspark']
    spark = spark_init(spark_params)

    # set up and run task
    run = eval(params['task'])
    run(params, spark)


if __name__ == "__main__":
    main()