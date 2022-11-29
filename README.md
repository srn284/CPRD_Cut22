# Structure

## requirements to begin usage
requirements.txt has all the packages required to use and contribute to this repo. Please create a new conda environment (python 3.7) with the function: 
```shell
conda create -n myenv python=3.7
```
Then proceed to navigate to the CPRD folder and install all requirements in the following way:
```shell
pip install -r requirements.txt
```
## config
config includes yaml file which specifies all parameters for genearting data
## CPRD
this folder includes 4 components:  
spark.py set up functions to initialise pyspark, which is then used for data processing.
table.py includes fundamental functions to process each table in CPRD, and utils includes commonly used functions for data processing. functions includes functions for data processing, this is a more specific function folder
## task
task defines the real task
## run the script
```shell
python main.py --params config/XXX.yaml --save_path XXX.
```

example is:
 ```shell
python main.py --params config/config.yaml --save_path config/
```

params is the location of yaml file, and save_path is the directory to save final results
