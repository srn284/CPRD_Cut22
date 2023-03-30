

import pyspark.sql.functions as F
from pyspark.sql import Window
from CPRD.base.table import *
from CPRD.functions import tables, merge

class PredictorExtractorBase:
    def predictor_extract(self, df, demographics, col, colname = 'code', col_baseline='study_entry', span_before_baseline_month=None,span_after_baseline_month=None,
                          type='last' ):
        """
        function to extract predictor at baseline, specifically for measurement type.
        extract the last record, or variance, or mean over vertain time period
        df: dataframe contains patid, eventdate, and columns for extraction, other columns will be ignored
        demographics: demographics dataframe that should include patid, and column to indicate the baseline date
        col: the column name in the df to be considered for extraction
        colname: name of column - default is 'code'
        col_baseline: the columns name in the demographics to indicate the baseline date
        span_before_baseline_month: only consider a time duration in month before the baseline for extraction,
                                    if None, all records before baseline will be considered
        span_after_baseline_month: only consider a time duration in month after the baseline for extraction,
                                    if None, NO records after baseline will be considered
        type: 'last', or 'var', or 'mean' type of information to be extracted,
                'last' returns the information in the latest time of recording
                'var' returns the variance of predictor within the defined time period
                'mean' returns the mean of predictor within the defined time period
        """

        # select study entry date as reference for predictor filtering
        demographics = demographics.select(['patid', col_baseline])

        # join dataframe with demographics keep patients existing in both and before study entry
        df = df.join(demographics, 'patid', 'inner')

        if span_before_baseline_month is not None:
            df = df.withColumn('span_date_before', df[col_baseline]
                               - F.expr('INTERVAL {} MONTH'.format(span_before_baseline_month)))
            df = df.where(F.col('eventdate') > F.col('span_date_before')).drop('span_date_before')
        if span_after_baseline_month is not None:
            df = df.withColumn('span_date_after', df[col_baseline]
                                + F.expr('INTERVAL {} MONTH'.format(span_after_baseline_month)))
            df = df.where(F.col('eventdate') < F.col('span_date_after')).drop('span_date_after')
        else:
            df= df.where(F.col('eventdate') < F.col(col_baseline))
        w = Window.partitionBy('patid').orderBy('eventdate')
        if type == 'last':
            df = df.withColumn(col, F.last(col).over(w)).groupBy('patid').agg(F.last(col).alias(col))
        elif type == 'var':
            df = df.withColumn(col, F.last(col).over(w)).groupBy('patid').agg(F.variance(col).alias(col))
        elif type == 'mean':
            df = df.withColumn(col, F.last(col).over(w)).groupBy('patid').agg(F.mean(col).alias(col))
        elif type == 'std':
            df = df.withColumn(col, F.last(col).over(w)).groupBy('patid').agg(F.stddev(col).alias(col))
        else:
            raise ValueError('type need to be set as either last, or var, or mean')

        df = df.select(['patid', col]).withColumnRenamed(col, colname)
        return df

    def predictor_check_exist(self, condition, df, demographics, col, colname='code', countsBased=-1, col_baseline='study_entry'):
        """
        check the existance of a condition before the baseline, exist 1, not exist 0
        condition: a list of code to be checked
        df: the dataframe to be checked includes patid, eventdate, and column to be checked
        demographics: demographics dataframe includes patid, column indicates the baseline date
        col: columns name in df indicates the column to be checked
        colname: name of column - default is 'code'
        col_baseline: column name in demographics indicates the baseline date
        """
        # keep all records before the baseline date
        demographics = demographics.select(['patid', col_baseline])
        df = df.join(demographics, 'patid', 'inner').where(F.col('eventdate') <= F.col(col_baseline))

        # cast positive patients with exist = 1

        if countsBased==-1:
            df = df.filter(F.col(col).isin(*condition)).select(['patid']).dropDuplicates().withColumn('exist', F.lit(1))
        else:
            df =  df.filter(F.col(col).isin(*condition)).select(['patid']).groupBy("patid").agg(

                F.count(F.lit(1)).alias("existCount")
            )
            df = df.filter(F.col('existCount')>=countsBased)
            df = df.withColumn('exist', F.lit(1)).drop('existCount')

        demographics = demographics.join(df, 'patid', 'left')

        # separate positive patients and negative patients and set exist = 0 for negative patients
        positive = demographics.filter(F.col('exist').isNotNull())
        negative = demographics.filter(F.col('exist').isNull()).withColumn('exist', F.lit(0))

        # join positive and negative group and select only patid and col columns
        demographics = positive.union(negative).withColumnRenamed('exist', col).select(['patid', col]).withColumnRenamed(col, colname)

        return demographics

    def flatten(self, file, spark, df, code_column='code', add_sep =True):
        """
        flatten the predictors that is important for utilisation in models like BEHRT
        df: the dataframe to be checked includes patid, age, eventdate, and code column
        code_column: columns name in df indicating the code column
        add_sep: flag to ask if SEP must be added as a separator token between visits in EHR
        """


        # in case age has not been calcualate, calculated below with the getAge helper.
        if 'age' not in df.columns:
            df = self.getAge(file, spark, df)

        df = df.groupby(['patid', 'eventdate']).agg(F.collect_list(code_column).alias(code_column),
                                                                  F.collect_list('age').alias('age'))


        # add sep value below. if requested. if not, then do not and continue with no separator token.
        if add_sep:
            df = EHR(df).array_add_element(code_column, 'SEP')
            extract_age = F.udf(lambda x: x[0])
            df = df.withColumn('age_temp', extract_age('age')).withColumn('age', F.concat(F.col('age'),
                                                                                                        F.array(F.col(
                                                                                                            'age_temp')))).drop('age_temp')
        w = Window.partitionBy('patid').orderBy('eventdate')
        df = df.withColumn(code_column, F.collect_list(code_column).over(w)) \
            .withColumn('age', F.collect_list('age').over(w)) \
            .groupBy('patid').agg(F.max(code_column).alias(code_column), F.max('age').alias('age'))
        df = EHR(df).array_flatten(code_column).array_flatten('age')

        return df


    def getAge(self, file, spark, df):
        """
            need to get age for the flatten functions to do their job. so we use patient to extract YOB.
            demographics: demographics dataframe includes patid, column indicates the baseline date
            df: the dataframe to be checked includes patid, age, eventdate, and code column
        """
        patient = tables.retrieve_patient(dir=file['patient'], spark=spark).select(['patid', 'yob'])
        df = df.join(patient, 'patid', 'inner')
        df = EHR(df).cal_age('eventdate', 'yob', year=False, name='age')
        return df


class BEHRTextraction(PredictorExtractorBase):
    def format_behrt(self, data, demorgraphics, col_entry='study_entry', col_yob='dob', age_col_name='age', year_col_name='year' , col_code='code', unique_in_months=None):
        """

        :param data: the records - e.g., med or disease records
        :param demorgraphics: the demo file for the work
        :param col_entry: the column to get the study entry variable (when to stop getting records)
        :param col_yob: the column to get the yob variable
        :param age_col_name: the column for the age variables
        :param col_code: the column for the code
        :param unique_per_year: too many repeats so delete all repeats within 1 year
        :return:  dataframe with behrt formatted data
        """
        # merge records and hf_cohort, and keep only records within the time period
        data = data.join(demorgraphics, 'patid', 'inner').dropna() \
            .where(F.col('eventdate') <= F.col(col_entry))
        data = data.dropDuplicates(['patid', 'eventdate', col_code]).dropna()

        # calulate age for each event
        data = EHR(data).cal_age('eventdate', col_yob, year=False, name=age_col_name)
        data = EHR(data).cal_year('eventdate', name=year_col_name)
        if unique_in_months:
            monthcal = F.udf(lambda x: int(((x.year * 12) + x.month - 1) / unique_in_months))
            data = data.withColumn('interval_cal', monthcal(F.col('eventdate')))
            window = Window.partitionBy([coldf for coldf in data.columns if coldf!='eventdate']).orderBy("eventdate")
            data  = data.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn").drop("interval_cal")

        data = self._format_sequence(data, col_code, age_col_name, year_col_name)
        return data

    def _format_sequence(self, data, col_code, col_age, col_year):
        """

        :param data: the records - e.g., med or disease records
        :param col_code: the column for the code
        :param col_age: the column for the age variables
        :param col_year: column for the year
        :return:
        """
        # group by date
        data = data.groupby(['patid', 'eventdate']).agg(F.collect_list(col_code).alias(col_code),
                                                        F.collect_list(col_age).alias(col_age), F.collect_list(col_year).alias(col_year), F.first('label').alias('label'))
        data = EHR(data).array_add_element(col_code, 'SEP')
        # add extra age to fill the gap of sep
        extract_age = F.udf(lambda x: x[0])
        data = data.withColumn('age_temp', extract_age(col_age)).withColumn(col_age, F.concat(F.col(col_age), F.array(
            F.col('age_temp')))).drop('age_temp')
        data = data.withColumn('year_temp', extract_age(col_year)).withColumn(col_year, F.concat(F.col(col_year), F.array(
            F.col('year_temp')))).drop('year_temp')


        # sort and merge code and age
        w = Window.partitionBy('patid').orderBy('eventdate')
        data = data.withColumn(col_code, F.collect_list(col_code).over(w)) \
            .withColumn(col_age, F.collect_list(col_age).over(w)) \
            .withColumn(col_year, F.collect_list(col_year).over(w)) \
            .groupBy('patid').agg(F.max(col_code).alias(col_code), F.max(col_age).alias(col_age), F.max(col_year).alias(col_year), F.first('label').alias('label'))
        data = EHR(data).array_flatten(col_code).array_flatten(col_age) .array_flatten(col_year).dropna()  # patid, code, age
        return data



class BEHRTCausal(PredictorExtractorBase):
    def format_behrt(self, data, demorgraphics, col_entry='study_entry', col_yob='dob', age_col_name='age', year_col_name='year', col_code='code',unique_per_year=False , explabel='explabel', exclcode = None):
        # merge records and hf_cohort, and keep only records within the time period
        data = data.join(demorgraphics, 'patid', 'inner').dropna() \
            .where(F.col('eventdate') <= F.col(col_entry))
        if exclcode is not None:
            # print(data.count())

            # print('exclude code in action: ' ,  exclcode)
            data = data.where(((F.col('eventdate') != F.col(col_entry)  ) | (F.col(col_code).isin(*exclcode) )  ))
            # print(data.count())

        # calulate age for each event
        data = EHR(data).cal_age('eventdate', col_yob, year=False, name=age_col_name)
        data = EHR(data).cal_year('eventdate', name='year')
        data = self._format_sequence(data, col_code, age_col_name, year_col_name,unique_per_year, explabel)
        return data

    def _format_sequence(self, data, col_code, col_age, col_year, unique_per_year, explabel):
        # group by date
        data = data.groupby(['patid', 'eventdate']).agg(F.collect_list(col_code).alias(col_code),
                                                        F.collect_list(col_age).alias(col_age), F.collect_list(col_year).alias(col_year), F.first('label').alias('label'), F.first(explabel).alias(explabel))
        data = EHR(data).array_add_element(col_code, 'SEP')
        # add extra age to fill the gap of sep
        extract_age = F.udf(lambda x: x[0])
        data = data.withColumn('age_temp', extract_age(col_age)).withColumn(col_age, F.concat(F.col(col_age), F.array(
            F.col('age_temp')))).drop('age_temp')
        data = data.withColumn('year_temp', extract_age(col_year)).withColumn(col_year, F.concat(F.col(col_year), F.array(
            F.col('year_temp')))).drop('year_temp')
        # sort and merge code and age
        if unique_per_year:
            data = data.dropDuplicates(["patid", col_code, col_year]).dropna()

        w = Window.partitionBy('patid').orderBy('eventdate')
        data = data.withColumn(col_code, F.collect_list(col_code).over(w)) \
            .withColumn(col_age, F.collect_list(col_age).over(w)) \
            .withColumn(col_year, F.collect_list(col_year).over(w)) \
            .groupBy('patid').agg(F.max(col_code).alias(col_code), F.max(col_age).alias(col_age),F.max(col_year).alias(col_year), F.first('label').alias('label') , F.first(explabel).alias(explabel))
        data = EHR(data).array_flatten(col_code).array_flatten(col_age) .array_flatten(col_year).dropna() # patid, code, age
        return data