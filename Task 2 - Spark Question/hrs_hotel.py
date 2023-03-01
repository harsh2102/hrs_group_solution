import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkFiles

spark = SparkSession.Builder().appName('hrs').getOrCreate()


class UnitTestMethods(unittest.TestCase):

    '''These are few sample test cases '''

    def df_count_test(self, df):
        self.assertGreater(df.count(), 0, 'Count should be greater than 0')
        print('Test Case 1 : df count is greater than 0 - PASS')

    def df_col_value_test(self, df):
        for row in df.collect():
            self.assertIn(row['with_family_breakfast'], ['Yes', 'No'], 'ERROR')
        print("""Test Case 2 : `with_family_breakfast` column must contains 'Yes' OR 'NO' - PASS""")


url = 'https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-02-11/hotels.csv'
spark.sparkContext.addFile(url)
hotel_df = spark.read.format('csv')\
    .option('path', SparkFiles.get("hotels.csv"))\
    .option('header', True)\
    .option('InferSchema', True)\
    .load()

'''First Unit Test Case'''
UnitTestMethods().df_count_test(hotel_df)


def get_filterd_data(df, col_name, val):
    '''This function retunred the filtered data'''
    '''As mentioned in task we need to extarct the booking where Market Segment is Tour Operators(TO) '''
    df = df.filter(col(col_name).isin(val))
    return df


def column_transformation(df):
    '''This function add/tranform the column which are mentioned in the task'''

    '''Here Adding the arrival_date column and converting it to proper date format'''
    df = df.withColumn('arrival_date', concat_ws('-', col('arrival_date_year'),
                       col('arrival_date_month'), col('arrival_date_day_of_month')))
    df = df.withColumn('arrival_date', to_date(from_unixtime(
        unix_timestamp(col('arrival_date'), "yyyy-MMMM-d"), 'yyyy-MM-dd')))

    '''Here created a temporary column to calculate the total time spent to get depature_date'''
    df = df.withColumn('total_spent_time', col('stays_in_weekend_nights').cast(
        'int')+col('stays_in_week_nights').cast('int'))
    df = df.withColumn('departure_date', date_add(
        col('arrival_date'), col('total_spent_time'))).drop('total_spent_time')

    '''Adding the with_family_breakfast column if there is any babies or children'''
    df = df.withColumn('with_family_breakfast', when(
        (col('children')+col('babies')) > 0, 'Yes').otherwise('No'))

    return df


filter_hotel_df = get_filterd_data(
    hotel_df, 'market_segment', ['Offline TA/TO'])
final_hotel_df = column_transformation(filter_hotel_df)

'''Second Unit Test Case'''
UnitTestMethods().df_col_value_test(final_hotel_df)

'''Saving dataframe as parquet'''
'''We can also repartition the dataframe so that we can take advantage of parallelism'''
'''We can also control the max file size or max Record Per File'''
final_hotel_df.write.format('parquet').mode(
    'overwrite').save('./ETL_hotel_df', header=True)
