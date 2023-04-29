from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
#import pyspark.sql.types as t
import pyspark.sql.functions as f
from settings import *
from columns import *
from read_write import *
from task1 import *
from task2 import *
from task3 import *
from task4 import *
from task5 import *
from task6 import *
from task7 import *
from task8 import *

def main():
    spark_session=(SparkSession.builder
                                .master('local')
                                .appName('task app')
                                .config(conf=SparkConf())
                                .getOrCreate())

    df1 = read_akas(spark_session, path_akas, akas_schema)
    task1(df1, path_task1)
    df2 = read_name_basics(spark_session, path_name_basics, name_basics_schema)
    task2(df2, path_task2)
    df3 = read_title_basics(spark_session, path_title_basics, title_basics_schema)
    task3(df3, path_task3)
    df4 = read_df(spark_session, path_principals, principals_schema)
    task4(df4, df2, df3, path_task4)
    task5(df1, df3, path_task5)
    df5 = read_df(spark_session, path_episode, episode_schema)
    task6(df5, df3, path_task6)
    df6 = read_df(spark_session, path_ratings, ratings_schema)
    task7(df6, df3, path_task7)
    task8(df6, df3, path_task8)

if __name__ == '__main__':
    main()
