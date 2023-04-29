from columns import *
import pyspark.sql.functions as f

def read_df(session, path, schema):
    """ Read data to dataframe from csv

    Args:
        session: spark session
        path: path to a csv file
        schema: datatset schema

    Returns:
        dataframe
    """
    df = session.read.csv(path, header=True, sep='\t',schema=schema,nullValue=r'\N')
    return df

def read_name_basics(session, path, schema):
    """ Read and base prepare data for name basics dataset
    Args:
        session: spark session
        path: path to a csv file
        schema: datatset schema

    Returns:
        dataframe
    """
    df=read_df(session, path, schema)
    df=df.select('*',
                    f.split(f.col(primaryProfession),',').alias('pP'),
                    f.split(f.col(knownForTitles),',').alias('kFT')).drop(knownForTitles,primaryProfession)
    df=df.withColumnRenamed('kFT',knownForTitles)
    df=df.withColumnRenamed('pP',primaryProfession)
    return df

def read_crew(session, path, schema):
    """ Read and base prepare data for crew dataset
    Args:
        session: spark session
        path: path to a csv file
        schema: datatset schema

    Returns:
        dataframe
    """
    df=read_df(session, path, schema)
    df=df.select('*',
                            f.split(f.col(directors),',').alias('dr'),
                            f.split(f.col(writers),',').alias('wr')).drop(directors,writers)
    df=df.withColumnRenamed('dr',directors)
    df=df.withColumnRenamed('wr',writers)
    return df

def read_akas(session, path, schema):
    """ Read and base prepare data for akas dataset
    Args:
        session: spark session
        path: path to a csv file
        schema: datatset schema

    Returns:
        dataframe
    """
    df=read_df(session, path, schema)
    df=df.select('*',
                    f.split(f.col(types),',').alias('ty'),
                    f.split(f.col(attributes),',').alias('att')).drop(types,attributes)
    df=df.withColumnRenamed('ty',types)
    df=df.withColumnRenamed('att',attributes)
    return df

def read_title_basics(session, path, schema):
    """ Read and base prepare data for title basics dataset
    Args:
        session: spark session
        path: path to a csv file
        schema: datatset schema

    Returns:
        dataframe
    """
    df=read_df(session, path, schema)
    df=df.select('*',
                    f.split(f.col(genres),',').alias('gen')).drop(genres)
    df=df.withColumnRenamed('gen',genres)
    return df
