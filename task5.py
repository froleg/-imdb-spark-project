from columns import *
import pyspark.sql.functions as f

def task5( df_akas, df_title, path_to_save):
    """Get information about how many adult movies/series etc. there are per
    region. Get the top 100 of them from the region with the biggest count to
    the region with the smallest one
    Args:
        df_akas: title akas dataframe
        df_title: title dataframe
        path_to_save: path to save dataframe
    Returns:
        out_df: tramsformed dataframe
    """
    df_akas=df_akas.filter(f.col(region).isNotNull()).select(f.col(titleId).alias('tconst'),
                                                             f.col(region)).dropDuplicates()
    df_title=df_title.select(tconst,
                           primaryTitle,
                           isAdult).filter(f.col(isAdult)==1)
    out_df = df_akas.join(df_title,on=tconst, how = 'inner')
    out_df = out_df.groupBy(region).count().orderBy('count', ascending=False).limit(100)
    out_df.write.csv(path_to_save, header=True, mode='overwrite')
    return out_df
