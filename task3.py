from columns import *
import pyspark.sql.functions as f

def task3 (in_df, path_to_save):
    """Get titles of all movies that last more than 2 hours
    Args:
        in_df: akas dataframe
        path_to_save: path to save dataframe
    Returns:
        out_df: tramsformed dataframe
    """
    out_df=in_df.filter((f.col(titleType)=='movie') & (f.col(runtimeMinutes)>120)).select(f.col(originalTitle))
    out_df.write.csv(path_to_save, header=True, mode='overwrite')
    return out_df
