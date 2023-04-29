from columns import *
import pyspark.sql.functions as f

def task2 (in_df, path_to_save):
    """Get the list of peopleʼs names, who were born in the 19th century
    Args:
        in_df: name basics dataframe
        path_to_save: path to save dataframe
    Returns:
        out_df: tramsformed dataframe
    """
    out_df=in_df.filter(f.col(birthYear).between(1800,1899)).select(f.col(primaryName))
    out_df.write.csv(path_to_save, header=True, mode='overwrite')
    return out_df
