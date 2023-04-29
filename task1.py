from columns import *
import pyspark.sql.functions as f

def task1 (in_df, path_to_save):
    """Get all titles of series/movies etc. that are available in Ukrainian
    Args:
        in_df: akas dataframe
        path_to_save: path to save dataframe
    Returns:
        out_df: tramsformed dataframe
    """
    out_df=in_df.filter(f.col(region)=='UA').select(f.col(title))
    out_df.write.csv(path_to_save, header=True, mode='overwrite')
    return out_df
