from columns import *
import pyspark.sql.functions as f

def task6(df_episode, df_title, path_to_save):
    """Get information about how many episodes in each TV Series. Get the top
    50 of them starting from the TV Series with the biggest quantity of
    episodes

    Args:
        df_episode: episode dataframe
        df_title: title dataframe
        path_to_save: path to save dataframe

    Returns:
        out_df: tramsformed dataframe
    """
    df_title=df_title.select(f.col(tconst).alias(parentTconst),
                             primaryTitle).filter((f.col(titleType)=='tvSeries'))
    out_df = df_title.join(df_episode,on=parentTconst, how = 'inner')
    out_df = out_df.groupBy(primaryTitle).count().orderBy('count', ascending=False).limit(50)
    out_df.write.csv(path_to_save, header=True, mode='overwrite')
    return out_df
