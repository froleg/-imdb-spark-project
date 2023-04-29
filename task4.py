from columns import *
import pyspark.sql.functions as f

def task4 (df_princ, df_name, df_title, path_to_save):
    """Get names of people, corresponding movies/series and characters they
    played in those films
    Args:
        df_princ: principals dataframe
        df_name: name dataframe
        df_title: title dataframe
        path_to_save: path to save dataframe
    Returns:
        out_df: tramsformed dataframe
    """
    df_princ=df_princ.drop(job, ordering).filter(f.col(category)=='actor')
    df_title=df_title.drop(originalTitle,
                           isAdult,
                           startYear,
                           endYear,
                           runtimeMinutes,
                           genres).filter((f.col(titleType)=='movie') |
                                          (f.col(titleType)=='tvMovie') |
                                           (f.col(titleType)=='tvSeries') |
                                           (f.col(titleType)=='tvMiniSeries'))
    out_df = df_name.join(df_princ, on=nconst, how = 'inner').drop(birthYear,
                                                                   deathYear,
                                                                   primaryProfession,
                                                                   knownForTitles)
    out_df = out_df.join(df_title,on=tconst, how = 'inner').select(f.col(primaryName),
                                                                   f.col(primaryTitle),
                                                                   f.col(characters),)
    out_df.write.csv(path_to_save, header=True, mode='overwrite')
    return out_df
