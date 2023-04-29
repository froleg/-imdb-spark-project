from columns import *
import pyspark.sql.functions as f
import pyspark.sql.types as t

def task8(df_ratings, df_title, path_to_save):
    """Get 10 titles of the most popular movies/series etc. by each genre

    Args:
        df_ratings: ratings dataframe
        df_title: title dataframe
        path_to_save: path to save dataframe
    Returns:
        out_df: tramsformed dataframe
    """
    df_title=df_title.select(tconst,
                             primaryTitle,
                             genres)
    df_title=df_title.withColumn('Genres2', f.concat_ws(',', f.col(genres)))
    genres_list=df_title.rdd.map(lambda x: x.Genres2).collect()
    genres_list=[x.split(',') for x in genres_list]
    gl=[]
    for x in genres_list:
        gl.extend(x)
    genres_set=set(gl)
    genres_set.remove('')
    join_df = df_title.join(df_ratings, on=tconst, how = 'inner').drop('Genres2')
    out_df=(join_df.select(primaryTitle,
                           averageRating).where(
                           f.array_contains(f.col(genres),list(genres_set)[0]))
                            .orderBy(averageRating, ascending=False).limit(10)
                            .dropDuplicates())
    out_df=out_df.withColumn('genre',f.lit(list(genres_set)[0]))
    for i in range(1, len(genres_set)):
        genre_df=(join_df.select(primaryTitle,
                           averageRating).where(
                           f.array_contains(f.col(genres),list(genres_set)[i]))
                            .orderBy(averageRating, ascending=False).limit(10)
                            .dropDuplicates())
        genre_df=genre_df.withColumn('genre',f.lit(list(genres_set)[i]))
        out_df=out_df.union(genre_df)
    out_df.write.csv(path_to_save, header=True, mode='overwrite')
    print(len(genres_set))
    return out_df
