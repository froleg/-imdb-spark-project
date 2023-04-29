import pyspark.sql.types as t

path_ratings = 'data/title.ratings.tsv.gz'
path_name_basics = 'data/name.basics.tsv.gz'
path_title_basics = 'data/title.basics.tsv.gz'
path_akas = 'data/title.akas.tsv.gz'
path_crew = 'data/title.crew.tsv.gz'
path_episode = 'data/title.episode.tsv.gz'
path_principals ='data/title.principals.tsv.gz'
path_task1 = 'saved_result/task1'
path_task2 = 'saved_result/task2'
path_task3 = 'saved_result/task3'
path_task4 = 'saved_result/task4'
path_task5 = 'saved_result/task5'
path_task6 = 'saved_result/task6'
path_task7 = 'saved_result/task7'
path_task8 = 'saved_result/task8'

akas_schema = t.StructType([t.StructField('titleId',t.StringType(),False),
                    t.StructField('ordering',t.IntegerType(),True),
                    t.StructField('title',t.StringType(),True),
                    t.StructField('region',t.StringType(),True),
                    t.StructField('language',t.StringType(),True),
                    t.StructField('types',t.StringType(),True),
                    t.StructField('attributes',t.StringType(),True),
                    t.StructField('isOriginalTitle',t.BooleanType(),True)
                    ])

title_basics_schema = t.StructType([t.StructField('tconst',t.StringType(),False),
                    t.StructField('titleType',t.StringType(),True),
                    t.StructField('primaryTitle',t.StringType(),True),
                    t.StructField('originalTitle',t.StringType(),True),
                    t.StructField('isAdult',t.IntegerType(),True),
                    t.StructField('startYear',t.IntegerType(),True),
                    t.StructField('endYear',t.IntegerType(),True),
                    t.StructField('runtimeMinutes',t.IntegerType(),True),
                    t.StructField('genres',t.StringType(),True)
                    ])
crew_schema = t.StructType([t.StructField('tconst',t.StringType(),False),
                    t.StructField('directors',t.StringType(),True),
                    t.StructField('writers',t.StringType(),True),
                    ])
episode_schema=t.StructType([t.StructField('tconst',t.StringType(),False),
                    t.StructField('parentTconst',t.StringType(),True),
                    t.StructField('seasonNumber',t.IntegerType(),True),
                    t.StructField('episodeNumber',t.IntegerType(),True),])
principals_schema=t.StructType([t.StructField('tconst',t.StringType(),False),
                    t.StructField('ordering',t.IntegerType(),True),
                    t.StructField('nconst',t.StringType(),True),
                    t.StructField('category',t.StringType(),True),
                    t.StructField('job',t.StringType(),True),
                    t.StructField('characters',t.StringType(),True),
                    ])
ratings_schema=t.StructType([t.StructField('tconst',t.StringType(),False),
                    t.StructField('averageRating',t.FloatType(),True),
                    t.StructField('numVotes',t.IntegerType(),True),])
name_basics_schema = t.StructType([t.StructField('nconst',t.StringType(),False),
                    t.StructField('primaryName',t.StringType(),True),
                    t.StructField('birthYear',t.IntegerType(),True),
                    t.StructField('deathYear',t.IntegerType(),True),
                    t.StructField('primaryProfession',t.StringType(),True),
                    t.StructField('knownForTitles',t.StringType(),True)])
