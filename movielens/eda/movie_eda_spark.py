from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from pyspark import SparkContext


# sc = SparkContext()
# spark = SparkSession(sc)
spark = SparkSession.builder.getOrCreate()

# define schema for movies df
movies_schema = StructType(
    [
        StructField(name="movie_id", dataType=IntegerType(), nullable=False),
        StructField(name="title", dataType=StringType(), nullable=True),
        StructField(name="genres", dataType=StringType(), nullable=True),
    ]
)

ratings_schema = StructType(
    [
        StructField(name="user_id", dataType=IntegerType(), nullable=False),
        StructField(name="movie_id", dataType=IntegerType(), nullable=False),
        StructField(name="rating", dataType=IntegerType(), nullable=True),
        StructField(name="timestamp", dataType=IntegerType(), nullable=True),
    ]
)

# how to read csv into dataframe
movies_df = (
    spark.read.format("csv")
    .schema(movies_schema)
    .option("header", "false")
    .option("sep", "*")
    .option("path", "data/movies.txt")
    .load()
)
ratings_df = (
    spark.read.format("csv")
    .schema(ratings_schema)
    .option("header", "false")
    .option("sep", "*")
    .option("path", "data/ratings.txt")
    .load()
)

print(movies_df.show())
print(ratings_df.show())
