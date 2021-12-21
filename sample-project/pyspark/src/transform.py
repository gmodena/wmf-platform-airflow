"""
A boilerplate Spark job for sample-project
"""

import datetime
from pyspark.sql import SparkSession

# Define our query to find a count of all new article on a wiki per day
SQL_QUERY = """
    SELECT database, count(*)
    FROM event.mediawiki_page_create
    WHERE year={} AND month={} AND day={} AND database='{}'
    GROUP BY database  
"""


def get_running_date(date_object=datetime.datetime.now().date(), num_days=1):
    """Function to provide running date for the sample project process.

    If no parameters are passed then today - 1 day will be returned.

    Args:
        date_object (datetime, optional). Date to run . Defaults to today.
        num_days (int, optional). Number of days to subtract from the date. Defaults to 1.
    Returns:
        datetime object
    """
    return date_object - datetime.timedelta(num_days)


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    running_date = get_running_date()

    # Execute the query
    pandas_df = spark.sql(SQL_QUERY.format(running_date.year,
                                           running_date.month,
                                           running_date.day,
                                           'enwiki'))

    # Rename the columns and return the DF
    pandas_df = pandas_df.withColumnRenamed('domain', 'DOMAIN').withColumnRenamed('count(1)', 'COUNT_OF_NEW_PAGES')

    spark.stop()
