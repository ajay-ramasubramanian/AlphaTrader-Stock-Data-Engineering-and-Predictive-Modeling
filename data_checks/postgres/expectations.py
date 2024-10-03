from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import FileDataContext
from great_expectations.data_context import get_context
import pandas as pd
import os
import sys
import site

def create_base_expectation_suite(suite_name):

    context = get_context(project_root_dir=".")

    # Create a base suite with common checks
    suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    
    # Check for empty table
    expectation_config_table_not_empty = ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 1}
    )

    # Check for minimum of 2 columns in table
    expectation_config_min_columns = ExpectationConfiguration(
        expectation_type="expect_table_column_count_to_be_between",
        kwargs={"min_value": 2}
    )
    
    # Add the expectation to the suite
    suite.add_expectation(expectation_configuration=expectation_config_table_not_empty)
    suite.add_expectation(expectation_configuration=expectation_config_min_columns)

    # Save the suite
    context.save_expectation_suite(suite)
    
    return suite, context

def dim_artist_expectation_suite():
    
    # Suite name for the time table
    suite_name = "dim_artist_suite"
    
    # Create or extend the base suite
    suite, context = create_base_expectation_suite(suite_name)

    expectation_config_unique_played_at = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_unique",
    kwargs={"column": "artist_id"}
    )

    expectation_config_not_null = [
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "artist_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "artist_name"}
    )
]
    
    expectation_config_valid_popularity = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_between",
    kwargs={"column": "artist_popularity", "min_value": 0, "max_value": 100}
)
    
    expectation_config_check_format = [
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": "artist_followers", "type_": "INTEGER"}
    )
]
    
    for expectation in expectation_config_not_null:
        suite.add_expectation(expectation)

    for expectation in expectation_config_check_format:
        suite.add_expectation(expectation)

    suite.add_expectation(expectation_config_unique_played_at)
    suite.add_expectation(expectation_config_valid_popularity)

    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")


def dim_artist_genre_bridge_expectation_suite():
    
    # Suite name for the time table
    suite_name = "dim_artist_genre_bridge_suite"
    
    # Create or extend the base suite
    suite, context = create_base_expectation_suite(suite_name)

    # Check for non-null values for genre_id
    expectation_config_non_null_artist = ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={"column": "artist_id"}  
    )

    # Check for non-null values for genre_id
    expectation_config_non_null_genre = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "genre_id"}
    )

    # Check for artist_id datatype
    expectation_config_artist_format = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_of_type",
    kwargs={"column": "artist_id", "type_": "VARCHAR"}
    )

    # Check for genre_id datatype
    expectation_config_genre_type = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": "genre_id", "type_": "INTEGER"}
    )

    # Expect the table to have exactly 2 columns
    expectation_config_column_count = ExpectationConfiguration(
        expectation_type="expect_table_column_count_to_equal",
        kwargs={"value": 2}
    )

    suite.add_expectation(expectation_config_non_null_artist)
    suite.add_expectation(expectation_config_non_null_genre)
    suite.add_expectation(expectation_config_artist_format)
    suite.add_expectation(expectation_config_genre_type)
    suite.add_expectation(expectation_config_column_count)
    
    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")


def dim_album_expectation_suite():

    # Suite name for the time table
    suite_name = "dim_album_suite"
    
    # Create or extend the base suite
    suite, context = create_base_expectation_suite(suite_name)


    expectation_config_not_null = [
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "album_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "album_name"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "artist_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "artist_name"}
    )
]
    
    # expectation_config_artist_foreign_key = ExpectationConfiguration(
    #         expectation_type="expect_column_values_to_be_in_set",
    #         kwargs={
    #             "column": "artist_id",
    #             "value_set": {"query": "SELECT DISTINCT artist_id FROM dim_artist"},
    #             "result_format": "COMPLETE"
    #         }
    #     )

    expectation_config_valid_album_type = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={"column": "album_type", "value_set": ["album", "single", "compilation"]}
)

    for expectation in expectation_config_not_null:
        suite.add_expectation(expectation)

    suite.add_expectation(expectation_config_valid_album_type)
    # suite.add_expectation(expectation_config_artist_foreign_key)
    
    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")
    

def dim_genre_expectation_suite():

    suite_name = "dim_genre_suite"

    suite, context = create_base_expectation_suite(suite_name)

    expectation_config_non_null_genre_id = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "genre_id"}
    )

    # Expect genre to not be null
    expectation_config_non_null_genre = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "genre"}
    )

    # Expect genre_id to be unique
    expectation_config_unique_genre_id = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "genre_id"}
    )

    # Expect genre to be unique
    expectation_config_unique_genre = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "genre"}
    )

    # Expect the table to have exactly 2 columns
    expectation_config_column_count = ExpectationConfiguration(
        expectation_type="expect_table_column_count_to_equal",
        kwargs={"value": 2}
    )

    suite.add_expectation(expectation_config_non_null_genre_id)
    suite.add_expectation(expectation_config_non_null_genre)
    suite.add_expectation(expectation_config_unique_genre_id)
    suite.add_expectation(expectation_config_unique_genre)
    suite.add_expectation(expectation_config_column_count)

    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")
    

def dim_time_expectation_suite():

    suite_name = "dim_time_suite"

    suite, context = create_base_expectation_suite(suite_name)

    expectation_config_unique = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "date_id"}
    )
        
    expectation_config_not_null = [
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "date_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "year"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "month"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "day"}
    )
]
    
    expectation_config_valid_range = [
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "year", "min_value": 1900, "max_value": pd.Timestamp.now().year}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "month", "min_value": 1, "max_value": 12}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "day", "min_value": 1, "max_value": 31}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "hour", "min_value": 0, "max_value": 23}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "minute", "min_value": 0, "max_value": 59}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "second", "min_value": 0, "max_value": 59}
    )
]
    
    for expectation in expectation_config_not_null:
        suite.add_expectation(expectation)
    
    for expectation in expectation_config_valid_range:
        suite.add_expectation(expectation)
    
    suite.add_expectation(expectation_config_unique)

    # Save the updated suite
    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")


def dim_track_expectation_suite():

    suite_name = "dim_track_suite"

    suite, context = create_base_expectation_suite(suite_name)

    expectation_config_unique_track_id = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_unique",
    kwargs={"column": "track_id"}
)

    expectation_config_not_null = [
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "track_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "track_name"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "artist_name"}
    )
]
    
    expectation_config_valid_popularity = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_between",
    kwargs={"column": "track_popularity", "min_value": 0, "max_value": 100}
)

    expectation_config_valid_date_format = ExpectationConfiguration(
    expectation_type="expect_column_values_to_match_regex",
    kwargs={"column": "ingested_on", "regex": r"^\d{14}$"}  
)
    
    for expectation in expectation_config_not_null:
        suite.add_expectation(expectation)
    
    suite.add_expectation(expectation_config_unique_track_id)
    suite.add_expectation(expectation_config_valid_popularity)
    suite.add_expectation(expectation_config_valid_date_format)

    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")


def fact_liked_songs_expectation_suite():

    suite_name = "fact_liked_songs_suite"

    suite, context = create_base_expectation_suite(suite_name)

    expectation_config_non_null = [
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "like_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "artist_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "track_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "time_id"}
    )
]

    expectation_config_unique = [
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "like_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "track_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "time_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "added_at"}
    )]

    
    for expectation in expectation_config_non_null:
        suite.add_expectation(expectation)

    for expectation in expectation_config_unique:
        suite.add_expectation(expectation)

    # expectation_config_referential_integrity = [
    #     ExpectationConfiguration(
    #         expectation_type="expect_column_values_to_be_in_set",
    #         kwargs={
    #             "column": "artist_id",
    #             "value_set": {"query": "SELECT DISTINCT artist_id FROM dim_artist"},
    #             "result_format": "COMPLETE"
    #         }
    #     ),
    #     ExpectationConfiguration(
    #         expectation_type="expect_column_values_to_be_in_set",
    #         kwargs={
    #             "column": "album_id",
    #             "value_set": {"query": "SELECT DISTINCT album_id FROM dim_album"},
    #             "result_format": "COMPLETE"
    #         }
    #     ),
    #     ExpectationConfiguration(
    #         expectation_type="expect_column_values_to_be_in_set",
    #         kwargs={
    #             "column": "track_id",
    #             "value_set": {"query": "SELECT DISTINCT track_id FROM dim_track"},
    #             "result_format": "COMPLETE"
    #         }
    #     ),
    #     ExpectationConfiguration(
    #         expectation_type="expect_column_values_to_be_in_set",
    #         kwargs={
    #             "column": "time_id",
    #             "value_set": {"query": "SELECT DISTINCT date_id FROM dim_time"},
    #             "result_format": "COMPLETE"
    #         }
    #     )
    # ]
    
    # for expectation in expectation_config_referential_integrity:
    #     suite.add_expectation(expectation)
    
    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")
    
    return suite



def fact_recently_played_expectation_suite():

    suite_name = "fact_recently_played_suite"

    suite, context = create_base_expectation_suite(suite_name)

    expectation_config_unique_played_at = [
    ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_unique",
    kwargs={"column": "played_at"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "recents_id"}
    )
]
    
    expectation_config_not_null = [
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "track_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "track_name"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "artist_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "recents_id"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "played_at"}
    )
]
    
    expectation_config_valid_popularity = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_between",
    kwargs={"column": "popularity", "min_value": 0, "max_value": 100}
)
    
    expectation_config_valid_ingested_format = ExpectationConfiguration(
    expectation_type="expect_column_values_to_match_regex",
    kwargs={"column": "ingested_on", "regex": r"^\d{14}$"}  
)

    for expectation in expectation_config_unique_played_at:
        suite.add_expectation(expectation)

    for expectation in expectation_config_not_null:
        suite.add_expectation(expectation)
    
    suite.add_expectation(expectation_config_valid_popularity)
    suite.add_expectation(expectation_config_valid_ingested_format)

    # expectation_config_referential_integrity = [
    #     ExpectationConfiguration(
    #         expectation_type="expect_column_values_to_be_in_set",
    #         kwargs={
    #             "column": "artist_id",
    #             "value_set": {"query": "SELECT DISTINCT artist_id FROM dim_artist"},
    #             "result_format": "COMPLETE"
    #         }
    #     ),
    #     ExpectationConfiguration(
    #         expectation_type="expect_column_values_to_be_in_set",
    #         kwargs={
    #             "column": "album_id",
    #             "value_set": {"query": "SELECT DISTINCT album_id FROM dim_album"},
    #             "result_format": "COMPLETE"
    #         }
    #     ),
    #     ExpectationConfiguration(
    #         expectation_type="expect_column_values_to_be_in_set",
    #         kwargs={
    #             "column": "track_id",
    #             "value_set": {"query": "SELECT DISTINCT track_id FROM dim_track"},
    #             "result_format": "COMPLETE"
    #         }
    #     )
    # ]
    
    # for expectation in expectation_config_referential_integrity:
    #     suite.add_expectation(expectation)
    
    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")
    
    return suite


def create_postgres_expectation_suites():
    dim_artist_expectation_suite()
    dim_album_expectation_suite()
    dim_time_expectation_suite()
    dim_track_expectation_suite()
    dim_artist_genre_bridge_expectation_suite()
    dim_genre_expectation_suite()
    fact_liked_songs_expectation_suite()
    fact_recently_played_expectation_suite()


create_postgres_expectation_suites()