from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import FileDataContext
from great_expectations.data_context import get_context
import pandas as pd
import os
import sys
import site

sys.path.extend(site.getsitepackages())
sys.path.append(os.path.abspath(os.path.join('..', 'time_table')))

def create_base_expectation_suite(suite_name):

    context = get_context()

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


def artist_genre_bridge_expectation_suite():
    
    # Suite name for the time table
    suite_name = "artist_genre_bridge_suite"
    
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
    kwargs={"column": "artist_id", "type_": "str"}
    )

    # Check for genre_id datatype
    expectation_config_genre_type = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": "genre_id", "type_": "int64"}
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


def genre_table_expectation_suite():
    
    # Suite name for the time table
    suite_name = "genre_table_suite"
    
    # Create or extend the base suite
    suite, context = create_base_expectation_suite(suite_name)

    # Expect genre_id to not be null
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


def time_table_expectation_suite():
    
    # Suite name for the time table
    suite_name = "time_table_suite"
    
    # Create or extend the base suite
    suite, context = create_base_expectation_suite(suite_name)
    
    # Check for unique column values
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
    


def all_tracks_expectation_suite():
    
    # Suite name for the time table
    suite_name = "all_tracks_suite"
    
    # Create or extend the base suite
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



def artist_album_expectation_suite():
    
    # Suite name for the time table
    suite_name = "artist_albums_suite"
    
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
    
    expectation_config_valid_ingested_on = ExpectationConfiguration(
    expectation_type="expect_column_values_to_match_regex",
    kwargs={"column": "ingested_on", "regex": r"^\d{14}$"} 
)

    expectation_config_valid_album_type = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={"column": "album_type", "value_set": ["album", "single", "compilation"]}
)

    for expectation in expectation_config_not_null:
        suite.add_expectation(expectation)

    suite.add_expectation(expectation_config_valid_ingested_on)
    suite.add_expectation(expectation_config_valid_album_type)

    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")


def following_artist_expectation_suite():
    
    # Suite name for the time table
    suite_name = "following_artists_suite"
    
    # Create or extend the base suite
    suite, context = create_base_expectation_suite(suite_name)

    expectation_config_non_null_follow_id = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "follow_id"}
    )

    expectation_config_non_null_artist_id = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "artist_id"}
    )

    # Expect genre_id to be unique
    expectation_config_unique_follow_id = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "follow_id"}
    )

    # Expect genre to be unique
    expectation_config_unique_artist_id = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "artist_id"}
    )

    # Expect the table to have exactly 2 columns
    expectation_config_column_count = ExpectationConfiguration(
        expectation_type="expect_table_column_count_to_equal",
        kwargs={"value": 3}
    )

    suite.add_expectation(expectation_config_non_null_follow_id)
    suite.add_expectation(expectation_config_non_null_artist_id)
    suite.add_expectation(expectation_config_unique_follow_id)
    suite.add_expectation(expectation_config_unique_artist_id)
    suite.add_expectation(expectation_config_column_count)

    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")


def liked_songs_expectation_suite():
    
    # Suite name for the time table
    suite_name = "liked_songs_suite"
    
    # Create or extend the base suite
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
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "added_at"}
    )]

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
    
    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")



def recent_plays_expectation_suite():
    
    # Suite name for the time table
    suite_name = "recent_plays_suite"
    
    # Create or extend the base suite
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

    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")




def related_artists_expectation_suite():
    
    # Suite name for the time table
    suite_name = "related_artists_suite"
    
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
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "genres"}
    )
]
    
    expectation_config_valid_popularity = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_between",
    kwargs={"column": "artist_popularity", "min_value": 0, "max_value": 100}
)
    
    expectation_config_check_format = [
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": "artist_followers", "type_": "int64"}
    ),
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": "genres", "type_": "list"}
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


def saved_playlist_expectation_suite():
    
    # Suite name for the time table
    suite_name = "saved_playlist_suite"
    
    # Create or extend the base suite
    suite, context = create_base_expectation_suite(suite_name)

    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")


def top_artists_expectation_suite():
    
    # Suite name for the time table
    suite_name = "top_artists_suite"
    
    # Create or extend the base suite
    suite, context = create_base_expectation_suite(suite_name)

    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")


def top_songs_expectation_suite():
    
    # Suite name for the time table
    suite_name = "top_songs_suite"
    
    # Create or extend the base suite
    suite, context = create_base_expectation_suite(suite_name)

    context.save_expectation_suite(suite)
    print(f"Added {suite_name} to context!!")


# Run the function to create the expectation suites
def create_expectation_suites():
    artist_genre_bridge_expectation_suite()
    genre_table_expectation_suite()
    time_table_expectation_suite()
    all_tracks_expectation_suite()
    artist_album_expectation_suite()
    following_artist_expectation_suite()
    liked_songs_expectation_suite()
    recent_plays_expectation_suite()
    related_artists_expectation_suite()
    saved_playlist_expectation_suite()
    top_artists_expectation_suite()
    top_songs_expectation_suite()


create_expectation_suites()