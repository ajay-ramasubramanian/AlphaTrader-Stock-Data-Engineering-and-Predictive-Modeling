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

    # Check for unique combinations of artist_id and genre_id
    expectation_config_unique = ExpectationConfiguration(
        expectation_type="expect_multicolumn_values_to_be_unique",
        kwargs={"column_list": ["artist_id", "genre_id"]}
    )

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
    expectation_type="expect_column_values_to_match_strftime_format",
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

    suite.add_expectations([
    expectation_config_unique,
    expectation_config_non_null_artist,
    expectation_config_non_null_genre,
    expectation_config_artist_format,
    expectation_config_genre_type,
    expectation_config_column_count
])
    
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

    # Expect genre_id to be of type int64
    expectation_config_genre_id_type = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": "genre_id", "type_": "int64"}
    )

    # Expect genre to be of type str
    expectation_config_genre_type = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": "genre", "type_": "str"}
    )

    # Expect the table to have exactly 2 columns
    expectation_config_column_count = ExpectationConfiguration(
        expectation_type="expect_table_column_count_to_equal",
        kwargs={"value": 2}
    )

    suite.add_expectation([
        expectation_config_non_null_genre_id,
        expectation_config_non_null_genre,
        expectation_config_unique_genre_id,
        expectation_config_unique_genre,
        expectation_config_genre_id_type,
        expectation_config_genre_id_type,
        expectation_config_genre_type,
        expectation_config_column_count
    ])

    context.save_expectation_suite(suite)



def create_time_table_expectation_suite():
    
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


    
    suite.add_expectation(expectation_configuration=expectation_config_not_null)
    
    # Save the updated suite
    context.save_expectation_suite(suite)
    
    return suite



def artist_genre_bridge_expectation_suite():
    # Initialize context
    context = FileDataContext.create(project_root_dir=".")
    
    # Suite name for the time table
    suite_name = "artist_genre_bridge"
    
    # Create or extend the base suite
    suite = create_base_expectation_suite(context, suite_name)


def artist_genre_bridge_expectation_suite():
    # Initialize context
    context = FileDataContext.create(project_root_dir=".")
    
    # Suite name for the time table
    suite_name = "artist_genre_bridge"
    
    # Create or extend the base suite
    suite = create_base_expectation_suite(context, suite_name)


def artist_genre_bridge_expectation_suite():
    # Initialize context
    context = FileDataContext.create(project_root_dir=".")
    
    # Suite name for the time table
    suite_name = "artist_genre_bridge"
    
    # Create or extend the base suite
    suite = create_base_expectation_suite(context, suite_name)


def artist_genre_bridge_expectation_suite():
    # Initialize context
    context = FileDataContext.create(project_root_dir=".")
    
    # Suite name for the time table
    suite_name = "artist_genre_bridge"
    
    # Create or extend the base suite
    suite = create_base_expectation_suite(context, suite_name)


def artist_genre_bridge_expectation_suite():
    # Initialize context
    context = FileDataContext.create(project_root_dir=".")
    
    # Suite name for the time table
    suite_name = "artist_genre_bridge"
    
    # Create or extend the base suite
    suite = create_base_expectation_suite(context, suite_name)


def artist_genre_bridge_expectation_suite():
    # Initialize context
    context = FileDataContext.create(project_root_dir=".")
    
    # Suite name for the time table
    suite_name = "artist_genre_bridge"
    
    # Create or extend the base suite
    suite = create_base_expectation_suite(context, suite_name)


def artist_genre_bridge_expectation_suite():
    # Initialize context
    context = FileDataContext.create(project_root_dir=".")
    
    # Suite name for the time table
    suite_name = "artist_genre_bridge"
    
    # Create or extend the base suite
    suite = create_base_expectation_suite(context, suite_name)


def artist_genre_bridge_expectation_suite():
    # Initialize context
    context = FileDataContext.create(project_root_dir=".")
    
    # Suite name for the time table
    suite_name = "artist_genre_bridge"
    
    # Create or extend the base suite
    suite = create_base_expectation_suite(context, suite_name)


# Run the function to create the expectation suite
def create_expectation_suites():
    create_time_table_expectation_suite()
    artist_genre_bridge_expectation_suite()

