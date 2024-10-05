import great_expectations as gx
from great_expectations.dataset import PandasDataset
from great_expectations.data_context import FileDataContext
from great_expectations.data_context import get_context
import pandas as pd

def validate_expectations(df: pd.DataFrame, expectation_suite_name):

    ## run only the first time to make a gx directory.
    # context = FileDataContext.create(project_root_dir=".")

    context = get_context(project_root_dir="/opt/airflow/projects")

    # Convert the pandas DataFrame to a Great Expectations PandasDataset
    ge_df = PandasDataset(df)

    # Try loading the expectation suite
    try:
        suite = context.get_expectation_suite(expectation_suite_name)
    except Exception as e:
        print(f"Expectation suite '{expectation_suite_name}' not found: {e}")
        return

    # Validate the dataframe against the expectations suite
    validation_result = ge_df.validate(expectation_suite=suite)

    # Check if the validation passed or failed
    if not validation_result["success"]:
        raise ValueError(f"Data validation failed with the following errors: {validation_result['results']}")
    else:
        print("Data quality checks passed!")