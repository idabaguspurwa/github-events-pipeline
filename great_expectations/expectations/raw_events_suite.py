# great_expectations/expectations/raw_events_suite.py
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

# This defines our suite of data quality tests for GitHub events
suite = ExpectationSuite(
    name="raw_events_suite",
    expectations=[
        # Basic structure expectations
        {
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 0, "max_value": 100000}
        },
        
        # Expected columns for GitHub events
        {
            "expectation_type": "expect_table_columns_to_match_set", 
            "kwargs": {"column_set": ["id", "type", "actor", "repo", "created_at", "payload"]}
        },
        
        # Critical fields should not be null
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "id"}
        },
        
        {
            "expectation_type": "expect_column_values_to_not_be_null", 
            "kwargs": {"column": "type"}
        },
        
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "created_at"}
        },
        
        # Data type validations
        {
            "expectation_type": "expect_column_values_to_be_of_type",
            "kwargs": {"column": "id", "type_": "string"}
        },
        
        # Value constraints
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "type",
                "value_set": [
                    "PushEvent", "CreateEvent", "DeleteEvent", "ForkEvent",
                    "WatchEvent", "IssuesEvent", "PullRequestEvent", 
                    "ReleaseEvent", "PublicEvent", "MemberEvent"
                ]
            }
        }
    ]
)

# Try to save the suite to context (will fail during DAG parsing, which is expected)
try:
    context = gx.get_context()
    context.suites.add(suite)
    print(f"✅ Great Expectations suite '{suite.name}' saved successfully")
except Exception as e:
    # This will fail when the DAG parses it, which is okay.
    # It only needs to run inside the actual validation environment.
    print(f"ℹ️ Ignoring GX context error during DAG parsing: {e}")