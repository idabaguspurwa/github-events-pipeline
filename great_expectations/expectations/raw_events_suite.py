# great_expectations/expectations/raw_events_suite.py
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

# This defines our suite of tests
suite = ExpectationSuite(
    name="raw_events.warning",
    expectations=[
        gx.expectation.ExpectColumnToExist(column="V"),
        gx.expectation.ExpectColumnValuesToNotBeNull(column="V:id"),
    ]
)

# We need a context to save the suite
try:
    context = gx.get_context(project_root_dir='/great_expectations')
    context.suites.add(suite)
    context.suites.persist()
except Exception as e:
    # This will fail when the DAG parses it, which is okay.
    # It only needs to run inside the GX container.
    print(f"Ignoring GX context error during DAG parsing: {e}")