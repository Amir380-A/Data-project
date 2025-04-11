'''
employee_suite = gx.ExpectationSuite(name="employee_expectations")


'''
import pandas as pd
import great_expectations as gx
import os

# df = pd.read_csv("./data/dim_employees.csv", low_memory=False)
df = pd.read_csv("/usr/local/airflow/include/archive/dim_employees.csv", low_memory=False)
context = gx.get_context(mode="file")
data_source = context.data_sources.get("pandas")

data_asset = data_source.add_dataframe_asset(name="employees_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe(name="batch_employees")

# Create Expectation Suite
suite = gx.ExpectationSuite(name="employees_suite")
if "passenger_count" in df.columns:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="passenger_count", min_value=1, max_value=6
        )
    )
else:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column=df.columns[0])
    )
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="name"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="position"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="name", type_="str"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="position", type_="str"))
context.suites.add(suite)

# Add Validation Definition
validation_definition = context.validation_definitions.add(
    gx.core.validation_definition.ValidationDefinition(
        name="employees_validation",
        data=batch_definition,
        suite=suite
    )
)

# Create Checkpoint
checkpoint = context.checkpoints.add(
    gx.checkpoint.checkpoint.Checkpoint(
        name="employees_checkpoint",
        validation_definitions=[validation_definition]
    )
)

# Run Checkpoint
checkpoint_result = checkpoint.run(
    batch_parameters={"dataframe": df}
)

# Print Validation Results
# Print results
print(checkpoint_result.describe())

# Build data docs
context.build_data_docs()


'''
# Print link to data docs
project_root = context.root_directory
data_docs_path = os.path.join(project_root, "uncommitted", "data_docs", "local_site", "index.html")
print(f" Data Docs available at: file://{data_docs_path}")

'''
