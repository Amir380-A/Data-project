import pandas as pd
import great_expectations as gx
import os

df = pd.read_csv("/usr/local/airflow/include/archive/dim_customers.csv", low_memory=False)
# df = pd.read_csv("./data/dim_discounts.csv", low_memory=False)
context = gx.get_context(mode="file")
data_source = context.data_sources.add_pandas("pandas")

data_asset = data_source.add_dataframe_asset(name="discounts_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe(name="batch_discounts")

suite = gx.ExpectationSuite(name="customer_suite")
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column=df.columns[0])
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column=df.columns[1])
)
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column=df.columns[0]))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="name"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="email"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="country"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="email"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(column="telephone", regex=r"^\+?\d{1,15}$"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="gender", value_set=["M", "F"]))
context.suites.add(suite)

validation_definition = context.validation_definitions.add(
    gx.core.validation_definition.ValidationDefinition(
        name="discounts_validation",
        data=batch_definition,
        suite=suite
    )
)
checkpoint = context.checkpoints.add(
    gx.checkpoint.checkpoint.Checkpoint(
        name="discounts_checkpoint",
        validation_definitions=[validation_definition]
    )
)

checkpoint_result = checkpoint.run(
    batch_parameters={"dataframe": df}
)

print(checkpoint_result.describe())
context.build_data_docs()

'''
# ✅ Print link to data docs
project_root = context.root_directory
data_docs_path = os.path.join(project_root, "uncommitted", "data_docs", "local_site", "index.html")
print(f"📊 Data Docs available at: file://{data_docs_path}")

'''
