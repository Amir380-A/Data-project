import pandas as pd
import great_expectations as gx
import os
import tempfile
import shutil


# Step 1: Load CSV
df = pd.read_csv("/usr/local/airflow/include/archive/dim_products.csv", low_memory=False)
context = gx.get_context(mode="ephemeral")

datasource_names = [ds["name"] for ds in context.list_datasources()]
if "pandas" not in datasource_names:
    data_source = context.data_sources.add_pandas(name="pandas")
else:
    data_source = context.data_sources.get("pandas")


data_asset = data_source.add_dataframe_asset(name="products_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe(name="batch_products")

suite = gx.ExpectationSuite(name="products_suite")
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column=df.columns[0])
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column=df.columns[1])
)
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column=df.columns[0]))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="description_en", type_="str"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="description_en"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="production_cost", type_="float64"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="unit_price", type_="float64"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="currency", value_set=["USD", "EUR", "GBP", "CNY"]))


context.suites.add(suite)

validation_definition = context.validation_definitions.add(
    gx.core.validation_definition.ValidationDefinition(
        name="products_validation",
        data=batch_definition,
        suite=suite
    )
)

checkpoint = context.checkpoints.add(
    gx.checkpoint.checkpoint.Checkpoint(
        name="products_checkpoint",
        validation_definitions=[validation_definition]
    )
)

checkpoint_result = checkpoint.run(
    batch_parameters={"dataframe": df}
)

print(checkpoint_result.describe())

context.build_data_docs()


'''
## ✅ Print link to data docs
project_root = context.root_directory
data_docs_path = os.path.join(project_root, "uncommitted", "data_docs", "local_site", "index.html")
print(f"📊 Data Docs available at: file://{data_docs_path}")
'''

