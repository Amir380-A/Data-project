'''
store_suite = gx.ExpectationSuite(name="store_expectations")

store_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeComplete(column="storeid_pk_bk"))
store_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeComplete(column="store_name"))
store_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeComplete(column="number_of_employees"))
store_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeComplete(column="zipcode"))
store_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeComplete(column="latitude"))
store_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeComplete(column="longitude"))

store_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="storeid_pk_bk"))
store_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="latitude", type_="float64"))
store_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="longitude", type_="float64"))

store_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90, max_value=90))
store_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180, max_value=180))
store_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeGreaterThan(column="number_of_employees", min_value=4))



'''

import pandas as pd
import great_expectations as gx
import os

# df = pd.read_csv("./data/dim_stores.csv", low_memory=False)
df = pd.read_csv("/usr/local/airflow/include/archive/dim_stores.csv", low_memory=False)
# Get GX context
context = gx.get_context(mode="file")
datasource_names = [ds["name"] for ds in context.list_datasources()]
if "pandas" not in datasource_names:
    data_source = context.data_sources.add_pandas(name="pandas")
else:
    data_source = context.data_sources.get("pandas")

data_asset = data_source.add_dataframe_asset(name="stores_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe(name="batch_stores")

suite = gx.ExpectationSuite(name="stores_suite")
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column=df.columns[0])
)
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="storeid_pk_bk"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="store_name"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="number_of_employees"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="zipcode"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="latitude"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="longitude"))

suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="storeid_pk_bk"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="latitude", type_="float64"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column="longitude", type_="float64"))

suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90, max_value=90))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180, max_value=180))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="number_of_employees",
        min_value=4,
        strict_min=True  
    )
)

context.suites.add(suite)

# Create validation definition
validation_definition = context.validation_definitions.add(
    gx.core.validation_definition.ValidationDefinition(
        name="stores_validation",
        data=batch_definition,
        suite=suite
    )
)
checkpoint = context.checkpoints.add(
    gx.checkpoint.checkpoint.Checkpoint(
        name="stores_checkpoint",
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
