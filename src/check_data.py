import great_expectations as gx
import pandas as pd
import sys

# 1. Load Data
df = pd.read_csv("data/processed/train.csv")

# 2. Setup GX Context
context = gx.get_context()

# 3. Create Data Source & Asset
data_source_name = "pandas_source"
try:
    data_source = context.data_sources.get(data_source_name)
except KeyError:
    data_source = context.data_sources.add_pandas(name=data_source_name)

# Add or get the dataframe asset
try:
    data_asset = data_source.get_asset(name="train_asset")
except LookupError:
    data_asset = data_source.add_dataframe_asset(name="train_asset")

# 4. Create an Expectation Suite
suite_name = "asteroid_suite"
try:
    suite = context.suites.get(suite_name)
except gx.exceptions.DataContextError:
    suite = context.suites.add(gx.core.ExpectationSuite(name=suite_name))

# Check A: Target should not be null
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="target"))

# Check B: Target must be 0 or 1
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="target", value_set=[0, 1]))

# 5. Run Validation
batch_definition = data_asset.add_batch_definition_whole_dataframe("full_dataframe")

# Create validation definition
validation_def_name = "my_validation"
try:
    validation_def = context.validation_definitions.get(validation_def_name)
except gx.exceptions.DataContextError:
    validation_def = gx.core.ValidationDefinition(
        name=validation_def_name,
        data=batch_definition,
        suite=suite
    )
    context.validation_definitions.add(validation_def)

print("Running validation...")
result = validation_def.run(batch_parameters={"dataframe": df})

# 6. Handle Results
if not result.success:
    print("Data validation failed!")
    print(result.describe_dict()) 
    sys.exit(1)
else:
    print("All data checks passed!")
    with open("validation_report.txt", "w") as f:
        f.write("Passed")