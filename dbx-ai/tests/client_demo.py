from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()

client.get_function(name="my_function")
