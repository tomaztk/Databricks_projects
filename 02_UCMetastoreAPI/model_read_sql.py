# Databricks notebook source
import mlflow
import pandas as pd
from mlflow.models import ModelSignature
from mlflow.types import Schema, ColSpec
import os
import textwrap

mlflow.set_registry_uri("databricks-uc")   

CATALOG = "demo_api_uc"
SCHEMA  = "app2"
TABLE   = f"{CATALOG}.{SCHEMA}.orders"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.orders_sql_proxy"

 
class SQLQueryModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        # Read connection info from env vars injected into the serving endpoint
        self.server_hostname = os.environ.get("DBSQL_HOST")
        self.http_path       = os.environ.get("DBSQL_HTTP_PATH")
        self.token           = os.environ.get("DBSQL_TOKEN")
        self.table           = os.environ.get("ORDERS_TABLE", TABLE)

        if not (self.server_hostname and self.http_path and self.token):
            raise RuntimeError("Missing DBSQL_* env vars required to connect to SQL Warehouse.")

    def _query(self, country: str, min_amount: float) -> pd.DataFrame:
        import databricks.sql as dbsql  # pulled from model env
        # Parameterized query (connector >=3.0 supports native params) :contentReference[oaicite:5]{index=5}
        sql = f"""
            SELECT customer_id, country, amount
            FROM {self.table}
            WHERE country = ? AND amount >= ?
            ORDER BY customer_id DESC
            LIMIT 100
        """
        with dbsql.connect(server_hostname=self.server_hostname,
                           http_path=self.http_path,
                           access_token=self.token) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (country, float(min_amount)))
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
        return pd.DataFrame(rows, columns=cols)

    def predict(self, context, model_input: pd.DataFrame):
        # Expect a single-row DataFrame with columns 'country' and 'min_amount'
        country = model_input.iloc[0]["country"]
        min_amount = model_input.iloc[0]["min_amount"]
        return self._query(country, min_amount)

# Model signature: inputs and outputs
input_schema  = Schema([ColSpec("string", "country"), ColSpec("double", "min_amount")])
output_schema = Schema([

    ColSpec("long", "customer_id"),
    ColSpec("string", "country"),
    ColSpec("double", "amount"),

])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)

# Minimal input example for the Serving UI
input_example = pd.DataFrame([{"country": "DE", "min_amount": 50.0}])

# Environment: include connector in the model package
conda_env = {
    "name": "sql_proxy_env",
    "channels": ["conda-forge"],
    "dependencies": [
        "python=3.11",
        "pip",
        {"pip": [
            "mlflow>=2.9.0",
            "pandas",
            "databricks-sql-connector>=3.0.0"  # native params (safer) :contentReference[oaicite:6]{index=6}
        ]}
    ]
}

# check for the access to UC
with mlflow.start_run(run_name="sql-proxy"):
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=SQLQueryModel(),
        conda_env=conda_env,
        signature=signature,
        input_example=input_example,
        code_path=None  # keep minimal; add helpers here if you have any
    )
    run = mlflow.active_run().info.run_id
    model_uri = f"runs:/{run}/model"

# Register to Models in UC (namespaced)
registered = mlflow.register_model(model_uri, MODEL_NAME)  
print("Registered version:", registered.version)
