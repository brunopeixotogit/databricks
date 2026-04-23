import sys
import os
import pytest

sys.path.append(os.getcwd())

@pytest.fixture()
def spark():
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        pass

    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.master("local[*]").getOrCreate()
    except ImportError:
        raise ImportError(
            "Spark não disponível. Instale 'pyspark' ou configure 'databricks-connect'."
        )