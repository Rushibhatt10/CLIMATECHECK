"""Checks that Java + PySpark work on this machine. Run from the project root."""
import os
import platform
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import pyspark
    from src.data_loader import get_spark
except ImportError as e:
    print(f"FAIL: cannot import PySpark ({e}). Activate the venv and run: pip install -r requirements.txt")
    sys.exit(1)

print(f"Python  : {platform.python_version()}")
print(f"PySpark : {pyspark.__version__}")
spark = None
try:
    spark = get_spark()
    jvm = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
    print(f"Java    : {jvm}")
    df = spark.createDataFrame([("A", 1), ("A", 3), ("B", 5)], ["k", "v"])
    result = {r["k"]: r["s"] for r in df.groupBy("k").sum("v").withColumnRenamed("sum(v)", "s").collect()}
    assert result == {"A": 4, "B": 5}, result
    print("Spark job ran correctly: PASS")
except Exception as e:  # noqa: BLE001
    print(f"FAIL: {type(e).__name__}: {str(e)[:400]}")
    print("Check that Java 17 is on PATH (java -version). If the error mentions Python workers, see README 'Troubleshooting'.")
    sys.exit(1)
finally:
    if spark is not None:
        spark.stop()
