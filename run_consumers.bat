@echo off
echo Starting StreamGuard Consumers...

set PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4

echo Starting Orders Consumer...
start "StreamGuard - Orders" cmd /k "set PYSPARK_PYTHON=C:\streamguard\venv\Scripts\python.exe && set PYSPARK_DRIVER_PYTHON=C:\streamguard\venv\Scripts\python.exe && set PYTHONPATH=C:\streamguard\streaming && set PYSPARK_SUBMIT_ARGS=--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4 pyspark-shell && cd C:\streamguard && python C:\streamguard\streaming\consumer_orders.py"

timeout /t 10 /nobreak

echo Starting Clickstream Consumer...
start "StreamGuard - Clickstream" cmd /k "set PYSPARK_PYTHON=C:\streamguard\venv\Scripts\python.exe && set PYSPARK_DRIVER_PYTHON=C:\streamguard\venv\Scripts\python.exe && set PYTHONPATH=C:\streamguard\streaming && set PYSPARK_SUBMIT_ARGS=--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4 pyspark-shell && cd C:\streamguard && python C:\streamguard\streaming\consumer_clickstream.py"

timeout /t 10 /nobreak

echo Starting Inventory Consumer...
start "StreamGuard - Inventory" cmd /k "set PYSPARK_PYTHON=C:\streamguard\venv\Scripts\python.exe && set PYSPARK_DRIVER_PYTHON=C:\streamguard\venv\Scripts\python.exe && set PYTHONPATH=C:\streamguard\streaming && set PYSPARK_SUBMIT_ARGS=--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4 pyspark-shell && cd C:\streamguard && python C:\streamguard\streaming\consumer_inventory.py"

echo All 3 consumers started!
pause
