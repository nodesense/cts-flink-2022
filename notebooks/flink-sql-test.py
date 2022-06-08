from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, CsvTableSource
import datetime
from pyflink.table.expressions import col
from pyflink.table.window import Over, GroupWindow
from pyflink.table.expressions import col, UNBOUNDED_RANGE, CURRENT_RANGE
from pyflink.table.udf import udf
# create a batch TableEnvironment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)
#table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///opt/flink-1.15.0/lib/flink-csv-1.15.0.jar;")


# InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
# 536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,12/01/2010 8:26,2.55,17850,United Kingdom
# create table in default catalog , default database
source_ddl = """
            CREATE TABLE invoices (
                   InvoiceNo STRING,
                   StockCode  STRING,
                   Description  STRING,
                   Quantity DOUBLE,
                   InvoiceDate TIMESTAMP(3),
                   UnitPrice DOUBLE,
                   CustomerID STRING,
                   Country STRING

                    ) WITH (
                      'connector' = 'filesystem',          
                      'path' = 'file:///home/rps/workshop/data/ecommerce-clean.csv', 
                      'format' = 'csv'
                    )"""


table_env.execute_sql(source_ddl) 

# tabel view for python table api
invoices = table_env.from_path("invoices")
##############################
print('\nRegistered Tables List')
print(table_env.list_tables())

print('\nFinancial Trxs Schema')
invoices.print_schema()

invoices.fetch(3).execute().print()