from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

# Shift + Enter to run the cell

# create environment for batch mode 
env = EnvironmentSettings.in_batch_mode()

#   create table environment
table_env = TableEnvironment.create(env)

# hardcoded data
# [] - python type list, mutable - change
# () = type  tuple, immutable - cannot change
data = [ ("Joe", 28), ("Mary", 34), ("Venkat", 40) ] # 3 records
columns = ["name", "age"]
# create virtual table or view
employee_table = table_env.from_elements(data, columns)

# flink always works with structured data means data + meta data 
# meta data = schema , columns, data types etc

# flink too infer schema
# print schema
employee_table.print_schema()

print("before")
print(table_env.list_tables())

# now register virtual table as table for flink sql
# employees2 shall be created in default cataglog, default database
table_env.register_table("employees2", employee_table)

print ("after register")
print(table_env.list_tables())

table2 = table_env.sql_query("SELECT name,age from employees2")
table2.print_schema()

result5 = employee_table.filter ( col ("age") <= 30)
result5.execute().print()
