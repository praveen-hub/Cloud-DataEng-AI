from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

# 1. Initialize a SparkSession
spark = SparkSession.builder.appName("SecondHighestSalary").getOrCreate()

# 2. Create a sample DataFrame
data = [("James", 70000),
        ("Maria", 70000),
        ("Scott", 60000),
        ("Jen", 60000),
        ("Jeff", 50000),
        ("Michael", 40000)]
columns = ["employee_name", "salary"]
employee_df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
employee_df.show()

# 3. Define a window specification for ranking
# We use `dense_rank()` to handle ties correctly
window_spec = Window.orderBy(col("salary").desc())

# 4. Add a new column with the rank of each salary
ranked_df = employee_df.withColumn("rank", dense_rank().over(window_spec))

print("DataFrame with Dense Rank:")
ranked_df.show()

# 5. Filter for the row(s) with a rank of 2 and show the result
second_highest_salary_df = ranked_df.filter(col("rank") == 2).select("salary").distinct()

print("Second Highest Salary:")
second_highest_salary_df.show()

# If you want to get the result as a simple value
second_highest_salary = [row.salary for row in second_highest_salary_df.collect()]
print(f"The second-highest salary is: {second_highest_salary[0]}")

# Stop the SparkSession
spark.stop()
