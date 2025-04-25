# Databricks notebook source
df_employees = spark.table("hr.employees")
df_departments = spark.table("hr.departments")
df_jobs = spark.table("hr.jobs")
df_job_history = spark.table("hr.job_history")
df_locations = spark.table("hr.locations")
df_countries = spark.table("hr.countries")
df_regions = spark.table("hr.regions")

# COMMAND ----------

#1.	Display the total salary (salary + commission_pct) being paid to all employees grouped by department, and only include departments where the total exceeds 50000..

from pyspark.sql.functions import col, sum

df_employees.withColumn('total_salary', col('salary') + col('commission_pct')).groupBy('department_id').agg(sum('total_salary').alias('total_salary')).filter(col('total_salary') > 50000).show()

# COMMAND ----------

#2.	Display the last name, salary, and job_id of the employee(s) earning the maximum salary departments 20 , 30 and 90
from pyspark.sql.functions import col, max

df_max_salary = df_employees.filter(col("department_id").isin(20, 30, 90)) \
    .groupBy("department_id") \
    .agg(max("salary").alias("max_salary"))

display(df_max_salary)


# COMMAND ----------

#3.Display the 2nd highest salary without rank().

from pyspark.sql.functions import max as max, col


max_salary = df_employees.agg(max('salary').alias('max_salary')).collect()[0]['max_salary']


df_less_than_max = df_employees.filter(col('salary') < max_salary)


df_second_max = df_less_than_max.agg(max('salary').alias('second_max_salary'))


display(df_second_max)



# COMMAND ----------

#4.	Display the last_name, salary of SA_MAN employees who earn more than the maximum salary of any SA_REP.

df_final=df_employees.select("last_name","salary").filter(df_employees["job_id"] == "SA_MAN").filter(df_employees["salary"] > df_employees.agg(max("salary")).collect()[0][0])

display(df_final)

# COMMAND ----------

#5.	Display department_id, total number of employees, and average salary in each department, for departments with at least 5 employees.

from pyspark.sql.functions import count, avg, col

df_employees.groupBy("department_id").agg(count("*").alias("total_employees"), avg("salary").alias("avg_salary")).filter(col("total_employees") >= 5).show()

# COMMAND ----------

#6.Display department_id and number of employees for departments where employee count > 3 AND total salary > 30000.

from pyspark.sql.functions import count, sum, col

df_department_stats = df_employees.groupBy("department_id") \
    .agg(
        count("*").alias("number_of_employees"),
        sum("salary").alias("total_salary")
    )

df_filtered_departments = df_department_stats.filter(
    (col("number_of_employees") > 3) & (col("total_salary") > 30000)
)

df_filtered_departments.show()


# COMMAND ----------

#7.	Display the top 3 earners by salary with their last name and salary.

df_top3_earners = df_employees.select("last_name", "salary").orderBy(col("salary").desc()).limit(3)

display(df_top3_earners)

# COMMAND ----------

#8.	Display the last_name, salary, department_id of employees who are working in ‘Sales’ departments.

df_employees.join(df_departments,df_employees.department_id==df_departments.department_id).select('last_name','salary',df_employees.department_id).filter(df_departments.department_name=='Sales').show()

# COMMAND ----------

#9.	Display last_name, department_name, and region_name of all employees.

df_employees.join(df_departments,df_employees.department_id==df_departments.department_id).join(df_locations,df_departments.location_id==df_locations.location_id).join(df_countries,df_locations.country_id==df_countries.country_id).join(df_regions,df_countries.region_id==df_regions.region_id).select('last_name','department_name','region_name').show()

# COMMAND ----------

#10.	Display last_name, salary, and job_title of employees whose salary is an odd number.

from pyspark.sql.functions import col

df_joined = df_employees.join(df_jobs, df_employees.job_id == df_jobs.job_id)
df_filtered = df_joined.filter((col('salary') % 2 == 1))
df_result = df_filtered.select('last_name', 'salary', 'job_title')

df_result.show()

