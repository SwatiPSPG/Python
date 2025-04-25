# Databricks notebook source
#1.	Display the last name, salary, department id of the employees who earn more than average salary in their respective departments (Without the window function).

from pyspark.sql.functions import avg, col, lit, sum

df_employees=spark.table('hr.employees')

df_avg_sal=df_employees.select('last_name','salary','department_id').groupBy(col('department_id')).agg(avg('salary').alias('avg_salary'))

df_join_avg_sal = df_employees.join(df_avg_sal, on='department_id', how='inner')


df_dept_sal = df_join_avg_sal.filter(col('salary') > col('avg_salary')) \
                       .select('last_name', 'salary', 'department_id')
                       
display(df_dept_sal)


# COMMAND ----------

df_employees = spark.table("hr.employees")
df_departments = spark.table("hr.departments")
df_jobs = spark.table("hr.jobs")
df_job_history = spark.table("hr.job_history")
df_locations = spark.table("hr.locations")
df_countries = spark.table("hr.countries")
df_regions = spark.table("hr.regions")

#1.	Display the last name, salary, department id of the employees who earn more than average salary in their respective departments (Without the window function).

from pyspark.sql.functions import avg,col

df_avg_sal=df_employees.select('last_name','salary','department_id').groupBy(col('department_id')).agg(avg('salary').alias('avg_salary'))

#display(df_avg_sal)

df_avg_sal_dept = df_employees.join(df_avg_sal,df_employees.department_id == df_avg_sal.department_id,'left')
df_final=df_avg_sal_dept.filter(col('salary')>col('avg_salary')).select('last_name','salary','avg_salary')
display(df_final)

# COMMAND ----------

#2.	Display last name of employees with their dept name, whose manager is King (Direct Reportees).

df_employees.join(df_departments,'department_id').filter(col('last_name')=='King').select('last_name','department_name').show()

# COMMAND ----------

# 3.	Display last name of employees with their dept name, whose indirect manager is King (upto 2 levels).

# a.	Eg: Suppose, Prem reports to king. We also need to find people reporting to prem and print them. 
# b.	Note: ClickHouse does not support recursive CTEs

from pyspark.sql.functions import col, lower

e = df_employees.alias("e")           
m = df_employees.alias("m")           
im = df_employees.alias("im")       
d = df_departments.alias("d")    


direct_reports = (
    e.join(m, col("e.manager_id") == col("m.employee_id"))
     .join(d, col("e.department_id") == col("d.department_id"))
     .filter(lower(col("m.last_name")) == "king")
     .select(col("e.last_name"), col("d.department_name"))
)

indirect_reports = (
    e.join(m, col("e.manager_id") == col("m.employee_id"))
     .join(im, col("m.manager_id") == col("im.employee_id"))
     .join(d, col("e.department_id") == col("d.department_id"))
     .filter(lower(col("im.last_name")) == "king")
     .select(col("e.last_name"), col("d.department_name"))
)

final_result = indirect_reports.unionByName(direct_reports)

display(final_result)


# COMMAND ----------

#4.	Display those employees whose salary is greater than his manager salary.

from pyspark.sql.functions import col

df_employees = spark.table('hr.employees')

e = df_employees.alias("e")           
m = df_employees.alias("m") 

df_mgr = e.join(m, col("e.manager_id") == col("m.employee_id"))

df_sal = df_mgr.filter(col("e.salary") > col("m.salary"))

df_sal.select(
    col("e.last_name").alias("last_name"),
    col("e.salary").alias("emp_sal"),
    col("m.salary").alias("mgr_sal"),
    col("e.manager_id")
).show()


# COMMAND ----------

from pyspark.sql.functions import col

df_employees = spark.table('hr.employees')

e = df_employees.alias("e")
m = df_employees.alias("m")

df_mgr = e.join(m, col("e.manager_id") == col("m.employee_id"))

df_sal = df_mgr.filter(col("e.salary") >= col("m.salary") * 1.2)

df_sal.select(
    col("e.last_name").alias("emp_last_name"),
    col("e.salary").alias("emp_sal"),
    col("m.salary").alias("mgr_sal"),
    col("e.manager_id")
).show()



# COMMAND ----------

#6.	Display last name, job id and manager name of all employees.

from pyspark.sql.functions import col, lit, sum
from pyspark.sql import functions as F


df_employees=spark.table('hr.employees')

e = df_employees.alias("e")           
m = df_employees.alias("m") 

df_mgr=e.join(m,e.manager_id==m.employee_id,'left')

df_sal=df_mgr.select(
    F.col('e.last_name').alias('employee_last_name'),
    F.col('e.job_id').alias('employee_job_id'),
    F.col('m.last_name').alias('manager_last_name')
)

df_sal.show()


# COMMAND ----------

#7.	Display department names where employee count exceeds the average employee count across all departments..


from pyspark.sql.functions import col, lit,avg,count

df_employees=spark.table('hr.employees')
df_departments=spark.table('hr.departments')

dept_emp_count=df_employees.groupBy('department_id').agg(count("*").alias("emp_count"))

avg_emp_count=dept_emp_count.agg(avg("emp_count").alias("avg_emp_count"))
greater_avg_depts = dept_emp_count.filter(col("emp_count") > avg_emp_count.first()[0])

df_final = greater_avg_depts.join(df_departments, "department_id") \
    .select("department_name", "emp_count")

display(df_final)


# COMMAND ----------

#8.	Display last name and hire date of employees who joined within the first 10 days of any month and on a Monday or Friday.

from pyspark.sql.functions import dayofmonth, dayofweek, col

df_employees.select('last_name','hire_date').filter( \
                                            (dayofmonth(col('hire_date')) <= 10) & \
                                             (dayofweek(col('hire_date')).isin(2,6))
).show()
            

# COMMAND ----------

#9.	Display the manager name who is having maximum number of employees working under him?

from pyspark.sql.functions import col, count

df_employees=spark.table('hr.employees')

df_emp_count=df_employees.groupBy('manager_id').agg(count("*").alias("emp_count"))\
             .orderBy(col("emp_count").desc())


df_final=df_emp_count.join(df_employees, "manager_id") \
    .select("last_name", "emp_count") \
    .show(1)


# COMMAND ----------

#10.	Display the manager’s name who has the highest total salary from all employees reporting to them.

from pyspark.sql.functions import col, sum

df_employees=spark.table('hr.employees')

e=df_employees.alias("e")
m=df_employees.alias("m")

df_emp_mgr=e.join(m,col("e.manager_id")==col("m.employee_id"),'left')

df_mgr_sal=df_emp_mgr.groupBy('m.last_name',"m.employee_id").agg(sum('e.salary').alias('total_salary'))

top_mgr = df_mgr_sal.orderBy(col("total_salary").desc()).limit(1)
df_final = top_mgr.select(
    col("m.employee_id"),
    col("m.last_name").alias("manager_last_name")
)

df_final.show()