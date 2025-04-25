# Databricks notebook source
df_employees = spark.table("hr.employees")
df_departments = spark.table("hr.departments")
df_jobs = spark.table("hr.jobs")
df_job_history = spark.table("hr.job_history")
df_locations = spark.table("hr.locations")
df_countries = spark.table("hr.countries")
df_regions = spark.table("hr.regions")
 
display(df_employees)
display(df_departments)
display(df_jobs)
display(df_job_history)
display(df_locations)
display(df_countries)
display(df_regions)

# COMMAND ----------

#1.	Display the departments information from department table. 

display(df_departments)


# COMMAND ----------

#2.	Display the details of all employees with department names.       

emp_with_dept_names=df_departments.join(df_employees,df_departments.department_id==df_employees.department_id,'left')

display(emp_with_dept_names)


# COMMAND ----------

#3.	Display the Last name and job id for all employees. 

display(df_employees.select('last_name','job_id'))

df_employees.select('last_name','job_id')

# COMMAND ----------

#3.	Display the Last name and job id for all employees. 
df_lastname_jobid=df_employees.select("last_name","job_id")

df_lastname_jobid.show()


# COMMAND ----------

#4.	Display Last name and salary of employees without managers/valid managers id.. 
# person should not have any manager_id and person who has invalid manager id       

# Filter employees without managers or with invalid manager IDs 

from pyspark.sql.functions import col

df_employees=spark.table("hr.employees")

df_no_managers = df_employees.filter((df_employees.manager_id.isNull()) | (~df_employees.manager_id.isin(df_employees.employee_id))) 

# Select and display last name and salary of these employees 
df_result = df_no_managers.select("last_name", "salary") 
display(df_result)  

# COMMAND ----------

#5.Display employee id, salary, commission_pct, and total salary (salary+commission) for each employee. 
#(commission_pct is a decimal col where .4 means 40%  commission of salary is there.)

from pyspark.sql.functions import col, coalesce,lit

df_employees = spark.table("hr.employees")
df_sal_comm = df_employees.select("employee_id",
                                  "salary",
                                  "commission_pct",
                                  (col("salary") + (col("salary") * coalesce(col("commission_pct"), lit(0)))).alias("total_salary")
)

display(df_sal_comm)

#this is another way
# df_employees.selectExpr("employee_id","salary","commission_pct","salary + (salary * IFNULL(commission_pct,0)) as # total_salary").display()  
 


# COMMAND ----------

#6.	Display Last name and annual salary (salary+commission)for all employees. 
from pyspark.sql.functions import col, coalesce,lit

df_annual_sal = df_employees.select("last_name", ((col("salary") + (col("salary") * coalesce(col("commission_pct"), lit(0)))) * 12).alias("annual_salary"))

display(df_annual_sal)

# COMMAND ----------

#7.Display the Last name, salary, department_id of all employees who are working in department number 10. 

df_dept_10=df_employees.select("last_name","salary","department_id").filter(df_employees.department_id==10)

display(df_dept_10)

# COMMAND ----------

#8.	Display the Last names, job_id, salary of all employees working as clerks and drawing a salary more than 3000. 

from pyspark.sql.functions import col, lower

df_clerk=df_employees.select('last_name','job_id','salary').filter((lower(col('job_id'))=='CLERK') & (df_employees.salary>3000))

display(df_clerk)



# COMMAND ----------

from pyspark.sql.functions import col

comm_pct_not_null=df_employees.select('employee_id','last_name','commission_pct') \
.filter(col('commission_pct').isNotNull())

display(comm_pct_not_null)

# COMMAND ----------

#11.Display the Last name, job id and salary of employees who are working as SA_MAN, SA_REP, IT_PROG and drawing a salary more than 3000. 

df_jobs=spark.table("hr.jobs")
df_final=df_employees.join(df_jobs,df_employees.job_id==df_jobs.job_id,'inner').filter((df_employees.salary>3000) \
)

display(df_final)

# COMMAND ----------

#12.Display the Last name,hire date of employees who have joined in the company in last 20 years.df_employees

from pyspark.sql.functions import col, current_date, year

df_recent_hires = df_employees.select('last_name','hire_date'). \
filter(year(col('hire_date')) >= year(current_date())-20)

display(df_recent_hires)

# COMMAND ----------

#13.	Display the last name and hire date of employees who have joined the company before 1st Jan 08 and after 31st   dec 08. 

df_dates=df_employees.select('last_name','hire_date').filter((df_employees.hire_date<'2008-01-01') & (df_employees.hire_date>'2008-12-31'))

display(df_dates)


# COMMAND ----------

#14.Find employees who have worked in more than one department.

from pyspark.sql.functions import col, countDistinct

df_emp_multiple_dept = df_employees.groupBy("employee_id") \
    .agg(countDistinct("department_id").alias("dept_count")) \
    .filter(col("dept_count") > 1)

display(df_emp_multiple_dept)

# COMMAND ----------

#15.Display the names of employees whose last name starts with alphabet S.

from pyspark.sql.functions import col
df_emp_name_S=df_employees.select('first_name','last_name').filter(col('last_name').startswith('S'))

display(df_emp_name_S)

# COMMAND ----------

#16. Display the last name of employees whose last name is exactly five characters in length.

from pyspark.sql.functions import col,length

df_emp_last_name_5=df_employees.select('last_name').filter(length(col('last_name'))==5)

display(df_emp_last_name_5)


# COMMAND ----------

#17.Display the total number of employees working in each country.

df_departments=spark.table("hr.departments")
df_locations=spark.table("hr.locations")
df_countries=spark.table("hr.countries")

df_employees.join(df_departments,df_employees.department_id==df_departments.department_id,'inner').join(df_locations,df_departments.location_id==df_locations.location_id,'inner').join(df_countries,df_locations.country_id==df_countries.country_id,'inner').groupBy('country_name').count().show()

# COMMAND ----------

#18.Display the name , department and salary of employees who has max salary in US and UK.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 1: Join employees, departments, and locations to get the required data
df_joined = df_employees.join(df_departments, df_employees.department_id == df_departments.department_id, \
                              "inner").\
                              join(df_locations, df_departments.location_id == df_locations.location_id, "inner").\
select("first_name", "last_name", "department_name", "salary", "country_id")

window_spec = Window.partitionBy("country_id").orderBy(F.col("salary").desc())

df_max_salary = df_joined.withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.upper(F.col("country_id")).isin("US", "UK")) \
    .filter(F.col("row_num") == 1) \
    .select("first_name", "last_name", "department_name", "salary")

display(df_max_salary)

# COMMAND ----------

#19. Display the employee_id, last_name of employees who are not working as managers.

from pyspark.sql.functions import col

df_employees=spark.table("hr.employees")
df_not_mgr=df_employees.select("employee_id","last_name").filter(col("employee_id")!=col("manager_id"))

display(df_not_mgr)

# COMMAND ----------

#20. Display the department name and corresponding manager’s employee_id

df_departments=spark.table("hr.departments")
df_departments.join(df_employees,df_departments.manager_id==df_employees.employee_id,'inner').select("department_name","employee_id").show()

# COMMAND ----------

# Filter employees without managers or with invalid manager IDs 

df_no_managers = df_employees.filter((df_employees.manager_id.isNull()) | (~df_employees.manager_id.isin(df_employees.employee_id))) 

# Select and display last name and salary of these employees 
df_result = df_no_managers.select("last_name", "salary").display(df_result)  