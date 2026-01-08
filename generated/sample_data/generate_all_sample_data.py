"""
Generate comprehensive sample data for all 5 Informatica to PySpark ETL migrations.
This script creates CSV files for all source tables and lookup tables needed for testing.
"""

import csv
import random
import os

# Ensure output directory exists
os.makedirs(".", exist_ok=True)

# Random data generators
first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
jobs = ["CLERK", "SALESMAN", "MANAGER", "ANALYST", "PRESIDENT"]
departments = [10, 20, 30, 40]
dept_names = {10: "ACCOUNTING", 20: "RESEARCH", 30: "SALES", 40: "OPERATIONS"}
locations = ["NEW YORK", "DALLAS", "CHICAGO", "BOSTON", "SEATTLE"]
contracts = ["Month-to-month", "One year", "Two year"]
genders = ["Male", "Female"]

def generate_email(first, last, emp_id):
    return f"{first.lower()}.{last.lower()}{emp_id}@company.com"

def generate_phone():
    return f"{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

# ============================================================================
# 1. EMPLOYEE table (for wf_s_m_multiple_unc11 and wf_m_mapplet_multiple_unconnected)
# ============================================================================
print("Generating employee.csv...")
with open("employee.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["EMPNO", "ENAME", "JOB", "MGR", "SAL", "COMM", "DEPTNO"])
    for i in range(1, 101):
        empno = 7000 + i
        ename = random.choice(first_names) + " " + random.choice(last_names)
        job = random.choice(jobs)
        mgr = random.choice([7839, 7698, 7782, 7566, None])
        sal = random.randint(1000, 5000)
        comm = random.randint(0, 500) if job == "SALESMAN" else None
        deptno = random.choice(departments)
        writer.writerow([empno, ename, job, mgr, sal, comm, deptno])

# ============================================================================
# 2. DDEPT table (for lookups - DEPTNO -> DNAME)
# ============================================================================
print("Generating ddept.csv...")
with open("ddept.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["D_DEPTNO", "DNAME", "LOC"])
    for deptno, dname in dept_names.items():
        loc = random.choice(locations)
        writer.writerow([deptno, dname, loc])

# ============================================================================
# 3. EMP_LOC table (for lookups - EMPNO/DEPID -> LOC)
# ============================================================================
print("Generating emp_loc.csv...")
with open("emp_loc.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["DEPID", "LOC"])
    for deptno in departments:
        loc = random.choice(locations)
        writer.writerow([deptno, loc])

# ============================================================================
# 4. EMP_LOC_EMPID table (for lookups by EMPID -> LOC)
# ============================================================================
print("Generating emp_loc_empid.csv...")
with open("emp_loc_empid.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["EMPID", "LOC"])
    for i in range(1, 101):
        empid = i
        loc = random.choice(locations)
        writer.writerow([empid, loc])

# ============================================================================
# 5. Churndata table (for wf_m_PatternForChurnData)
# ============================================================================
print("Generating churndata.csv...")
with open("churndata.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["customerID", "gender", "SeniorCitizen", "Partner", "Dependents", 
                     "tenure", "PhoneService", "MultipleLines", "InternetService",
                     "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport",
                     "StreamingTV", "StreamingMovies", "Contract", "PaperlessBilling",
                     "PaymentMethod", "MonthlyCharges", "TotalCharges", "Churn"])
    for i in range(1, 101):
        customer_id = f"CUST{i:04d}"
        gender = random.choice(genders)
        senior = random.choice([0, 1])
        partner = random.choice(["Yes", "No"])
        dependents = random.choice(["Yes", "No"])
        tenure = random.randint(1, 72)
        phone = random.choice(["Yes", "No"])
        multi_lines = random.choice(["Yes", "No", "No phone service"])
        internet = random.choice(["DSL", "Fiber optic", "No"])
        security = random.choice(["Yes", "No", "No internet service"])
        backup = random.choice(["Yes", "No", "No internet service"])
        protection = random.choice(["Yes", "No", "No internet service"])
        support = random.choice(["Yes", "No", "No internet service"])
        tv = random.choice(["Yes", "No", "No internet service"])
        movies = random.choice(["Yes", "No", "No internet service"])
        contract = random.choice(contracts)
        paperless = random.choice(["Yes", "No"])
        payment = random.choice(["Electronic check", "Mailed check", "Bank transfer", "Credit card"])
        monthly = round(random.uniform(20, 100), 2)
        total = round(monthly * tenure, 2)
        churn = random.choice(["Yes", "No"])
        writer.writerow([customer_id, gender, senior, partner, dependents, tenure, phone,
                        multi_lines, internet, security, backup, protection, support,
                        tv, movies, contract, paperless, payment, monthly, total, churn])

# ============================================================================
# 6. Gender lookup table (for wf_m_PatternForChurnData)
# ============================================================================
print("Generating gender.csv...")
with open("gender.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Gender", "Type"])
    writer.writerow(["Male", "M"])
    writer.writerow(["Female", "F"])

# ============================================================================
# 7. EMPLOYEEDETAILS table (for wf_s_m_multiple_mapplet and wf_s_m_complex5)
# ============================================================================
print("Generating employeedetails.csv...")
with open("employeedetails.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["EMPID", "FIRSTNAME", "LASTNAME", "EMAIL", "PHONENO", "SALARY", "DEPID"])
    for i in range(1, 101):
        empid = i
        first = random.choice(first_names)
        last = random.choice(last_names)
        email = generate_email(first, last, empid)
        phone = generate_phone()
        salary = random.randint(30000, 150000)
        depid = random.choice(departments)
        writer.writerow([empid, first, last, email, phone, salary, depid])

# ============================================================================
# 8. DEPT table (for mapplet lookups - DEPTNO -> DNAME)
# ============================================================================
print("Generating dept.csv...")
with open("dept.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["DEPTNO", "DNAME", "LOC"])
    for deptno, dname in dept_names.items():
        loc = random.choice(locations)
        writer.writerow([deptno, dname, loc])

# ============================================================================
# 9. EMP_LOC_DEPT table (for mapplet lookups - DEPID -> LOC)
# ============================================================================
print("Generating emp_loc_dept.csv...")
with open("emp_loc_dept.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["DEPID", "LOC"])
    for deptno in departments:
        loc = random.choice(locations)
        writer.writerow([deptno, loc])

# ============================================================================
# 10. EMP_DEPT table (for wf_s_m_complex5 - department reference)
# ============================================================================
print("Generating emp_dept.csv...")
with open("emp_dept.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["DEPTNO", "DNAME", "LOC"])
    for deptno, dname in dept_names.items():
        loc = random.choice(locations)
        writer.writerow([deptno, dname, loc])

# ============================================================================
# 11. EMPLOYEE_DETAILS1 table (for wf_s_m_complex5 - source 1)
# ============================================================================
print("Generating employee_details1.csv...")
with open("employee_details1.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["EMPID", "FIRSTNAME", "LASTNAME", "EMAIL", "PHONENO", "SALARY", "DEPID"])
    for i in range(1, 26):
        empid = i
        first = random.choice(first_names)
        last = random.choice(last_names)
        email = generate_email(first, last, empid)
        phone = generate_phone()
        salary = random.randint(30000, 150000)
        depid = random.choice(departments)
        writer.writerow([empid, first, last, email, phone, salary, depid])

# ============================================================================
# 12. EMPLOYEE_DETAILS2 table (for wf_s_m_complex5 - source 2)
# ============================================================================
print("Generating employee_details2.csv...")
with open("employee_details2.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["EMPID", "FIRSTNAME", "LASTNAME", "EMAIL", "PHONENO", "SALARY", "DEPID"])
    for i in range(26, 51):
        empid = i
        first = random.choice(first_names)
        last = random.choice(last_names)
        email = generate_email(first, last, empid)
        phone = generate_phone()
        salary = random.randint(30000, 150000)
        depid = random.choice(departments)
        writer.writerow([empid, first, last, email, phone, salary, depid])

# ============================================================================
# 13. EMPLOYEE_DETAILS_FLAT table (for wf_s_m_complex5 - flat file source)
# ============================================================================
print("Generating employee_details_flat.csv...")
with open("employee_details_flat.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["EMPID", "FIRSTNAME", "LASTNAME", "EMAIL", "PHONENO", "SALARY", "DEPID"])
    for i in range(51, 76):
        empid = i
        first = random.choice(first_names)
        last = random.choice(last_names)
        email = generate_email(first, last, empid)
        phone = generate_phone()
        salary = random.randint(30000, 150000)
        depid = random.choice(departments)
        writer.writerow([empid, first, last, email, phone, salary, depid])

# ============================================================================
# 14. TARGET_LOOKUP table (for wf_s_m_complex5 - existing records for update strategy)
# ============================================================================
print("Generating target_lookup.csv...")
with open("target_lookup.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["T_EMPID", "EMAIL", "PHONENO", "SALARY", "DEPID", "DNAME", "ENAME", "ELOC"])
    # Add some existing records (subset of employees)
    for i in [1, 5, 10, 15, 20, 25, 30, 35, 40, 45]:
        first = random.choice(first_names)
        last = random.choice(last_names)
        email = generate_email(first, last, i)
        phone = generate_phone()
        salary = random.randint(30000, 150000)
        depid = random.choice(departments)
        dname = dept_names[depid]
        ename = f"{first} {last}"
        eloc = random.choice(locations)
        writer.writerow([i, email, phone, salary, depid, dname, ename, eloc])

print("\nAll sample data files generated successfully!")
print("Files created:")
for f in sorted(os.listdir(".")):
    if f.endswith(".csv"):
        print(f"  - {f}")
