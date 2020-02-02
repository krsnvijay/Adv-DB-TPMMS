from faker import Faker
import random

MAX_RECORDS = 1500000
MAX_EMPLOYEE_IDS = 100000
fake = Faker()
file = open("records.txt", "w")
emp_ids = random.sample(range(10000000, 99999999), MAX_EMPLOYEE_IDS)
dept_ids = random.sample(range(100, 999), 100)
generated_records = 0
while generated_records <= MAX_RECORDS:
    profile = fake.profile(fields=['name','sex','address','ssn'])
    if len(profile['name']) > 25 or len(profile['address']) > 43:
        continue

    if profile['sex'] == 'F':
        profile['sex'] = 0
    else:
        profile['sex'] = 1
    profile['ssn'] = profile['ssn'].replace('-', '')
    profile['address'] = profile['address'].replace('\n', ' ')
    profile['dept'] = random.choice(dept_ids)
    profile['lastUpdate'] = fake.date(pattern='%Y-%m-%d', end_datetime=None)
    profile['empId'] = random.choice(emp_ids)

    tuple = f'{profile["empId"]:8}{profile["lastUpdate"]:10}{profile["name"]:25}{profile["sex"]:1}{profile["dept"]:3}{profile["ssn"]:9}{profile["address"]:43}\n'
    file.write(tuple)
    generated_records += 1
    print(generated_records)
file.close()
print("generated records successfully")
