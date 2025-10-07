import pandas as pd
from faker import Faker
import random

# Initialize Faker instance
fake = Faker()

# Function to generate dummy data for the source_doctors table
def generate_doctors_data(num_records):
    doctors_data = []
    
    for i in range(1, num_records + 1):
        # Generate the doctor_id with an incremented prefix
        prefix = f"B{i-1}"  # Prefix will start at "B0", "B1", "B2", ...
        doctor_id = f"{prefix}-{i:03d}"  # e.g., B0-001, B1-002, ...
        
        first_name = fake.first_name()
        last_name = fake.last_name()
        specialty = random.choice(['Cardiology', 'Neurology', 'Orthopedics', 'Pediatrics', 'General Surgery'])
        phone_number = fake.phone_number()
        email = fake.email()
        
        doctors_data.append({
            'doctor_id': doctor_id,
            'first_name': first_name,
            'last_name': last_name,
            'specialty': specialty,
            'phone_number': phone_number,
            'email': email
        })
    
    return pd.DataFrame(doctors_data)

# Function to generate dummy data for the source_patients table
def generate_patients_data(num_records):
    patients_data = []
    
    for i in range(1, num_records + 1):
        # patient_id = fake.uuid4()  # UUID for patient_id
        prefix = f"P{i-1}"  # Prefix will start at "B0", "B1", "B2", ...
        patient_id = f"{prefix}-{i:03d}"  # e.g., B0-001, B1-002, ...
        first_name = fake.first_name()
        last_name = fake.last_name()
        date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=90)
        gender = random.choice(['Male', 'Female', 'Other'])
        address = fake.address().replace("\n", ", ")
        phone_number = fake.phone_number()
        email = fake.email()
        primary_doctor_id = f"B{random.randint(0, num_records-1)}-{random.randint(1, num_records):03d}"  # Random doctor ID
        
        patients_data.append({
            'patient_id': patient_id,
            'first_name': first_name,
            'last_name': last_name,
            'date_of_birth': date_of_birth,
            'gender': gender,
            'address': address,
            'phone_number': phone_number,
            'email': email,
            'primary_doctor_id': primary_doctor_id
        })
    
    return pd.DataFrame(patients_data)

# Generate 10 dummy records for each table
num_records = 1000000
doctors_df = generate_doctors_data(num_records)
patients_df = generate_patients_data(num_records)

# Display the dummy data
# print("Doctors Table:")
print(doctors_df)
# print("\nPatients Table:")
print(patients_df)

# Optionally, save the data to CSV
doctors_df.to_csv('doctors_data.csv', index=False)
patients_df.to_csv('patients_data.csv', index=False)
