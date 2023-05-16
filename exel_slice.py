import csv
from itertools import islice
import pandas as pd
import random

# Open the input CSV file for reading
def data_set_creat(num):
    with open('Patient_data\PatientCorePopulatedTable.csv', 'r') as infile:

        # Create a reader object
        reader = csv.reader(infile)

        # Slice the first 10 rows of the CSV file
        sliced_rows = islice(reader, num)

        # Create a new CSV file for writing
        with open('10_Patient_data\Patient_Details_Table.csv', 'w', newline='') as outfile:

            # Create a writer object
            writer = csv.writer(outfile)

            # Write the sliced rows to the new CSV file
            for row in sliced_rows:
                writer.writerow(row)

# Making a filter list according to patient id
    filter_df = pd.read_csv('10_Patient_data\Patient_Details_Table.csv')
    # Create a list of values to filter by
    filter_list = filter_df['PatientID'].tolist()

# 1.Making Diagnosis Data set
    # Load the original CSV file
    org_diagnosis = pd.read_csv('Patient_data\AdmissionsDiagnosesCorePopulatedTable.csv')

    # Filter the original DataFrame by the list of values
    filtered_org_diagnosis = org_diagnosis[org_diagnosis['PatientID'].isin(filter_list)]

    # Save the filtered data to a new CSV file
    filtered_org_diagnosis.to_csv('10_Patient_data\Diagnosis_Report.csv', index=False)

# 2.Making Admission dataset
    org_admission = pd.read_csv('Patient_data\AdmissionsCorePopulatedTable.csv')
    filtered_org_admission = org_admission[org_admission['PatientID'].isin(filter_list)]
    filtered_org_admission.to_csv('10_Patient_data\Admission_Details.csv', index=False)

# 3.Making labreport dataset
    org_lab_report = pd.read_csv('Patient_data\LabsCorePopulatedTable.csv')
    filtered_org_lab_report = org_lab_report[org_lab_report['PatientID'].isin(filter_list)]
    filtered_org_lab_report.to_csv('10_Patient_data\Lab_Report.csv', index=False)       

