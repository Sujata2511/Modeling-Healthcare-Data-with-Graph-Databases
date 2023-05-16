from py2neo import Graph, Node, Relationship
import csv
import exel_slice

# TODO: Change the project name & password within auth()
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))
i = 0
my_dic = {}

# ------- Creating Data set -------
patient_number = int(input("Enter the number of patient: "))+1
exel_slice.data_set_creat(patient_number)

# -------Check if patient node is present or not
def patient_not_exist(v):
    f = 0
    for key in my_dic:
        # patient node is already created return 0
        if (key == v):
            return v
    # else return 1
    return 1


# ------- PATIENT NODE CREATED & SAVE IT IN DICTIONARY --------
with open('10_Patient_data\Patient_Details_Table.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # For reading 50 patient node only
        var = row['PatientID']
        if patient_not_exist(var):
            # New Patient node created in Databaase &
            # Level name is "PNODE"
            p_node = Node("PNODE", name=row['PatientID'], gender=row['PatientGender'], DOB=row['PatientDateOfBirth'], race=row['PatientRace'],
                          maritalStatus=row['PatientMaritalStatus'], language=row['PatientLanguage'], poverty=row['PatientPopulationPercentageBelowPoverty'])
            graph.create(p_node)
            # Creating Dictionary
            # key=>Patient_ID: Value=>[(Patient_Id_number,Patient_Node_ID)]
            my_dic[(row['PatientID'])] = [(row['PatientID'], p_node.identity)]


# ----------ADMISSION NODE IS CREATED & SAVE IT IN DICTIONARY-------------
with open('10_Patient_data\Admission_Details.csv', newline='') as csvfile2:
    reader = csv.DictReader(csvfile2)
    for row in reader:
        p_name = row['PatientID']
        # Store Admission value in the my_dict list
        for key in my_dic:
            # Find the specific Patient Id
            if (key == p_name):
                # Create Admission node & level => "ADMISSION"
                a_node = Node(
                    "ADMISSION", name=row['Pid_AdNo'], admission_start_date=row['AdmissionStartDate'], admission_end_date=row['AdmissionEndDate'])
                graph.create(a_node)
                # Add to the exsisting patient list as a pair
                # key => Patient_Id : vlaue => (PatientId_AdmissionID, Admission_Node_Id)
                my_dic[key].append((row['Pid_AdNo'], a_node.identity))

# -----Sort the list of values in dictionary------
for key in my_dic:
    my_dic[key] = sorted(my_dic[key])

# ------CREATING NEXT RELATION OF NODES-------
for key in my_dic:
    # Link patient node with 1st admission of the patient
    # "my_dic[key][0][1]" => Node_id
    n1 = graph.nodes.get(my_dic[key][0][1])
    n2 = graph.nodes.get(my_dic[key][1][1])
    rel = Relationship(n1, "admit", n2)
    graph.create(rel)
    # Link all the next admission nodes
    for i in range(1, len(my_dic[key])-1):
        # Link all the list values
        n1 = graph.nodes.get(my_dic[key][i][1])
        n2 = graph.nodes.get(my_dic[key][i+1][1])
        rel = Relationship(n1, "admit", n2)
        graph.create(rel)

# --------------CREATING PATIENT DESCRIPTION NODE---------------
with open('10_Patient_data\Diagnosis_Report.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        p_name = row['PatientID']
        for key in my_dic:
            # Find the spasic Node
            if (key == p_name):
                # Create Diagnosis Node
                # Level name: "DIAGNOSIS"
                diag_node = Node("DIAGNOSIS", name="Diagnoses",
                                 dia_code=row['PrimaryDiagnosisCode'], dia_desc=row['PrimaryDiagnosisDescription'])
                graph.create(diag_node)
                # link with Admission node
                ad_no = row['Pid_AdNo']
                for i in range(1, len(my_dic[key])):
                    # Find out spesic patient_id with their admission_ids
                    if (my_dic[key][i][0] == ad_no):
                        n = graph.nodes.get((my_dic[key][i][1]))
                        rel = Relationship(n, "has_Desc", diag_node)
                        graph.create(rel)

# --------------CREATING PATIENT DESCRIPTION NODE---------------
with open('10_Patient_data\Lab_Report.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        pa_name = row['PatientID']
        for key in my_dic:
            # Find the spasic Node
            if (key == pa_name):
                # Create Lab Node
                # Level name: "LAB REPORT"
                lab_node = Node("LAB", name=row['LabName'], lab_value=row['LabValue'],
                                lab_units=row['LabUnits'], test_date=row['LabDateTime'])
                graph.create(lab_node)
                # link with Admission node
                ad_no = row['Pid_AdNo']
                for i in range(1, len(my_dic[key])):
                    # Find out spesic patient_id with their admission_ids
                    if (my_dic[key][i][0] == ad_no):
                        n = graph.nodes.get((my_dic[key][i][1]))
                        rel = Relationship(n, "tesed", lab_node)
                        graph.create(rel)

print("ok\n")
