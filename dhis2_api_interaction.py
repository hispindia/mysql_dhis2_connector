# dhis2_api_interaction.py
# Author: sourabhB
import requests
import json
import base64
from requests.auth import HTTPBasicAuth
from datetime import datetime

dhis2_api_url = "https://links.hispindia.org/api"
dhis2_username = "###"
dhis2_password = "##"
org_unit_api_url = f"{dhis2_api_url}organisationUnits"
options_api_url = f"{dhis2_api_url}options"
enrollment_endpoint = f"{dhis2_api_url}trackedEntityInstances"

def get_org_unit_and_option_data(PermSubDistrictId, PermVillageId):
    filters = [
        f"attributeValues.attribute.id:eq:l38VgCtdLFD",
        f"attributeValues.value:eq:{PermSubDistrictId}",
        "level:eq:4"
    ]

    url_with_filters = f"{org_unit_api_url}?fields=id,name,level,attributeValues&filter={'&filter='.join(filters)}"

    filters1 = [
        f"attributeValues.attribute.id:eq:RDZKbFFn7EL",
        f"attributeValues.value:eq:{PermVillageId}"
    ]

    url_with_filters1 = f"{options_api_url}?fields=id,displayName,attributeValues&filter={'&filter='.join(filters1)}"

    response_org_unit = requests.get(url_with_filters, auth=HTTPBasicAuth(dhis2_username, dhis2_password))
    response_options = requests.get(url_with_filters1, auth=HTTPBasicAuth(dhis2_username, dhis2_password))

    if response_org_unit.status_code == 200 and response_options.status_code == 200:
        response_data_org_unit = response_org_unit.json()
        response_data_options = response_options.json()

        org_unit_data = response_data_org_unit.get('organisationUnits', [])
        option_data = response_data_options.get('options', [])

        if org_unit_data:
            for org_unit in org_unit_data:
                org_unit_id = org_unit['id']
                org_unit_name = org_unit['name']
        else:
            error_message = f"No data received for PermSubDistrictId-- {PermSubDistrictId}"
            print(error_message)

        if option_data:
            option_id = option_data[0]['id']
            option_name = option_data[0]['displayName']
        else:
            error_message = f"No option data PermVillageId-- {PermVillageId}."
            print(error_message)
    else:
        print(f"Failed to retrieve organization units. Status code: {response_org_unit.status_code}")
        print(f"Failed to retrieve options. Status code: {response_options.status_code}")

    return org_unit_id, option_name

def create_enrollment(enrollment_data, org_unit_id):
    CreatedDate = enrollment_data["attributes"][3]["value"]
    CreatedDate = datetime.strptime(CreatedDate, "%Y-%m-%d").strftime("%Y-%m-%d")

    enrollment_data["orgUnit"] = org_unit_id
    enrollment_data["enrollments"][0]["orgUnit"] = org_unit_id
    enrollment_data["enrollments"][0]["enrollmentDate"] = CreatedDate
    enrollment_data["enrollments"][0]["incidentDate"] = CreatedDate

    response = requests.post(
        enrollment_endpoint,
        data=json.dumps(enrollment_data),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {base64.b64encode(f'{dhis2_username}:{dhis2_password}'.encode()).decode()}"
        }
    )

    return response

def check_existing_tei(beneficiary_reg_id):
    tei_search_url = f"{enrollment_endpoint}?ouMode=ALL&program=vyQPQ07JB9M&filter=HKw3ToP2354:eq:{beneficiary_reg_id}"

    response = requests.get(tei_search_url, auth=HTTPBasicAuth(dhis2_username, dhis2_password))
    if response.status_code == 200:
        response_data = response.json()
        teis = response_data.get('trackedEntityInstances', [])
        return teis 
    else:
        return []
