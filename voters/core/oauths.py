import jwt, environ, requests, json
from jwt import InvalidTokenError

env = environ.Env()


class MicrosoftAuth:
    def __init__(self):
        # Fetch configuration from environment variables
        self.microsoft_public_key_url = env('MICROSOFT_PUBLIC_KEY_URL')
        self.microsoft_client_id = env('MICROSOFT_CLIENT_ID')
        self.microsoft_client_secret = env('MICROSOFT_CLIENT_SECRET')
        self.microsoft_tenant_id = env('MICROSOFT_TENANT_ID')
        self.token_url = f'https://login.microsoftonline.com/{self.microsoft_tenant_id}/oauth2/v2.0/token'
        self.graph_api_url = 'https://graph.microsoft.com/v1.0'
        self.attendance_group_id = env('ATTENDANCE_GROUP_ID')
        self.teacher_group_id = env('TEACHER_GROUP_ID')
        self.admin_group_id = env('ADMIN_GROUP_ID')
        self.read_only_admin_group_id = env('READ_ONLY_ADMIN_GROUP_ID')
        self.accounts_group_id = env('ACCOUNTS_GROUP_ID')
        if not all([self.microsoft_public_key_url, self.microsoft_client_id, self.microsoft_client_secret,
                    self.microsoft_tenant_id]):
            raise ValueError("One or more environment variables are not set.")

        self.public_keys = self.get_microsoft_public_keys()

    def get_microsoft_public_keys(self):
        response = requests.get(self.microsoft_public_key_url)
        jwks = response.json()
        return jwks['keys']

    def verify_token(self, token):
        # Try all available keys to decode the token
        for key in self.public_keys:
            try:
                public_key = jwt.algorithms.RSAAlgorithm.from_jwk(key)
                decoded_token = jwt.decode(token, public_key, algorithms=['RS256'], audience=self.microsoft_client_id)
                return decoded_token
            except InvalidTokenError:
                continue
        raise InvalidTokenError("Invalid token.")

    def decode_access_token(self, token):
        decoded = jwt.decode(token, algorithms=["RS256"], options={"verify_signature": False})
        return decoded

    def get_microsoft_email(self, token):
        decoded_token = self.decode_access_token(token)
        return decoded_token.get('unique_name')

    def get_access_token(self):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'grant_type': 'client_credentials',
            'client_id': self.microsoft_client_id,
            'client_secret': self.microsoft_client_secret,
            'scope': 'https://graph.microsoft.com/.default',
        }

        response = requests.post(self.token_url, headers=headers, data=data)
        response_data = response.json()

        if 'access_token' in response_data:
            return response_data['access_token']
        else:
            raise Exception('Failed to retrieve access token')

    def get_user_license_details(self, user_id, access_token):
        url = f'{self.graph_api_url}/users/{user_id}/licenseDetails'
        headers = {
            'Authorization': f'Bearer {access_token}',
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()  # License details of the user
        else:
            raise Exception(f'Error fetching license details: {response.status_code}, {response.text}')

    def get_user_group_ids(self, auth_token):
        """
        Fetch the group IDs for the user based on the provided authentication token.

        :param auth_token: The user's auth token (access token or ID token).
        :return: A list of group IDs the user is a member of.
        """
        try:
            # Decode the token to get the user's object ID (oid)
            decoded_token = self.decode_access_token(auth_token)
            user_id = decoded_token.get('oid')

            # Get an access token to query Microsoft Graph API
            access_token = self.get_access_token()

            # Query the user's group memberships via Microsoft Graph API
            url = f'{self.graph_api_url}/users/{user_id}/memberOf'
            headers = {
                'Authorization': f'Bearer {access_token}',
            }

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                group_memberships = response.json().get('value', [])
                group_ids = [group['id'] for group in group_memberships]

                return group_ids  # Return the list of group IDs
            else:
                raise Exception(f'Error fetching group memberships: {response.status_code}, {response.text}')

        except Exception as e:
            # Log the error if needed
            print(f'Error in get_user_group_ids: {e}')
            return []

    def is_in_teacher_group(self,group_ids):
        try:
            return self.teacher_group_id in group_ids
        except Exception as e:
            return False
        
    def is_in_admin_group(self,group_ids):
        try:
            return self.admin_group_id in group_ids
        except Exception as e:
            return False
    
    def is_in_read_only_admin_group(self,group_ids):
        try:
            return self.read_only_admin_group_id in group_ids
        except Exception as e:
            return False
        
    def is_in_accounts_group(self,group_ids):
        try:
            return self.accounts_group_id in group_ids
        except Exception as e:
            return False

    def is_faculty(self, auth_token):
        try:
            decoded_token = self.decode_access_token(auth_token)
            user_id = decoded_token.get('oid')
            access_token = self.get_access_token()
            user_license_details = self.get_user_license_details(user_id, access_token)
            sku_part_number = user_license_details['value'][0]['skuPartNumber']
            if sku_part_number == "STANDARDWOFFPACK_FACULTY":
                return True
            else:
                return False
        except Exception as e:
            return False


# Example usage of ZohoCRMUtility in a Django utility
class ZohoCRMUtility:
    def __init__(self):
        """
        Initialize the utility by loading credentials directly from environment variables and getting an OAuth token.
        """
        self.refresh_token = env("ZOHO_REFRESH_TOKEN")  # Use env() to access the environment variable
        self.client_id = env("ZOHO_CLIENT_ID")
        self.client_secret = env("ZOHO_CLIENT_SECRET")
        self.base_url = "https://www.zohoapis.com.au/crm/v7"
        self.token_url = "https://accounts.zoho.com.au/oauth/v2/token"
        self.headers = {
            "Content-Type": "application/json; charset=utf-8"
        }
        self.get_oauth_token()

    def get_oauth_token(self):
        """
        Generate OAuth token using the refresh token.
        """
        params = {
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "ZohoCRM.modules.ALL",
            "grant_type": "refresh_token"
        }
        response = requests.post(self.token_url, data=params)
        if response.status_code == 200:
            access_token = response.json().get('access_token')
            self.headers["Authorization"] = f"Zoho-oauthtoken {access_token}"
        else:
            raise Exception("Failed to get OAuth token")

    def search_student_by_id(self, student_id):
        """
        Search for a student by their student ID in Zoho CRM.
        """
        url = f"{self.base_url}/Contacts/search?criteria=CIHE_Student_ID:equals:{student_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json().get('data', [])
            if len(data) == 1:
                return data[0]
            elif len(data) > 1:
                raise Exception("Multiple students found with this Student ID.")
            else:
                raise Exception("No student found with this Student ID.")
        else:
            raise Exception("Failed to search student.")

    def create_deal(self, student_crm_id, student_id):
        """
        Create a deal for the student in the Zoho CRM.
        """
        url = f"{self.base_url}/Deals"
        deal_data = [{
            "Contact_Name": student_crm_id,
            "Pipeline": "Attendance Progression",
            "Deal_Name": f"Testing Name - {student_id}",
            "Stage": "Red List Students",
            "Layout": "59387000000000631"
        }]
        response = requests.post(url, headers=self.headers, data=json.dumps({"data": deal_data}))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to create deal: {response.json()}")

    def process_student_deal(self, student_id):
        """
        Search for the student and create a deal in Zoho CRM.
        This is a single method to handle both tasks.
        """
        try:
            # Search for student
            student = self.search_student_by_id(student_id)
            student_crm_id = student['id']

            # Create deal for the student
            deal_response = self.create_deal(student_crm_id, student_id)
            return deal_response

        except Exception as e:
            return str(e)