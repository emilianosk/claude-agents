from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class SalesforceConfig:
    base_login_url: str = ''
    base_url: str = ''
    client_id: str = ''
    client_secret: str = ''
    refresh_token: str = ''
    api_version: str = 'v59.0'
    timeout_seconds: int = 30
    ssl_verify: bool = True


class SalesforceService:
    """Salesforce API query infrastructure.

    Focused service for OAuth auth + REST/SOQL query access.
    """

    def __init__(self, config: SalesforceConfig) -> None:
        self.config = config
        self._access_token: str | None = None
        self._instance_url: str | None = None
        self._expires_at: float = 0

    def validate_config(self) -> dict[str, Any]:
        required = {
            'SALESFORCE_BASE_LOGIN_URL': self.config.base_login_url,
            'SALESFORCE_CLIENT_ID': self.config.client_id,
            'SALESFORCE_CLIENT_SECRET': self.config.client_secret,
            'SALESFORCE_REFRESH_TOKEN': self.config.refresh_token,
        }
        errors = [f'Missing required Salesforce configuration: {k}' for k, v in required.items() if not v]
        return {'valid': len(errors) == 0, 'errors': errors}

    def get_token(self, force_refresh: bool = False) -> dict[str, Any]:
        if not force_refresh and self._access_token and time.time() < self._expires_at:
            return {'access_token': self._access_token, 'instance_url': self._instance_url}

        token_url = f"{self.config.base_login_url.rstrip('/')}/services/oauth2/token"
        payload = {
            'grant_type': 'refresh_token',
            'client_id': self.config.client_id,
            'client_secret': self.config.client_secret,
            'refresh_token': self.config.refresh_token,
        }

        response = requests.post(
            token_url,
            data=payload,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=self.config.timeout_seconds,
            verify=self.config.ssl_verify,
        )
        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(f'Salesforce auth failed: {response.status_code} {response.text}')

        body = response.json()
        token = body.get('access_token')
        instance_url = body.get('instance_url') or self.config.base_url
        if not token or not instance_url:
            raise RuntimeError(f'Invalid Salesforce token response: {response.text}')

        # Salesforce refresh-token response may omit expires_in; keep short local cache window.
        expires_in = int(body.get('expires_in', 1800))
        self._access_token = token
        self._instance_url = instance_url
        self._expires_at = time.time() + max(60, expires_in - 60)

        return {'access_token': token, 'instance_url': instance_url}

    def request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        auth = self.get_token()
        base = auth['instance_url'].rstrip('/')
        url = f"{base}/{path.lstrip('/')}"

        response = requests.request(
            method.upper(),
            url,
            headers={
                'Authorization': f"Bearer {auth['access_token']}",
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            params=params or None,
            json=json_body,
            timeout=self.config.timeout_seconds,
            verify=self.config.ssl_verify,
        )

        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(f'Salesforce request failed ({response.status_code}): {response.text}')

        return response.json() if response.text else {}

    def query(self, soql: str) -> dict[str, Any]:
        api = self.config.api_version.strip() or 'v59.0'
        return self.request('GET', f'/services/data/{api}/query', params={'q': soql})

    def query_all(self, soql: str, max_pages: int = 50) -> list[dict[str, Any]]:
        first = self.query(soql)
        records = list(first.get('records', []))
        next_url = first.get('nextRecordsUrl')
        pages = 1

        while next_url and pages < max_pages:
            page = self.request('GET', next_url)
            records.extend(page.get('records', []))
            next_url = page.get('nextRecordsUrl')
            pages += 1

        return records

    def test_connection(self) -> dict[str, Any]:
        result = self.query('SELECT Id FROM Organization LIMIT 1')
        return {
            'success': True,
            'totalSize': result.get('totalSize', 0),
            'done': result.get('done', True),
        }

    def query_service_not_canceled_appointments(
        self,
        lid_number: str,
        start_datetime: str,
        end_datetime: str,
    ) -> dict[str, Any]:
        escaped_lid = lid_number.replace("'", "\\'")
        soql = (
            'SELECT Id, ServiceAppointmentId, ServiceResourceId,'
            ' ServiceResource.Name, ServiceResource.ResourceType, ServiceResource.AccountId,'
            ' ServiceResource.IsPrimary, ServiceResource.RelatedRecord.Username, ServiceResource.RelatedRecord.Email,'
            ' ServiceAppointment.Id, ServiceAppointment.AppointmentNumber, ServiceAppointment.Appointment_Source__c,'
            ' ServiceAppointment.SchedStartTime, ServiceAppointment.SchedEndTime, ServiceAppointment.Customer_Appointment_Start__c,'
            ' ServiceAppointment.Assigned_Piercer__c, ServiceAppointment.Assigned_Piercer__r.Id,'
            ' ServiceAppointment.Assigned_Piercer__r.FirstName, ServiceAppointment.Assigned_Piercer__r.LastName,'
            ' ServiceAppointment.Assigned_Piercer__r.Email, ServiceAppointment.Assigned_Piercer__r.Employee_PID__c,'
            ' ServiceAppointment.Status, ServiceAppointment.ServiceTerritoryId,'
            ' ServiceAppointment.ServiceTerritory.Name, ServiceAppointment.ServiceTerritory.LID_Number__c'
            ' FROM AssignedResource'
            f" WHERE ServiceAppointment.ServiceTerritory.LID_Number__c = '{escaped_lid}'"
            " AND ServiceAppointment.Status != 'Canceled'"
            f' AND ServiceAppointment.SchedStartTime >= {start_datetime}'
            f' AND ServiceAppointment.SchedEndTime <= {end_datetime}'
        )
        payload = self.query(soql)
        return {
            'success': True,
            'totalSize': payload.get('totalSize', 0),
            'done': payload.get('done', True),
            'records': payload.get('records', []),
            'nextRecordsUrl': payload.get('nextRecordsUrl'),
        }

    def get_appointments_not_canceled_for_date(self, lid_number: str, date_yyyy_mm_dd: str) -> dict[str, Any]:
        return self.query_service_not_canceled_appointments(
            lid_number,
            f'{date_yyyy_mm_dd}T00:00:00Z',
            f'{date_yyyy_mm_dd}T23:59:59Z',
        )

    def get_appointments_not_canceled_for_date_range(
        self,
        lid_number: str,
        start_date_yyyy_mm_dd: str,
        end_date_yyyy_mm_dd: str,
    ) -> dict[str, Any]:
        return self.query_service_not_canceled_appointments(
            lid_number,
            f'{start_date_yyyy_mm_dd}T00:00:00Z',
            f'{end_date_yyyy_mm_dd}T23:59:59Z',
        )

    def get_work_type_by_sid(self, service_id: str) -> dict[str, Any]:
        sid = service_id.replace("'", "\\'")
        soql = (
            'SELECT Id, Name, Service_ID__c, Work_Type_Star_Rating__c, '
            'EstimatedDuration, Prep_Time__c, Piercing_Time__c, Clean_Up_Time__c, Quantity_Slot__c '
            'FROM WorkType '
            f"WHERE Service_ID__c = '{sid}'"
        )
        payload = self.query(soql)
        records = payload.get('records', [])
        return records[0] if records else {}
