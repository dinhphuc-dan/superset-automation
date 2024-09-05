from typing import Any, Generator
import requests
from urllib.parse import urljoin
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, text, inspect, Boolean
from functools import lru_cache 
import time

load_dotenv(override=True)

@lru_cache()
def handle_user_input(fuction_type, company_name):   
    '''
    return necessary info for superset based on input
    '''
    app_related_role = ['mkt', 'all_users']
    performance_related_role = ['mkt']

    if fuction_type == 'app_related_role':
        company_name = company_name
        bigquery_table_env = f'{company_name.upper()}_APP_TABLE_ID'
        bigquery_table_id = os.getenv(bigquery_table_env)
        if company_name.lower() == 'govo':
            bigquery_object_query = (
                    f" SELECT " 
                        f" concat(app_name, '_', lower(platform)) as object_name, " 
                        f" 'app' as group_key, " 
                        f" concat('app_name = \\'', app_name, '\\'') as clause, "
                        f" concat('auto_{company_name.upper()}_', app_name, '_', lower(platform)) as role_level_security_name, " 
                    f" FROM `{bigquery_table_id}` "
                    f" WHERE end_date is null "
            )
        else:
            bigquery_object_query = (
                    f" SELECT " 
                        f" concat(app_name, '_', lower(platform)) as object_name, " 
                        f" 'app' as group_key, " 
                        f" concat('app_name = \\'', app_name, '\\' and platform = \\'', lower(platform), '\\'') as clause, "
                        f" concat('auto_{company_name.upper()}_', app_name, '_', lower(platform)) as role_level_security_name, " 
                    f" FROM `{bigquery_table_id}` "
                    f" WHERE end_date is null "
            )
        list_dataset =[company_name + '_' + i for i in app_related_role]
        table_id_query_condition = None
        row_level_security_id_query_condition = list_dataset[0]
        
    elif fuction_type == 'mkt_performance_related_role':
        company_name = 'volio'
        bigquery_table_env = 'VOLIO_MKT_LEVEL_TABLE_ID'
        bigquery_table_id = os.getenv(bigquery_table_env)
        bigquery_object_query = (
                f" SELECT "
                    f" distinct marketer_name as object_name, "
                    f" 'mkt' as group_key, " 
                    f" concat('marketer_name = \\'', marketer_name, '\\'') as clause, " 
                    f" concat('auto_{company_name.upper()}_MKT_', marketer_name) as role_level_security_name, "
                f" FROM `{bigquery_table_id}` "
                f" WHERE end_date is null and marketer_name not in ('vuonglt', 'InInventory', 'anhbqt')"
        )
        list_dataset =[company_name + '_' + i + '_performance' for i in performance_related_role]
        table_id_query_condition = "table_name not in ('volio_marketer_commisison_table', 'volio_ranking_marketer_performance', 'volio_app_of_marketer_all_companies')"
        row_level_security_id_query_condition = list_dataset[0]
    
    elif fuction_type == 'update_permission_mkt_app':
        company_name = company_name
        bigquery_table_env = f'{company_name.upper()}_APP_MKT_TABLE_ID'
        bigquery_table_id = os.getenv(bigquery_table_env)
        if company_name.lower() == 'govo':
            bigquery_object_query = (
                    f" SELECT " 
                        f" app_name, " 
                        f" 'Android' as platform, " 
                        f" marketer_name, " 
                        f" \'{company_name.lower()}\' as company_name, "
                    f" FROM `{bigquery_table_id}` "
                    f" WHERE end_date is null and marketer_name not in ('vuonglt', 'InInventory', 'anhbqt')"
            )
        else:
            bigquery_object_query = (
                    f" SELECT " 
                        f" app_name, " 
                        f" platform, " 
                        f" marketer_name, " 
                        f" \'{company_name.lower()}\' as company_name, "
                    f" FROM `{bigquery_table_id}` "
                    f" WHERE end_date is null and marketer_name not in ('vuonglt', 'InInventory', 'anhbqt')"
            )
        list_dataset =None
        table_id_query_condition = None
        row_level_security_id_query_condition = None
    elif fuction_type == 'update_permission_mkt_mkt':
        pass

    return bigquery_object_query, list_dataset, row_level_security_id_query_condition, table_id_query_condition, company_name


class Superset():
    '''
    Class for interacting with superset API including: 
    + clone data to a postgres database 
    + create role and update role
    + create and update row_level_security
    '''
    list_table = [
        {   
            'id': 0,
            'table_name': 'superset_permission_resources',
            'table_schema':{
                'id': {'type': Integer},
                'permission_name':{'type': String},
                'view_menu_name': {'type': String}
            },
            'path': 'security/permissions-resources/',
            'request_method': 'GET',
            'response_schema': {
                'id': {'type': Integer},
                'permission':{'type': object, 'properties': {'name': {'type': String}}},
                'view_menu': {'type': object, 'properties': {'name': {'type': String}}}
            }
        },
        {
            'id': 1,
            'table_name': 'superset_roles',
            'table_schema':{
                'id': {'type': Integer},
                'name': {'type': String}
            },
            'path': 'security/roles/',
            'request_method': 'GET',
            'response_schema': {
                'id': {'type': Integer},
                'name': {'type': String}
            }
        },
        {
            'id': 2,
            'table_name': 'superset_tables',
            'table_schema':{
                'id': {'type': Integer},
                'database_id': {'type': Integer},
                'database_database_name': {'type': String},
                'datasource_type': {'type': String},
                'schema': {'type': String},
                'table_name': {'type': String}

            },
            'path': 'dataset/',
            'request_method': 'GET',
            'response_schema': {
                'id': {'type': Integer},
                'database': {'type': object, 'properties': {'database_name': {'type': String}, 'id': {'type': Integer}}},
                'datasource_type': {'type': String},
                'schema': {'type': String},
                'table_name': {'type': String}
            }
        },
        {
            'id': 3,
            'table_name': 'superset_role_level_security',
            'table_schema':{
                'id': {'type': Integer},
                'name': {'type': String},
                'roles_id': {'type': String},
                'roles_name': {'type': String},
                'tables_id': {'type': String},
                'tables_schema': {'type': String},
                'tables_table_name': {'type': String}
            },
            'path': 'rowlevelsecurity/',
            'request_method': 'GET',
            'response_schema': {
                'id': {'type': Integer},
                'name': {'type': String},
                'roles': {'type': 'Array_object', 'properties': {'name': {'type': String}, 'id': {'type': Integer}}},
                'tables': {'type': 'Array_object', 'properties': {'id': {'type': Integer}, 'schema': {'type': String}, 'table_name': {'type': String}}}
            }
        },
        {
            'id': 4,
            'table_name': 'superset_resouces',
            'table_schema':{
                'id': {'type': Integer},
                'name': {'type': String}
            },
            'path': 'security/resources/',
            'request_method': 'GET',
            'response_schema': {
                'id': {'type': Integer},
                'name': {'type': String}
            }
        },
        {
            'id': 5,
            'table_name': 'superset_users',
            'table_schema':{
                'id': {'type': Integer},
                'username': {'type': String},
                'active': {'type': Boolean},
                'first_name': {'type': String},
                'last_name': {'type': String},
                'roles_id': {'type': String},
                'roles_name': {'type': String}

            },
            'path': 'security/users/',
            'request_method': 'GET',
            'response_schema': {
                'id': {'type': Integer},
                'username': {'type': String},
                'active': {'type': Boolean},
                'first_name': {'type': String},
                'last_name': {'type': String},
                'roles': {'type': 'Array_object', 'properties': {'name': {'type': String}, 'id': {'type': Integer}}}
            }
        }
    ]

    page_size = 100
    '''following format: postgresql+psycopg2://username:password@host:port/database'''
    postgres_url = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    
    def __init__(self, base_url ,username, password, access_token_as_dict=None):
        self.base_url = base_url
        self.username = username
        self.user_password = password
        self._access_token_and_expire_time = access_token_as_dict
        self._check_data_between_superset_and_postgres()

    def _setup_postgres(self):
        try: 
            self.engine = create_engine(self.postgres_url)
            return self.engine
        except Exception as e:
            raise e

    def _setup_postgres_table(self):
        self._setup_postgres()
        metadata = MetaData()
        for table in self.list_table:
            if not inspect(self.engine).has_table(table_name=table['table_name']):
                    try: 
                        Table(
                            table['table_name'], metadata,
                            *(Column(column_name, column_type['type']) for column_name, column_type in table['table_schema'].items()),
                        )
                        
                        metadata.create_all(bind=self.engine)
                    except Exception as e:
                        raise e
    
    def _prepare_final_url(self, path, query_params = None):
        if query_params:
            final_url = self.base_url + path + query_params
        else:
            final_url = self.base_url + path
        return final_url

    def _get_superset_access_token_using_login(self) -> dict:
        request_method = 'POST'
        path = 'security/login'
        response = requests.request(
            method=request_method,
            url=self._prepare_final_url(path),
            json={
                'username': self.username,
                'password': self.user_password,
                'provider': 'db',
                'refresh': True
            }

        )
        if self._handle_response_status_code(response = response):
            access_token = response.json()['access_token']
            access_token_expire_time = datetime.now() + timedelta(seconds=3590)
            access_token_as_dict = {}
            access_token_as_dict.update({
                'access_token': access_token,
                'access_token_expire_time': access_token_expire_time
            })

            return access_token_as_dict
    def _get_csrf_token(self, headers):
        request_method = 'GET'
        path = 'security/csrf_token'
        response = requests.request(
                            method=request_method,
                            url=self._prepare_final_url(path),
                            headers=headers
        )
        if self._handle_response_status_code(response = response):
            return {
                'csrf_token': response.json()['result'],
                'csrf_url' : self._prepare_final_url(path),
                'cookies': response.cookies
            }
    
    def _handle_superset_access_token_expire_time(self) -> dict:
        if not self._access_token_and_expire_time or self._access_token_and_expire_time["access_token_expire_time"] < datetime.now():
            self._access_token_and_expire_time = self._get_superset_access_token_using_login()
            access_token = self._access_token_and_expire_time['access_token']
            headers = {'Authorization': 'Bearer ' + access_token}
            csrf_token = self._get_csrf_token(headers)
            self._access_token_and_expire_time.update(**csrf_token)
        return self._access_token_and_expire_time
    
    def _prepare_authorization_header(self):
        token = self._handle_superset_access_token_expire_time()
        access_token = token['access_token']
        csrf_token = token['csrf_token']
        csrf_url = token['csrf_url']
        cookies = token['cookies']

        headers = {'Authorization': 'Bearer ' + access_token,
                   'X-CSRFToken': csrf_token,
                   'Referer': csrf_url
        }

        return headers, cookies
    
    def _handle_response_status_code(self, response) -> bool:
        ''' check if response status code is 200 or 201, otherwise raise exception'''
        if response.status_code in (200,201):
            return True
        else:
            raise Exception(response.status_code, response.text)

    def _fetch_superset_api_one_page(self, request_method, path, page_params) -> Any:
        query_params = f'?q={page_params}'
        headers, cookies = self._prepare_authorization_header()
        response = requests.request(
            method=request_method,
            url=self._prepare_final_url(path, query_params),
            headers=headers,
            cookies=cookies
        )
        if self._handle_response_status_code(response = response):
            return response
    
    def _fetch_superset_api_all_page(self, table) -> Generator:
        pagination_complete = False
        next_page = {'page': 0, 'page_size': self.page_size}
        while not pagination_complete:
            params_as_rison = f"(page:{next_page['page']},page_size:{next_page['page_size']})"
            response = self._fetch_superset_api_one_page(
                request_method = table['request_method'],
                path = table['path'],
                page_params = params_as_rison
            )
            if response.json()['count'] <= next_page['page'] * next_page['page_size']:
                pagination_complete = True
            else:
                next_page = {
                    'page': next_page['page'] + 1,
                    'page_size': self.page_size
                }
            yield response
    
    def _get_record(self, table , parse_function) -> Generator:
        for response in self._fetch_superset_api_all_page(table=table):
            yield from parse_function(table = table, response = response)

    def _to_csv(self, response, table_name) -> None:
        df = pd.DataFrame(response)
        df.to_csv(table_name, index=False)
    
    def _to_postgres(self, response, table_name) -> Table:
        query = f"TRUNCATE TABLE {table_name};"
        k = self._query_postgres(query, return_type=None)
        df = pd.DataFrame(response)
        df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
        print('Clone Complete \\n')
        
    def _parse_response(self, table, response) -> Generator:
        for row in response.json()['result']:
                result = {}
                for column_name, column_type in table['response_schema'].items():
                    if column_type['type'] == object:
                        result.update(**{(column_name + '_' + k): row[column_name][k] for k,v in column_type['properties'].items()})
                        
                    elif column_type['type'] == 'Array_object':
                        for k,v in column_type['properties'].items():
                            result.update(**{column_name + '_' + k: ','.join(str(j[k]) for j in row[column_name])})
                    else:
                        result.update({
                            column_name: row[column_name],
                        })
                yield result

    def _check_data_between_superset_and_postgres(self):
        '''
        if number of row of permission_view between superset and postgres is not equal, 
        we clone data from superset to postgres
        else we skip
        '''
        self._setup_postgres_table()
        for table in self.list_table:
            response = self._fetch_superset_api_one_page(
                request_method = table['request_method'],
                path = table['path'],
                page_params = '(page:0,page_size:100)'
            )

            
            query = f"select count(*) from {table['table_name']}"
            row_count_postgres = self._query_postgres(query, return_type='scalar')
            
            print(f'Running check data between superset and postgres for table {table["table_name"]} \\n')
            if table['id'] == 2 or table['id'] == 5:
                print(f'Cloning data from superset to postgres {table["table_name"]} \\n')
                records = self._get_record(
                    parse_function = self._parse_response,
                    table = table
                )
                self._to_postgres(response=records, table_name=table['table_name'])
            elif response.json()['count'] != row_count_postgres:
                print(f'Cloning data from superset to postgres {table["table_name"]} \\n')
                records = self._get_record(
                    parse_function = self._parse_response,
                    table = table
                )
                self._to_postgres(response=records, table_name=table['table_name'])
    
    def _query_postgres(self, query, return_type):
        ''' three type of return: 
        + scalar: get the first row and first column
        + dict: get column name and data in dict format
        + None: execute query and return nothing
        '''
        with self.engine.connect() as connection:
            if return_type == 'scalar':
                value = connection.execute(text(query)).scalar()
            elif return_type == 'dict':
                value = connection.execute(text(query)).mappings().all()
            elif not return_type: 
                connection.execute(text(query))
                connection.commit()
                value = None 
            return value

    def _prepare_dataset_information(self, list_dataset, table_id_query_condition):
        '''
        getting dataset_name, its permission_view_id and table_id in superset
        dataset_name and permission_view_id is for creating role
        table_id is for creating row_level_security
        return a list of dataset
        dataset = { 'name': dataset_name, 'permission_view_id': permission_view_id, 'table_id': table_id}
        '''
        list_dataset_with_information=[]
        for dataset in list_dataset:
            # update permission_view_id
            query = f"select id from {self.list_table[0]['table_name']} where permission_name = 'schema_access' and view_menu_name like '%{dataset}%'"
            permission_id = self._query_postgres(query, return_type='scalar')
            dataset_info = {'permission_view_id': permission_id}

            # update table_id
            if not table_id_query_condition:
                # for app_related_dataset
                query = f"SELECT string_agg(cast(id as TEXT), ',') FROM {self.list_table[2]['table_name']} where schema = '{dataset}'"
            else:
                # for mkt_related_dataset, excluding some table_id
                query = f"SELECT string_agg(cast(id as TEXT), ',') FROM {self.list_table[2]['table_name']} where schema = '{dataset}' and {table_id_query_condition}"
            table_id = self._query_postgres(query, return_type='scalar')
            dataset_info.update({'table_id': table_id})
            if not dataset_info['permission_view_id'] or not dataset_info['table_id']:
                raise Exception(f"There is no {dataset} dataset in superset")  
            list_dataset_with_information.append({'dataset_name': dataset, **dataset_info})
        return list_dataset_with_information
 
    def create_role(self, list_object, list_dataset, table_id_query_condition):
        self._check_data_between_superset_and_postgres()
        list_dataset_with_information = self._prepare_dataset_information(list_dataset=list_dataset, table_id_query_condition=table_id_query_condition)
        for object in list_object:
            object.update({'role_ids': [], 'table_ids': []})
            for dataset in list_dataset_with_information:
                role_name = f"auto_{dataset['dataset_name'].upper()}_{object['object_name']}"

                # check role exist
                query = f"select count(*) from {self.list_table[1]['table_name']}  where name = '{role_name}'"
                has_role = self._query_postgres(query, return_type='scalar')

                # create role
                if not has_role:
                    path = 'security/roles/'
                    request_method = 'POST'
                    headers, cookies = self._prepare_authorization_header()
                    response = requests.request(
                            method=request_method,
                            url=self._prepare_final_url(path),
                            headers=headers,
                            cookies=cookies,
                            json={"name":  role_name}
                        )
                    if self._handle_response_status_code(response = response):
                        print(f'Create role {role_name} successfully \\n')
                        # add permission_view_id to role
                        role_id = response.json()['id']                        
                        permission_view_menu_ids = dataset['permission_view_id']
                        self._update_role_permission(role_id, role_name, permission_view_menu_ids)

                        # add role id and role name to object to create row_level_security
                        object['role_ids'].append((role_id))
                        object['table_ids'].extend((int(i) for i in dataset['table_id'].split(',')))
                        
                        time.sleep(1.5)

            # create row_level_security
            self._create_row_level_security(object)

    def _update_role_permission(self, role_id, role_name, permission_view_menu_ids):
        path = f'security/roles/{role_id}/permissions'
        request_method = 'POST'
        headers, cookies = self._prepare_authorization_header()
        response = requests.request(
                            method=request_method,
                            url=self._prepare_final_url(path),
                            headers=headers,
                            cookies=cookies,
                            json={'permission_view_menu_ids': [permission_view_menu_ids]}
                        )
        if self._handle_response_status_code(response = response):
            print(f'Update role permission {role_name} successfully \\n')
    
    def _create_row_level_security(self, object):
        # check role exist
        query = f"select count(*) from {self.list_table[3]['table_name']}  where name = '{object['role_level_security_name']}'"
        has_role = self._query_postgres(query, return_type='scalar')
        if not has_role:
            path = 'rowlevelsecurity'
            request_method = 'POST'
            headers, cookies = self._prepare_authorization_header()
            response = requests.request(
                                method=request_method,
                                url=self._prepare_final_url(path),
                                headers=headers,
                                cookies=cookies,
                                json={
                                    "clause": object['clause'],
                                    "filter_type": "Regular",
                                    "group_key": object['group_key'],
                                    "name": object['role_level_security_name'],
                                    "roles": object['role_ids'],
                                    "tables": object['table_ids'],
                                },
                            )
            if self._handle_response_status_code(response = response):
                print(f'Create role level security {object["role_level_security_name"]} successfully \\n')
                time.sleep(1.5)
    
    def update_table_all_row_level_security(self, tables_schema, list_dataset, table_id_query_condition):
        self._check_data_between_superset_and_postgres()

        # putting all table id of all dataset into a list 
        list_dataset_with_information = self._prepare_dataset_information(list_dataset=list_dataset, table_id_query_condition=table_id_query_condition)
        tables_id_all_dataset: str =  ','.join(i['table_id'] for i in list_dataset_with_information)
        tables_id_as_list = [int(i) for i in tables_id_all_dataset.split(',')]
        
        # get row_level_security_id that need update
        query = f"select id, roles_id, name from {self.list_table[3]['table_name']} where tables_schema like '%{tables_schema}%' and lower(name) similar to '%(ios|android|mkt|auto)%' "
        list_row_level_security_as_dict = self._query_postgres(query, return_type='dict')
        for object in list_row_level_security_as_dict:
            self._update_table_row_level_security(object = object, tables_id=tables_id_as_list)
            time.sleep(1.5)
        
    def _update_table_row_level_security(self, object, tables_id):
        # adjust role_level_security_name
        if 'auto' not in object['name']:
             old_name = object['name'].split('_')
             if 'performance' in old_name:
                 new_name = f'auto_VOLIO_MKT_{old_name[2]}'
             else:
                 new_name = f'auto_{old_name[0].upper()}_{old_name[2]}_{old_name[1].lower()}'
        else:
            new_name = object['name']
        path = f"rowlevelsecurity/{object['id']}"
        request_method = 'PUT'
        headers, cookies = self._prepare_authorization_header()
        response = requests.request(
                            method=request_method,
                            url=self._prepare_final_url(path),
                            headers=headers,
                            cookies=cookies,
                            json={
                                "roles": [int(i) for i in object['roles_id'].split(',')],
                                "tables": tables_id,
                                "name": new_name
                            }
                        )
        if self._handle_response_status_code(response = response):
            print(f'Update role level security {new_name} successfully \\n')
    
    def _update_roles_name(self, company_name):
        self._check_data_between_superset_and_postgres()

        if company_name == 'volio':
            query = f"select id, name from {self.list_table[1]['table_name']} where lower(name) similar to '%(mkt_performance)%' and lower(name) not like '%auto%' "
        else:
            query = f"select id, name from {self.list_table[1]['table_name']} where lower(name) similar to '%(mkt|inapp)%' and lower(name) not similar to '%(auto|allapps|allgames)%' and lower(name) like '%{company_name}%' "
            
        roles = self._query_postgres(query, return_type='dict')
        for role in roles:
            role_name = role['name'].split('_')
            role_id = role['id']
            if role_name[1].lower() == 'mkt':
                dataset_code = f'{company_name}_mkt'
            elif role_name[1].lower() == 'inapp': 
                dataset_code = f'{company_name}_all_users'
            elif role_name[1].lower() == 'performance':
                dataset_code = f'{company_name}_mkt_performance'

            # update role name
            if company_name in ('govo', 'pion'):
                app_name = role_name[2]
                platform = 'android'
                object = app_name + '_' + platform.lower()
            elif company_name == 'volio':
                object = role_name[2].lower()
            else:
                app_name = role_name[3]
                platform = role_name[2]
                object = app_name + '_' + platform.lower()
            
            new_role_name = f"auto_{dataset_code.upper()}_{object}"
            self._update_role_name(role_name = new_role_name, role_id = role_id)


    def _update_role_name(self, role_name, role_id):
        path = f"security/roles/{role_id}"
        request_method = 'PUT'
        headers, cookies = self._prepare_authorization_header()
        response = requests.request(
                            method=request_method,
                            url=self._prepare_final_url(path),
                            headers=headers,
                            cookies=cookies,
                            json={
                                "name": f"{role_name}"
                            }
                        )
        if self._handle_response_status_code(response = response):
            print(f'Update role name {role_name} successfully \\n')
    
    def _delete_object(self, object_id, object_name, path):

        final_path = path + str(object_id)
        request_method = 'DELETE'
        headers, cookies = self._prepare_authorization_header()
        response = requests.request(
                            method=request_method,
                            url=self._prepare_final_url(final_path),
                            headers=headers,
                            cookies=cookies
                        )
        if self._handle_response_status_code(response = response):
            print(f'Delete {object_name} successfully \\n')

    def delete_roles(self):
        path = 'security/roles/'
        query = f"select id, name from {self.list_table[1]['table_name']} where lower(name)like '%auto%'"
        object = self._query_postgres(query, return_type='dict')
        for role in object:
            self._delete_object(object_id= role['id'], object_name= role['name'], path=path)
            time.sleep(1.5)
            
    def delete_permission_views(self):
        path = 'security/permissions-resources/'
        query = f"select id, view_menu_name as name from {self.list_table[0]['table_name']} where view_menu_name like '% 2%'"
        objects = self._query_postgres(query, return_type='dict')
        for object in objects:
            self._delete_object(object_id= object['id'], object_name= object['name'], path=path)
            time.sleep(1.5)
    
    def delete_view_resources(self):
        path = 'security/resources/'
        query = f"select id, name from {self.list_table[4]['table_name']} where name like '% 2%'"
        objects = self._query_postgres(query, return_type='dict')
        for object in objects:
            self._delete_object(object_id= object['id'], object_name= object['name'], path=path)
            time.sleep(1.5)
    
    def _update_user_role(self, user_id, user_name ,role_id, user_password):
        path = f'security/users/{user_id}'
        request_method = 'PUT'
        headers, cookies = self._prepare_authorization_header()
        response = requests.request(
                            method=request_method,
                            url=self._prepare_final_url(path),
                            headers=headers,
                            cookies=cookies,
                            json={
                                "roles": role_id,
                                "password": user_password
                            },
                            
                        )
        if self._handle_response_status_code(response = response):
            print(f'Update {user_name} successfully \\n')
    def update_users_app_permission(self, objects, company_name, bigquery_connection):
        list_object = [object for object in objects]

        # getting new_role_id for each users
        list_mkt_and_new_role_id = []
        for object in list_object:
            query = f"select string_agg(cast(id as TEXT), ',') from {self.list_table[1]['table_name']} where lower(name) like '%{object['app_name'].lower() + '_' + object['platform'].lower()}%' and lower(name) like '%{'auto_' + object['company_name'].lower()}%' "
            new_role_ids = self._query_postgres(query, return_type='scalar')
            list_mkt_and_new_role_id.append({'marketer_name': object['marketer_name'], 'new_role_id': new_role_ids})
        
        # distinct mkt name and their new role id
        list_mkt_distict = {i['marketer_name'] for i in list_object}
        list_mkt_and_new_role_id_distinct = [] 
        for mkt_name in list_mkt_distict:
            result = {'marketer_name': mkt_name, 'new_role_id': []}
            for j in list_mkt_and_new_role_id:
                if mkt_name == j['marketer_name']:
                    result['new_role_id'].extend(int(id) for id in j['new_role_id'].split(','))
            list_mkt_and_new_role_id_distinct.append(result)

        # print(list_mkt_and_new_role_id_distinct)

        # getting active mkt and their old role id
        list_mkt_and_old_id_remain_distinct = []
        list_mkt_for_query = ','.join("'{}'".format(i) for i in list_mkt_distict)
        query = f"select username, id as user_id, roles_id as old_role_id, roles_name as old_role_name from {self.list_table[5]['table_name']} where username in ({list_mkt_for_query}) and active = true"
        list_active_mkt = self._query_postgres(query, return_type='dict')

        for mkt in list_active_mkt:
            mkt_and_old_role_remain = {}
            old_role_id_remain = []
            for i in zip(mkt['old_role_name'].split(','), mkt['old_role_id'].split(',')):
                if (company_name.lower() + '_all_users') not in i[0].lower() and (company_name.lower() + '_mkt') not in i[0].lower():
                    old_role_id_remain.append(int(i[1]))
            mkt_and_old_role_remain.update({'marketer_name': mkt['username'], 'old_role_id_remain': old_role_id_remain, 'user_id': mkt['user_id']})
            list_mkt_and_old_id_remain_distinct.append(mkt_and_old_role_remain)
        
        # print(list_mkt_and_old_id_remain_distinct)
        
        # mapping mkt_name with old_role_id_remain abnd new_role_id
        for i in list_mkt_and_old_id_remain_distinct:
            for j in list_mkt_and_new_role_id_distinct:
                if i['marketer_name'] == j['marketer_name']:
                    list_final_role_id = j['new_role_id'] + i['old_role_id_remain']
                    # get user password
                    user_password_table_id = os.getenv('VOLIO_MKT_PASSWORD_TABLE_ID')
                    user_password_query = (
                            f" SELECT " 
                            f" password, " 
                            f" FROM `{user_password_table_id}` "
                          f" WHERE marketer_name in (\'{i['marketer_name']}\')"
                    )
                    user_password  = next(i for i in bigquery_connection.get_object_from_bigquery(query=user_password_query, start_date=None)).get('password')
                    # update users role
                    self._update_user_role(user_id = i['user_id'], user_name = i['marketer_name'], role_id = list_final_role_id, user_password= user_password)

    def update_users_mkt_leader_permission(self, object):
        pass



class Bigquery():
    def __init__(self):
        self.bqclient = self._client()

    @lru_cache()
    def _client(self):
        service_account_info = json.loads(os.getenv('SERVICE_ACCOUNT'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        bqclient = bigquery.Client(credentials=credentials)
        return bqclient

    def get_object_from_bigquery(self, query ,start_date=None) -> Generator:
        '''
        return a list of object which will be used to create role in superset
        object = {'object_name': 'app_name_platform'}
        or
        object = {'object_name': 'marketer_name'}
        '''
        
        if start_date: where_condition = f' and cast(start_date as DATE) >= cast("{start_date}" as DATE)'
        else: where_condition = ''

        if query: 
            final_query = query + f" {where_condition} "
            table = self.bqclient.query_and_wait(query=final_query).to_arrow()
            list_object = table.to_pylist()
            yield from list_object
        else:
            return None

if __name__ == '__main__':
    # com = 'govo'
    # com = 'jacat'
    # com = 'fidra'
    # com = 'pion'

    # fuction_type, start_date = 'app_related_role', '2024-06-01'
    # fuction_type, start_date = 'mkt_performance_related_role' , None
    fuction_type, start_date = 'update_permission_mkt_app' , '2024-06-01'

    for com in ['govo', 'jacat', 'fidra']:
        bigquery_object_query, list_dataset, row_level_security_id_query_condition, table_id_query_condition, company_name = handle_user_input(fuction_type=fuction_type, company_name=com)

        bg = Bigquery()
        list_object = bg.get_object_from_bigquery(query=bigquery_object_query, start_date=start_date)

    session = Superset(
        base_url= os.getenv('SUPERSET_BASE_URL'),
        username = os.getenv('SUPERSET_USERNAME'),
        password = os.getenv('SUPERSET_PASSWORD') 
    )
    # session.create_role(list_object = list_object, list_dataset=list_dataset, table_id_query_condition=table_id_query_condition)
    # session.update_table_all_row_level_security(tables_schema=row_level_security_id_query_condition, list_dataset=list_dataset, table_id_query_condition=table_id_query_condition)
    # session.delete_roles()
    # session.update_users_app_permission(objects = list_object, company_name = company_name, bigquery_connection= bg)
        