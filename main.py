import logging
import boto3
import subprocess
import json
import os
from typing import Dict, List, Any, Optional, Union
from mcp.server import FastMCP
import asyncio

# 初始化 FastMCP 服务器
mcp = FastMCP(
    name='quicksight-lineage-mcp'
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DEFAULT_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID', '764946308314')
DEFAULT_REGION = os.environ.get('AWS_REGION', 'us-east-1')


class QuickSightLineage:
    """QuickSight 元数据血缘分析工具"""
    
    def __init__(self, aws_account_id: str, region: str = 'us-east-1'):
        """
        初始化 QuickSight 血缘分析工具
        
        参数:
            aws_account_id (str): AWS 账户 ID
            region (str): AWS 区域，默认为 us-east-1
        """
        self.aws_account_id = aws_account_id
        self.region = region
        self.quicksight = self._get_quicksight_client()
        
        
    def _get_quicksight_client(self):
        """获取 QuickSight 客户端，通过 ada + role assumption"""
        logger.info(f"创建新的 QuickSight 客户端，区域: {self.region}")
        try:
            profile = os.environ.get('AWS_PROFILE', 'ixtde-core-etl-alpha-datagrip-role')
            role_arn = os.environ.get('QS_ROLE_ARN', 'arn:aws:iam::764946308314:role/P210516748-IXT-Discovery')
            result = subprocess.run(
                ['/Users/dubbat/.toolbox/bin/ada', 'credentials', 'print', f'--profile={profile}'],
                capture_output=True, text=True, timeout=30
            )
            base_creds = json.loads(result.stdout)
            sts = boto3.client(
                'sts', region_name=self.region,
                aws_access_key_id=base_creds['AccessKeyId'],
                aws_secret_access_key=base_creds['SecretAccessKey'],
                aws_session_token=base_creds['SessionToken'],
            )
            assumed = sts.assume_role(RoleArn=role_arn, RoleSessionName='quicksight-lineage-mcp')['Credentials']
            session = boto3.Session(
                aws_access_key_id=assumed['AccessKeyId'],
                aws_secret_access_key=assumed['SecretAccessKey'],
                aws_session_token=assumed['SessionToken'],
                region_name=self.region,
            )
            return session.client('quicksight')
        except Exception as e:
            logger.warning(f"Role assumption failed, falling back to ambient creds: {e}")
            return boto3.client('quicksight', region_name=self.region)


    def list_all_analyses(self) -> List[Dict[str, Any]]:
        """
        获取所有分析列表
        
        返回:
            List[Dict[str, Any]]: 分析列表
        """
        try:
            analyses = []
            next_token = None
            
            while True:
                if next_token:
                    response = self.quicksight.list_analyses(
                        AwsAccountId=self.aws_account_id,
                        NextToken=next_token
                    )
                else:
                    response = self.quicksight.list_analyses(
                        AwsAccountId=self.aws_account_id
                    )
                    
                analyses.extend(response.get('AnalysisSummaryList', []))
                
                next_token = response.get('NextToken')
                if not next_token:
                    break
                    
            return analyses
            
        except Exception as e:
            logger.error(f"获取分析列表时出错: {str(e)}")
            return []
    
    def list_all_dashboards(self) -> List[Dict[str, Any]]:
        """
        获取所有仪表板列表
        
        返回:
            List[Dict[str, Any]]: 仪表板列表
        """
        try:
            dashboards = []
            next_token = None
            
            while True:
                if next_token:
                    response = self.quicksight.list_dashboards(
                        AwsAccountId=self.aws_account_id,
                        NextToken=next_token
                    )
                else:
                    response = self.quicksight.list_dashboards(
                        AwsAccountId=self.aws_account_id
                    )
                    
                dashboards.extend(response.get('DashboardSummaryList', []))
                
                next_token = response.get('NextToken')
                if not next_token:
                    break
                    
            return dashboards
            
        except Exception as e:
            logger.error(f"获取仪表板列表时出错: {str(e)}")
            return []
    
    def list_all_datasets(self) -> List[Dict[str, Any]]:
        """
        获取所有数据集列表
        
        返回:
            List[Dict[str, Any]]: 数据集列表
        """
        try:
            datasets = []
            next_token = None
            
            while True:
                if next_token:
                    response = self.quicksight.list_data_sets(
                        AwsAccountId=self.aws_account_id,
                        NextToken=next_token
                    )
                else:
                    response = self.quicksight.list_data_sets(
                        AwsAccountId=self.aws_account_id
                    )
                    
                datasets.extend(response.get('DataSetSummaries', []))
                
                next_token = response.get('NextToken')
                if not next_token:
                    break
                    
            return datasets
            
        except Exception as e:
            logger.error(f"获取数据集列表时出错: {str(e)}")
            return []

    def list_all_datasources(self) -> List[Dict[str, Any]]:
        """
        获取所有数据源列表
        
        返回:
            List[Dict[str, Any]]: 数据集列表
        """
        try:
            datasources = []
            next_token = None
            
            while True:
                if next_token:
                    response = self.quicksight.list_data_sources(
                        AwsAccountId=self.aws_account_id,
                        NextToken=next_token
                    )
                else:
                    response = self.quicksight.list_data_sources(
                        AwsAccountId=self.aws_account_id
                    )
                    
                datasources.extend(response.get('DataSources', []))
                
                next_token = response.get('NextToken')
                if not next_token:
                    break
                    
            return datasources
            
        except Exception as e:
            logger.error(f"获取数据源时出错: {str(e)}")
            return []


    def get_dataset_details(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """
        获取数据集的详细信息，包括SQL查询和字段列表
        
        参数:
            dataset_id (str): 数据集ID
            
        返回:
            Optional[Dict[str, Any]]: 包含数据集详细信息的字典，如果出错则返回None
        """
        try:
            logger.info(f"正在获取数据集 {dataset_id} 的详细信息...")
            
            # 获取数据集详细信息
            dataset = self.quicksight.describe_data_set(
                AwsAccountId=self.aws_account_id,
                DataSetId=dataset_id
            )
            
            dataset_details = dataset['DataSet']
            
            result = {
                'DataSetId': dataset_id,
                'PhysicalTableMap': dataset_details["PhysicalTableMap"],
                'LogicalTableMap': dataset_details["LogicalTableMap"],
                'OutputColumns': dataset_details["OutputColumns"],
                'ImportMode': dataset_details["ImportMode"]
            }
            
        except Exception as e:
            logger.error(f"获取数据集 {dataset_id} 详细信息时出错: {str(e)}")
            result = {
                'DataSetId': dataset_id,
                'Error': str(e)
            }
        
        return result
    
    
    def get_datasource_details(self, datasource_id: str) -> Optional[Dict[str, Any]]:
        """
        获取数据源的详细信息
        
        参数:
            datasource_id (str): 数据源ID
            
        返回:
            Optional[Dict[str, Any]]: 包含数据源详细信息的字典，如果出错则返回None
        """
        try:
            # 获取数据源详细信息
            datasource = self.quicksight.describe_data_source(
                AwsAccountId=self.aws_account_id,
                DataSourceId=datasource_id
            )
            
            datasource_details = datasource['DataSource']
            result = {
                'DataSourceId': datasource_id,
                'DataSourceParameters': datasource_details["DataSourceParameters"]
            }
            
        except Exception as e:
            logger.error(f"获取数据源 {datasource_id} 详细信息时出错: {str(e)}")
            result = {
                'DataSourceId': datasource_id,
                'Error': str(e)
            }
        
        return result
    

    def get_analysis_details(self, analysis_id: str, section: str = None) -> Dict[str, Any]:
        """
        获取分析的sheet和visual信息
        
        参数:
            analysis_id (str): 分析ID
            section (str): Optional section filter. Valid values:
                           CalculatedFields, Sheets, FilterGroups,
                           ParameterDeclarations, DataSetIdentifier.
                           If None, returns all sections.
            
        返回:
            Dict[str, Any]: 包含sheet和visual信息的字典，如果出错则返回空结构
        """
        try:
            # 获取分析详细信息
            analysis_details = self.quicksight.describe_analysis_definition(
                AwsAccountId=self.aws_account_id,
                AnalysisId=analysis_id
            )
            
            analysis_definition = analysis_details.get('Definition', {})

            section_map = {
                'DataSetIdentifier': 'DataSetIdentifierDeclarations',
                'Sheets': 'Sheets',
                'CalculatedFields': 'CalculatedFields',
                'ParameterDeclarations': 'ParameterDeclarations',
                'FilterGroups': 'FilterGroups',
            }

            if section and section in section_map:
                result = {
                    'AnalysisId': analysis_id,
                    'Section': section,
                    section: analysis_definition.get(section_map[section], [])
                }
            else:
                result = {
                    'AnalysisId': analysis_id,
                    'DataSetIdentifier': analysis_definition.get('DataSetIdentifierDeclarations', []),
                    'Sheets': analysis_definition.get('Sheets', []),
                    'CalculatedFields': analysis_definition.get('CalculatedFields', []),
                    'ParameterDeclarations': analysis_definition.get('ParameterDeclarations', []),
                    'FilterGroups': analysis_definition.get('FilterGroups', [])
                }
            
        except Exception as e:
            logger.error(f"获取分析 {analysis_id} 详细信息时出错: {str(e)}")
            return {
                'AnalysisId': analysis_id,
                'Error': str(e)
            }
        
        return result
    
    
    def get_dashboard_details(self, dashboard_id: str) -> Dict[str, Any]:
        try:
            dashboard_details = self.quicksight.describe_dashboard(
                AwsAccountId=self.aws_account_id,
                DashboardId=dashboard_id
            )
            dashboard_definition = dashboard_details.get('Dashboard', {}).get('Version', {})
            result = {
                'DashboardId': dashboard_id,
                'AnalysisArn': dashboard_definition.get('SourceEntityArn', ''),
                'DataSetArns': dashboard_definition.get('DataSetArns', []),
                'Sheets': dashboard_definition.get('Sheets', [])
            }
        except Exception as e:
            logger.error(f"Error getting dashboard {dashboard_id}: {str(e)}")
            return {'DashboardId': dashboard_id, 'Error': str(e)}
        return result

    def get_dashboard_definition(self, dashboard_id: str, section: str = None) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_dashboard_definition(
                AwsAccountId=self.aws_account_id,
                DashboardId=dashboard_id
            )
            defn = resp.get('Definition', {})

            section_map = {
                'DataSetIdentifier': 'DataSetIdentifierDeclarations',
                'Sheets': 'Sheets',
                'CalculatedFields': 'CalculatedFields',
                'ParameterDeclarations': 'ParameterDeclarations',
                'FilterGroups': 'FilterGroups',
            }

            if section and section in section_map:
                return {
                    'DashboardId': dashboard_id,
                    'Section': section,
                    section: defn.get(section_map[section], [])
                }
            return {
                'DashboardId': dashboard_id,
                'DataSetIdentifier': defn.get('DataSetIdentifierDeclarations', []),
                'Sheets': defn.get('Sheets', []),
                'CalculatedFields': defn.get('CalculatedFields', []),
                'ParameterDeclarations': defn.get('ParameterDeclarations', []),
                'FilterGroups': defn.get('FilterGroups', [])
            }
        except Exception as e:
            return {'DashboardId': dashboard_id, 'Error': str(e)}

    def get_dataset_permissions(self, dataset_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_data_set_permissions(
                AwsAccountId=self.aws_account_id,
                DataSetId=dataset_id
            )
            return {'DataSetId': dataset_id, 'Permissions': resp.get('Permissions', [])}
        except Exception as e:
            return {'DataSetId': dataset_id, 'Error': str(e)}

    def get_dashboard_permissions(self, dashboard_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_dashboard_permissions(
                AwsAccountId=self.aws_account_id,
                DashboardId=dashboard_id
            )
            return {'DashboardId': dashboard_id, 'Permissions': resp.get('Permissions', [])}
        except Exception as e:
            return {'DashboardId': dashboard_id, 'Error': str(e)}

    def get_analysis_permissions(self, analysis_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_analysis_permissions(
                AwsAccountId=self.aws_account_id,
                AnalysisId=analysis_id
            )
            return {'AnalysisId': analysis_id, 'Permissions': resp.get('Permissions', [])}
        except Exception as e:
            return {'AnalysisId': analysis_id, 'Error': str(e)}

    def get_datasource_permissions(self, datasource_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_data_source_permissions(
                AwsAccountId=self.aws_account_id,
                DataSourceId=datasource_id
            )
            return {'DataSourceId': datasource_id, 'Permissions': resp.get('Permissions', [])}
        except Exception as e:
            return {'DataSourceId': datasource_id, 'Error': str(e)}

    def list_ingestions(self, dataset_id: str) -> List[Dict[str, Any]]:
        try:
            ingestions = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id, 'DataSetId': dataset_id}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_ingestions(**kwargs)
                ingestions.extend(resp.get('Ingestions', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return ingestions
        except Exception as e:
            return [{'Error': str(e)}]

    def describe_ingestion(self, dataset_id: str, ingestion_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_ingestion(
                AwsAccountId=self.aws_account_id,
                DataSetId=dataset_id,
                IngestionId=ingestion_id
            )
            return resp.get('Ingestion', {})
        except Exception as e:
            return {'Error': str(e)}

    def get_dataset_refresh_properties(self, dataset_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_data_set_refresh_properties(
                AwsAccountId=self.aws_account_id,
                DataSetId=dataset_id
            )
            return {'DataSetId': dataset_id, 'RefreshProperties': resp.get('DataSetRefreshProperties', {})}
        except Exception as e:
            return {'DataSetId': dataset_id, 'Error': str(e)}

    def list_refresh_schedules(self, dataset_id: str) -> List[Dict[str, Any]]:
        try:
            resp = self.quicksight.list_refresh_schedules(
                AwsAccountId=self.aws_account_id,
                DataSetId=dataset_id
            )
            return resp.get('RefreshSchedules', [])
        except Exception as e:
            return [{'Error': str(e)}]

    def list_folders(self) -> List[Dict[str, Any]]:
        try:
            folders = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_folders(**kwargs)
                folders.extend(resp.get('FolderSummaryList', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return folders
        except Exception as e:
            return [{'Error': str(e)}]

    def describe_folder(self, folder_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_folder(
                AwsAccountId=self.aws_account_id,
                FolderId=folder_id
            )
            return resp.get('Folder', {})
        except Exception as e:
            return {'FolderId': folder_id, 'Error': str(e)}

    def list_folder_members(self, folder_id: str) -> List[Dict[str, Any]]:
        try:
            members = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id, 'FolderId': folder_id}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_folder_members(**kwargs)
                members.extend(resp.get('FolderMemberList', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return members
        except Exception as e:
            return [{'Error': str(e)}]

    def describe_theme(self, theme_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_theme(
                AwsAccountId=self.aws_account_id,
                ThemeId=theme_id
            )
            return resp.get('Theme', {})
        except Exception as e:
            return {'ThemeId': theme_id, 'Error': str(e)}

    def list_themes(self) -> List[Dict[str, Any]]:
        try:
            themes = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_themes(**kwargs)
                themes.extend(resp.get('ThemeSummaryList', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return themes
        except Exception as e:
            return [{'Error': str(e)}]

    def list_namespaces(self) -> List[Dict[str, Any]]:
        try:
            namespaces = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_namespaces(**kwargs)
                namespaces.extend(resp.get('Namespaces', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return namespaces
        except Exception as e:
            return [{'Error': str(e)}]

    def list_users(self, namespace: str = 'default') -> List[Dict[str, Any]]:
        try:
            users = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id, 'Namespace': namespace}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_users(**kwargs)
                users.extend(resp.get('UserList', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return users
        except Exception as e:
            return [{'Error': str(e)}]

    def list_groups(self, namespace: str = 'default') -> List[Dict[str, Any]]:
        try:
            groups = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id, 'Namespace': namespace}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_groups(**kwargs)
                groups.extend(resp.get('GroupList', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return groups
        except Exception as e:
            return [{'Error': str(e)}]

    def list_group_memberships(self, group_name: str, namespace: str = 'default') -> List[Dict[str, Any]]:
        try:
            members = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id, 'GroupName': group_name, 'Namespace': namespace}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_group_memberships(**kwargs)
                members.extend(resp.get('GroupMemberList', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return members
        except Exception as e:
            return [{'Error': str(e)}]

    def describe_account_settings(self) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_account_settings(
                AwsAccountId=self.aws_account_id
            )
            return resp.get('AccountSettings', {})
        except Exception as e:
            return {'Error': str(e)}

    def list_tags_for_resource(self, resource_arn: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.list_tags_for_resource(ResourceArn=resource_arn)
            return {'ResourceArn': resource_arn, 'Tags': resp.get('Tags', [])}
        except Exception as e:
            return {'ResourceArn': resource_arn, 'Error': str(e)}

    def search_datasets(self, name_filter: str) -> List[Dict[str, Any]]:
        try:
            resp = self.quicksight.search_data_sets(
                AwsAccountId=self.aws_account_id,
                Filters=[{'Operator': 'StringLike', 'Name': 'DATASET_NAME', 'Value': name_filter}]
            )
            return resp.get('DataSetSummaries', [])
        except Exception as e:
            return [{'Error': str(e)}]

    def search_dashboards(self, name_filter: str) -> List[Dict[str, Any]]:
        try:
            resp = self.quicksight.search_dashboards(
                AwsAccountId=self.aws_account_id,
                Filters=[{'Operator': 'StringLike', 'Name': 'DASHBOARD_NAME', 'Value': name_filter}]
            )
            return resp.get('DashboardSummaryList', [])
        except Exception as e:
            return [{'Error': str(e)}]

    def search_analyses(self, name_filter: str) -> List[Dict[str, Any]]:
        try:
            resp = self.quicksight.search_analyses(
                AwsAccountId=self.aws_account_id,
                Filters=[{'Operator': 'StringLike', 'Name': 'ANALYSIS_NAME', 'Value': name_filter}]
            )
            return resp.get('AnalysisSummaryList', [])
        except Exception as e:
            return [{'Error': str(e)}]

    def describe_analysis(self, analysis_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_analysis(AwsAccountId=self.aws_account_id, AnalysisId=analysis_id)
            return resp.get('Analysis', {})
        except Exception as e:
            return {'AnalysisId': analysis_id, 'Error': str(e)}

    def describe_folder_permissions(self, folder_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_folder_permissions(AwsAccountId=self.aws_account_id, FolderId=folder_id)
            return {'FolderId': folder_id, 'Permissions': resp.get('Permissions', [])}
        except Exception as e:
            return {'FolderId': folder_id, 'Error': str(e)}

    def describe_folder_resolved_permissions(self, folder_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_folder_resolved_permissions(AwsAccountId=self.aws_account_id, FolderId=folder_id)
            return {'FolderId': folder_id, 'Permissions': resp.get('Permissions', [])}
        except Exception as e:
            return {'FolderId': folder_id, 'Error': str(e)}

    def describe_refresh_schedule(self, dataset_id: str, schedule_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_refresh_schedule(AwsAccountId=self.aws_account_id, DataSetId=dataset_id, ScheduleId=schedule_id)
            return resp.get('RefreshSchedule', {})
        except Exception as e:
            return {'Error': str(e)}

    def describe_user(self, username: str, namespace: str = 'default') -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_user(AwsAccountId=self.aws_account_id, UserName=username, Namespace=namespace)
            return resp.get('User', {})
        except Exception as e:
            return {'UserName': username, 'Error': str(e)}

    def describe_group(self, group_name: str, namespace: str = 'default') -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_group(AwsAccountId=self.aws_account_id, GroupName=group_name, Namespace=namespace)
            return resp.get('Group', {})
        except Exception as e:
            return {'GroupName': group_name, 'Error': str(e)}

    def describe_group_membership(self, group_name: str, member_name: str, namespace: str = 'default') -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_group_membership(AwsAccountId=self.aws_account_id, GroupName=group_name, MemberName=member_name, Namespace=namespace)
            return resp.get('GroupMember', {})
        except Exception as e:
            return {'Error': str(e)}

    def describe_namespace(self, namespace: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_namespace(AwsAccountId=self.aws_account_id, Namespace=namespace)
            return resp.get('Namespace', {})
        except Exception as e:
            return {'Namespace': namespace, 'Error': str(e)}

    def list_dashboard_versions(self, dashboard_id: str) -> List[Dict[str, Any]]:
        try:
            versions = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id, 'DashboardId': dashboard_id}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_dashboard_versions(**kwargs)
                versions.extend(resp.get('DashboardVersionSummaryList', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return versions
        except Exception as e:
            return [{'Error': str(e)}]

    def list_user_groups(self, username: str, namespace: str = 'default') -> List[Dict[str, Any]]:
        try:
            groups = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id, 'UserName': username, 'Namespace': namespace}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_user_groups(**kwargs)
                groups.extend(resp.get('GroupList', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return groups
        except Exception as e:
            return [{'Error': str(e)}]

    def list_folders_for_resource(self, resource_arn: str) -> List[Dict[str, Any]]:
        try:
            resp = self.quicksight.list_folders_for_resource(AwsAccountId=self.aws_account_id, ResourceArn=resource_arn)
            return resp.get('Folders', [])
        except Exception as e:
            return [{'Error': str(e)}]

    def search_data_sources(self, name_filter: str) -> List[Dict[str, Any]]:
        try:
            resp = self.quicksight.search_data_sources(
                AwsAccountId=self.aws_account_id,
                Filters=[{'Operator': 'StringLike', 'Name': 'DATASOURCE_NAME', 'Value': name_filter}]
            )
            return resp.get('DataSourceSummaries', [])
        except Exception as e:
            return [{'Error': str(e)}]

    def search_folders(self, name_filter: str) -> List[Dict[str, Any]]:
        try:
            resp = self.quicksight.search_folders(
                AwsAccountId=self.aws_account_id,
                Filters=[{'Operator': 'StringLike', 'Name': 'FOLDER_NAME', 'Value': name_filter}]
            )
            return resp.get('FolderSummaryList', [])
        except Exception as e:
            return [{'Error': str(e)}]

    def search_groups(self, namespace: str, name_filter: str) -> List[Dict[str, Any]]:
        try:
            resp = self.quicksight.search_groups(
                AwsAccountId=self.aws_account_id, Namespace=namespace,
                Filters=[{'Operator': 'StartsWith', 'Name': 'GROUP_NAME', 'Value': name_filter}]
            )
            return resp.get('GroupList', [])
        except Exception as e:
            return [{'Error': str(e)}]

    def list_templates(self) -> List[Dict[str, Any]]:
        try:
            templates = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_templates(**kwargs)
                templates.extend(resp.get('TemplateSummaryList', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return templates
        except Exception as e:
            return [{'Error': str(e)}]

    def describe_template(self, template_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_template(AwsAccountId=self.aws_account_id, TemplateId=template_id)
            return resp.get('Template', {})
        except Exception as e:
            return {'TemplateId': template_id, 'Error': str(e)}

    def describe_template_definition(self, template_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_template_definition(AwsAccountId=self.aws_account_id, TemplateId=template_id)
            defn = resp.get('Definition', {})
            return {
                'TemplateId': template_id,
                'DataSetIdentifier': defn.get('DataSetIdentifierDeclarations', []),
                'Sheets': defn.get('Sheets', []),
                'CalculatedFields': defn.get('CalculatedFields', []),
                'ParameterDeclarations': defn.get('ParameterDeclarations', []),
                'FilterGroups': defn.get('FilterGroups', [])
            }
        except Exception as e:
            return {'TemplateId': template_id, 'Error': str(e)}

    def describe_template_permissions(self, template_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_template_permissions(AwsAccountId=self.aws_account_id, TemplateId=template_id)
            return {'TemplateId': template_id, 'Permissions': resp.get('Permissions', [])}
        except Exception as e:
            return {'TemplateId': template_id, 'Error': str(e)}

    def list_template_versions(self, template_id: str) -> List[Dict[str, Any]]:
        try:
            versions = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id, 'TemplateId': template_id}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_template_versions(**kwargs)
                versions.extend(resp.get('TemplateVersionSummaryList', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return versions
        except Exception as e:
            return [{'Error': str(e)}]

    def describe_account_subscription(self) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_account_subscription(AwsAccountId=self.aws_account_id)
            return resp.get('AccountInfo', {})
        except Exception as e:
            return {'Error': str(e)}

    def describe_ip_restriction(self) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_ip_restriction(AwsAccountId=self.aws_account_id)
            return {'IpRestrictionRuleMap': resp.get('IpRestrictionRuleMap', {}), 'Enabled': resp.get('Enabled')}
        except Exception as e:
            return {'Error': str(e)}

    def list_vpc_connections(self) -> List[Dict[str, Any]]:
        try:
            conns = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_vpc_connections(**kwargs)
                conns.extend(resp.get('VPCConnectionSummaries', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return conns
        except Exception as e:
            return [{'Error': str(e)}]

    def describe_vpc_connection(self, vpc_connection_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_vpc_connection(AwsAccountId=self.aws_account_id, VPCConnectionId=vpc_connection_id)
            return resp.get('VPCConnection', {})
        except Exception as e:
            return {'VPCConnectionId': vpc_connection_id, 'Error': str(e)}

    def list_iam_policy_assignments(self, namespace: str = 'default') -> List[Dict[str, Any]]:
        try:
            assignments = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id, 'Namespace': namespace}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_iam_policy_assignments(**kwargs)
                assignments.extend(resp.get('IAMPolicyAssignments', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return assignments
        except Exception as e:
            return [{'Error': str(e)}]

    def list_topics(self) -> List[Dict[str, Any]]:
        try:
            topics = []
            next_token = None
            while True:
                kwargs = {'AwsAccountId': self.aws_account_id}
                if next_token:
                    kwargs['NextToken'] = next_token
                resp = self.quicksight.list_topics(**kwargs)
                topics.extend(resp.get('TopicsSummaries', []))
                next_token = resp.get('NextToken')
                if not next_token:
                    break
            return topics
        except Exception as e:
            return [{'Error': str(e)}]

    def describe_topic(self, topic_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_topic(AwsAccountId=self.aws_account_id, TopicId=topic_id)
            return resp.get('Topic', {})
        except Exception as e:
            return {'TopicId': topic_id, 'Error': str(e)}

    def describe_topic_permissions(self, topic_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_topic_permissions(AwsAccountId=self.aws_account_id, TopicId=topic_id)
            return {'TopicId': topic_id, 'Permissions': resp.get('Permissions', [])}
        except Exception as e:
            return {'TopicId': topic_id, 'Error': str(e)}

    def describe_topic_refresh(self, topic_id: str, refresh_id: str) -> Dict[str, Any]:
        try:
            resp = self.quicksight.describe_topic_refresh(AwsAccountId=self.aws_account_id, TopicId=topic_id, RefreshId=refresh_id)
            return resp.get('RefreshDetails', {})
        except Exception as e:
            return {'Error': str(e)}

    def list_topic_refresh_schedules(self, topic_id: str) -> List[Dict[str, Any]]:
        try:
            resp = self.quicksight.list_topic_refresh_schedules(AwsAccountId=self.aws_account_id, TopicId=topic_id)
            return resp.get('RefreshSchedules', [])
        except Exception as e:
            return [{'Error': str(e)}]

    def search_topics(self, name_filter: str) -> List[Dict[str, Any]]:
        try:
            resp = self.quicksight.search_topics(AwsAccountId=self.aws_account_id, KeyWord=name_filter)
            return resp.get('TopicsSummaries', [])
        except Exception as e:
            return [{'Error': str(e)}]

    def get_dashboard_embed_url(self, dashboard_id: str, identity_type: str = 'QUICKSIGHT', session_lifetime: int = 600) -> Dict[str, Any]:
        try:
            resp = self.quicksight.get_dashboard_embed_url(
                AwsAccountId=self.aws_account_id, DashboardId=dashboard_id,
                IdentityType=identity_type, SessionLifetimeInMinutes=session_lifetime
            )
            return {'EmbedUrl': resp.get('EmbedUrl', '')}
        except Exception as e:
            return {'Error': str(e)}

    def get_session_embed_url(self, entry_point: str = None) -> Dict[str, Any]:
        try:
            kwargs = {'AwsAccountId': self.aws_account_id}
            if entry_point:
                kwargs['EntryPoint'] = entry_point
            resp = self.quicksight.get_session_embed_url(**kwargs)
            return {'EmbedUrl': resp.get('EmbedUrl', '')}
        except Exception as e:
            return {'Error': str(e)}



@mcp.tool(
    name="quicksight_overview",
    description="执行 QuickSight 资源统计"
)
async def quicksight_overview(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    """
    参数:
        account_id: AWS 账户 ID
        region: AWS 区域，默认为 us-east-1
        
    返回:
        结果的字典包含数据集、分析和仪表板的数量
    """
    # 创建 QuickSight 血缘分析工具实例
    lineage = QuickSightLineage(
        aws_account_id=account_id,
        region=region
    )
    
    logger.info("获取 QuickSight 概览...")
    # 获取所有数据集
    datasets = lineage.list_all_datasets()
    logger.info(f"找到 {len(datasets)} 个数据集")

    # 获取所有数据源
    datasources = lineage.list_all_datasources()
    logger.info(f"找到 {len(datasources)} 个数据集")
    
    # 获取所有分析
    analyses = lineage.list_all_analyses()
    logger.info(f"找到 {len(analyses)} 个分析")
            
    # 获取所有仪表板
    dashboards = lineage.list_all_dashboards()
    logger.info(f"找到 {len(dashboards)} 个仪表板")

    return {
            "datasets_count": len(datasets),
            "datasources_count": len(datasources),
            "analyses_count": len(analyses),
            "dashboards_count": len(dashboards)
    }


@mcp.tool(
    name="list_datasets",
    description="所有数据集列表"
)
async def list_datasets(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    """
    参数:
        account_id: AWS 账户 ID
        region: AWS 区域，默认为 us-east-1
        
    返回:
        所有数据集列表
    """
    # 创建 QuickSight 血缘分析工具实例
    lineage = QuickSightLineage(
        aws_account_id=account_id,
        region=region
    )
    
    datasets = lineage.list_all_datasets()
    
    all_datasets = {}
            
    for dataset in datasets:
        dataset_id = dataset['DataSetId']
        dataset_name = dataset['Name']
        all_datasets[dataset_id] = dataset_name

    return all_datasets


@mcp.tool(
    name="list_datasources",
    description="所有数据源"
)
async def list_datasources(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    """
    参数:
        account_id: AWS 账户 ID
        region: AWS 区域，默认为 us-east-1
        
    返回:
        所有数据源
    """
    # 创建 QuickSight 血缘分析工具实例
    lineage = QuickSightLineage(
        aws_account_id=account_id,
        region=region
    )
    
    datasources = lineage.list_all_datasources()
    
    all_datasources = {}
            
    for datasource in datasources:
        datasource_id = datasource['DataSourceId']
        datasource_name = datasource['Name']
        all_datasources[datasource_id] = datasource_name

    return all_datasources


@mcp.tool(
    name="list_analyses",
    description="所有分析列表"
)
async def list_analyses(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    """
    参数:
        account_id: AWS 账户 ID
        region: AWS 区域，默认为 us-east-1
        
    返回:
        所有分析列表
    """
    # 创建 QuickSight 血缘分析工具实例
    lineage = QuickSightLineage(
        aws_account_id=account_id,
        region=region
    )
    
    analyses = lineage.list_all_analyses()
    
    all_analyses = {}
            
    for analysis in analyses:
        analysis_id = analysis['AnalysisId']
        analysis_name = analysis['Name']
        all_analyses[analysis_id] = analysis_name

    return all_analyses

@mcp.tool(
    name="list_dashboards",
    description="所有仪表板列表"
)
async def list_dashboards(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    """
    参数:
        account_id: AWS 账户 ID
        region: AWS 区域，默认为 us-east-1
        
    返回:
        所有仪表板列表
    """
    # 创建 QuickSight 血缘分析工具实例
    lineage = QuickSightLineage(
        aws_account_id=account_id,
        region=region
    )
    
    dashboards = lineage.list_all_dashboards()
    
    all_dashboards = {}
            
    for dashboard in dashboards:
        dashboard_id = dashboard['DashboardId']
        dashboard_name = dashboard['Name']
        all_dashboards[dashboard_id] = dashboard_name

    return all_dashboards

@mcp.tool(
    name="analyze_dataset",
    description="分析指定数据集的血缘关系"
)
async def analyze_dataset_id(dataset_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    """
        参数:
            dataset_id (str): 数据集ID
            
        返回:
            Dict[str, Any]: 包含数据集血缘关系的字典
    """
    
    # 创建 QuickSight 血缘分析工具实例
    lineage = QuickSightLineage(
        aws_account_id=account_id,
        region=region
    )
    
    dataset_details = lineage.get_dataset_details(dataset_id)

    return dataset_details
            

@mcp.tool(
    name="analyze_datasource",
    description="分析指定数据源的血缘关系"
)
async def analyze_datasource_id(datasource_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    """
        参数:
            datasource_id (str): 数据源ID
            
        返回:
            Dict[str, Any]: 包含数据源血缘关系的字典
    """
    
    # 创建 QuickSight 血缘分析工具实例
    lineage = QuickSightLineage(
        aws_account_id=account_id,
        region=region
    )
    
    datasource_details = lineage.get_datasource_details(datasource_id)

    return datasource_details

@mcp.tool(
    name="analyze_analysis",
    description="分析指定分析的血缘关系. Use optional 'section' parameter to return only one section and avoid output size limits. Valid sections: CalculatedFields, Sheets, FilterGroups, ParameterDeclarations, DataSetIdentifier. Omit section to get all (may exceed output limits for large analyses)."
)
async def analyze_analysis_id(analysis_id: str, section: str = None, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    """ 
        参数:
            analysis_id (str): 分析ID
            section (str): Optional - return only this section (CalculatedFields, Sheets, FilterGroups, ParameterDeclarations, DataSetIdentifier)
            
        返回:
            Dict[str, Any]: 包含分析血缘关系的字典
    """
    
    # 创建 QuickSight 血缘分析工具实例
    lineage = QuickSightLineage(
        aws_account_id=account_id,
        region=region
    )
    
    analysis_details = lineage.get_analysis_details(analysis_id, section=section)

    return analysis_details


@mcp.tool(
    name="analyze_dashboard",
    description="Analyze dashboard lineage - sheets, datasets, source analysis"
)
async def analyze_dashboard_id(dashboard_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.get_dashboard_details(dashboard_id)


@mcp.tool(
    name="analyze_dashboard_definition",
    description="Get full dashboard definition - visuals, calculated fields, filters, parameters. Use optional 'section' parameter to return only one section: CalculatedFields, Sheets, FilterGroups, ParameterDeclarations, DataSetIdentifier."
)
async def analyze_dashboard_definition(dashboard_id: str, section: str = None, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.get_dashboard_definition(dashboard_id, section=section)


@mcp.tool(
    name="get_dataset_permissions",
    description="Get permissions/access control for a dataset"
)
async def get_dataset_permissions(dataset_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.get_dataset_permissions(dataset_id)


@mcp.tool(
    name="get_dashboard_permissions",
    description="Get permissions/access control for a dashboard"
)
async def get_dashboard_permissions(dashboard_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.get_dashboard_permissions(dashboard_id)


@mcp.tool(
    name="get_analysis_permissions",
    description="Get permissions/access control for an analysis"
)
async def get_analysis_permissions(analysis_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.get_analysis_permissions(analysis_id)


@mcp.tool(
    name="get_datasource_permissions",
    description="Get permissions/access control for a data source"
)
async def get_datasource_permissions(datasource_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.get_datasource_permissions(datasource_id)


@mcp.tool(
    name="list_ingestions",
    description="List SPICE ingestion history for a dataset - useful for debugging refresh failures"
)
async def list_ingestions(dataset_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_ingestions(dataset_id)


@mcp.tool(
    name="describe_ingestion",
    description="Get details of a specific SPICE ingestion - status, error info, row counts"
)
async def describe_ingestion(dataset_id: str, ingestion_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_ingestion(dataset_id, ingestion_id)


@mcp.tool(
    name="get_dataset_refresh_properties",
    description="Get SPICE refresh properties for a dataset"
)
async def get_dataset_refresh_properties(dataset_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.get_dataset_refresh_properties(dataset_id)


@mcp.tool(
    name="list_refresh_schedules",
    description="List SPICE refresh schedules for a dataset"
)
async def list_refresh_schedules(dataset_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_refresh_schedules(dataset_id)


@mcp.tool(
    name="list_folders",
    description="List all QuickSight folders"
)
async def list_folders(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_folders()


@mcp.tool(
    name="describe_folder",
    description="Get details of a specific folder"
)
async def describe_folder(folder_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_folder(folder_id)


@mcp.tool(
    name="list_folder_members",
    description="List all assets (datasets, dashboards, analyses) in a folder"
)
async def list_folder_members(folder_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_folder_members(folder_id)


@mcp.tool(
    name="list_themes",
    description="List all QuickSight themes"
)
async def list_themes(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_themes()


@mcp.tool(
    name="describe_theme",
    description="Get details of a specific theme"
)
async def describe_theme(theme_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_theme(theme_id)


@mcp.tool(
    name="list_namespaces",
    description="List all QuickSight namespaces"
)
async def list_namespaces(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_namespaces()


@mcp.tool(
    name="list_users",
    description="List all QuickSight users in a namespace"
)
async def list_users(namespace: str = 'default', account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_users(namespace)


@mcp.tool(
    name="list_groups",
    description="List all QuickSight groups in a namespace"
)
async def list_groups(namespace: str = 'default', account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_groups(namespace)


@mcp.tool(
    name="list_group_memberships",
    description="List members of a QuickSight group"
)
async def list_group_memberships(group_name: str, namespace: str = 'default', account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_group_memberships(group_name, namespace)


@mcp.tool(
    name="describe_account_settings",
    description="Get QuickSight account settings - edition, notification email, etc."
)
async def describe_account_settings(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_account_settings()


@mcp.tool(
    name="list_tags_for_resource",
    description="List tags on any QuickSight resource by ARN"
)
async def list_tags_for_resource(resource_arn: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_tags_for_resource(resource_arn)


@mcp.tool(
    name="search_datasets",
    description="Search datasets by name pattern (supports wildcards)"
)
async def search_datasets(name_filter: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.search_datasets(name_filter)


@mcp.tool(
    name="search_dashboards",
    description="Search dashboards by name pattern (supports wildcards)"
)
async def search_dashboards(name_filter: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.search_dashboards(name_filter)


@mcp.tool(name="search_analyses", description="Search analyses by name pattern (supports wildcards)")
async def search_analyses(name_filter: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.search_analyses(name_filter)


@mcp.tool(name="describe_analysis", description="Get basic analysis metadata - name, status, errors, timestamps")
async def describe_analysis(analysis_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_analysis(analysis_id)


@mcp.tool(name="describe_folder_permissions", description="Get permissions for a folder")
async def describe_folder_permissions(folder_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_folder_permissions(folder_id)


@mcp.tool(name="describe_folder_resolved_permissions", description="Get effective (inherited) permissions for a folder")
async def describe_folder_resolved_permissions(folder_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_folder_resolved_permissions(folder_id)


@mcp.tool(name="describe_refresh_schedule", description="Get details of a specific SPICE refresh schedule")
async def describe_refresh_schedule(dataset_id: str, schedule_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_refresh_schedule(dataset_id, schedule_id)


@mcp.tool(name="describe_user", description="Get details of a specific QuickSight user")
async def describe_user(username: str, namespace: str = 'default', account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_user(username, namespace)


@mcp.tool(name="describe_group", description="Get details of a specific QuickSight group")
async def describe_group(group_name: str, namespace: str = 'default', account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_group(group_name, namespace)


@mcp.tool(name="describe_group_membership", description="Check if a user is a member of a group")
async def describe_group_membership(group_name: str, member_name: str, namespace: str = 'default', account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_group_membership(group_name, member_name, namespace)


@mcp.tool(name="describe_namespace", description="Get details of a specific QuickSight namespace")
async def describe_namespace(namespace: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_namespace(namespace)


@mcp.tool(name="list_dashboard_versions", description="List version history of a dashboard")
async def list_dashboard_versions(dashboard_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_dashboard_versions(dashboard_id)


@mcp.tool(name="list_user_groups", description="List all groups a QuickSight user belongs to")
async def list_user_groups(username: str, namespace: str = 'default', account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_user_groups(username, namespace)


@mcp.tool(name="list_folders_for_resource", description="List which folders contain a given resource (by ARN)")
async def list_folders_for_resource(resource_arn: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_folders_for_resource(resource_arn)


@mcp.tool(name="search_data_sources", description="Search data sources by name pattern (supports wildcards)")
async def search_data_sources(name_filter: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.search_data_sources(name_filter)


@mcp.tool(name="search_folders", description="Search folders by name pattern (supports wildcards)")
async def search_folders(name_filter: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.search_folders(name_filter)


@mcp.tool(name="search_groups", description="Search QuickSight groups by name pattern")
async def search_groups(name_filter: str, namespace: str = 'default', account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.search_groups(namespace, name_filter)


@mcp.tool(name="list_templates", description="List all QuickSight templates")
async def list_templates(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_templates()


@mcp.tool(name="describe_template", description="Get details of a specific QuickSight template")
async def describe_template(template_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_template(template_id)


@mcp.tool(name="describe_template_definition", description="Get full template definition - sheets, calculated fields, filters")
async def describe_template_definition(template_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_template_definition(template_id)


@mcp.tool(name="describe_template_permissions", description="Get permissions for a template")
async def describe_template_permissions(template_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_template_permissions(template_id)


@mcp.tool(name="list_template_versions", description="List version history of a template")
async def list_template_versions(template_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_template_versions(template_id)


@mcp.tool(name="describe_account_subscription", description="Get QuickSight account subscription info - edition, capacity pricing")
async def describe_account_subscription(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_account_subscription()


@mcp.tool(name="describe_ip_restriction", description="Get IP restriction rules for the QuickSight account")
async def describe_ip_restriction(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_ip_restriction()


@mcp.tool(name="list_vpc_connections", description="List all VPC connections")
async def list_vpc_connections(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_vpc_connections()


@mcp.tool(name="describe_vpc_connection", description="Get details of a specific VPC connection")
async def describe_vpc_connection(vpc_connection_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_vpc_connection(vpc_connection_id)


@mcp.tool(name="list_iam_policy_assignments", description="List IAM policy assignments in a namespace")
async def list_iam_policy_assignments(namespace: str = 'default', account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_iam_policy_assignments(namespace)


@mcp.tool(name="list_topics", description="List all QuickSight Q topics")
async def list_topics(account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_topics()


@mcp.tool(name="describe_topic", description="Get details of a QuickSight Q topic")
async def describe_topic(topic_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_topic(topic_id)


@mcp.tool(name="describe_topic_permissions", description="Get permissions for a Q topic")
async def describe_topic_permissions(topic_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_topic_permissions(topic_id)


@mcp.tool(name="describe_topic_refresh", description="Get status of a Q topic refresh")
async def describe_topic_refresh(topic_id: str, refresh_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.describe_topic_refresh(topic_id, refresh_id)


@mcp.tool(name="list_topic_refresh_schedules", description="List refresh schedules for a Q topic")
async def list_topic_refresh_schedules(topic_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.list_topic_refresh_schedules(topic_id)


@mcp.tool(name="search_topics", description="Search Q topics by keyword")
async def search_topics(name_filter: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.search_topics(name_filter)


@mcp.tool(name="get_dashboard_embed_url", description="Get embed URL for a dashboard")
async def get_dashboard_embed_url(dashboard_id: str, identity_type: str = 'QUICKSIGHT', session_lifetime: int = 600, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.get_dashboard_embed_url(dashboard_id, identity_type, session_lifetime)


@mcp.tool(name="get_session_embed_url", description="Get embed URL for a QuickSight console session")
async def get_session_embed_url(entry_point: str = None, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    lineage = QuickSightLineage(aws_account_id=account_id, region=region)
    return lineage.get_session_embed_url(entry_point)


if __name__ == "__main__":
    mcp.run(transport='stdio')
