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
    

    def get_analysis_details(self, analysis_id: str) -> Dict[str, Any]:
        """
        获取分析的sheet和visual信息
        
        参数:
            analysis_id (str): 分析ID
            
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
        """
        获取仪表板的sheet和visual信息
        
        参数:
            dashboard_id (str): 仪表板ID
            
        返回:
            Dict[str, Any]: 包含sheet和visual信息的字典，如果出错则返回空结构
        """
        try:
            # 获取仪表板详细信息
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
            logger.error(f"获取仪表板 {dashboard_id} 的sheet信息时出错: {str(e)}")
            return {
                'DashboardId': dashboard_id,
                'Error': str(e)
            }
        return result



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
    description="分析指定分析的血缘关系"
)
async def analyze_analysis_id(analysis_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    """ 
        参数:
            analysis_id (str): 分析ID
            
        返回:
            Dict[str, Any]: 包含分析血缘关系的字典
    """
    
    # 创建 QuickSight 血缘分析工具实例
    lineage = QuickSightLineage(
        aws_account_id=account_id,
        region=region
    )
    
    analysis_details = lineage.get_analysis_details(analysis_id)

    return analysis_details


@mcp.tool(
    name="analyze_dashboard",
    description="分析指定仪表板的血缘关系"
)
async def analyze_dashboard_id(dashboard_id: str, account_id: str = DEFAULT_ACCOUNT_ID, region: str = DEFAULT_REGION) -> Dict[str, Any]:
    """
        参数:
            dashboard_id (str): 仪表板ID
            
        返回:
            Dict[str, Any]: 包含仪表板血缘关系的字典
    """
    
    # 创建 QuickSight 血缘分析工具实例
    lineage = QuickSightLineage(
        aws_account_id=account_id,
        region=region
    )
    
    dashboard_details = lineage.get_dashboard_details(dashboard_id)

    return dashboard_details


if __name__ == "__main__":
    mcp.run(transport='stdio')