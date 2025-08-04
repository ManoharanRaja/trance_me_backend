from google.cloud import bigquery
from typing import List, Dict, Any, Optional
import pandas as pd
from app.core.base_extractor import BaseLineageExtractor
from app.models.lineage import LineageNode, LineageEdge, TableInfo, ColumnInfo
from datetime import datetime


class BigQueryLineageExtractor(BaseLineageExtractor):
    """BigQuery-specific lineage extractor using Metadata APIs and INFORMATION_SCHEMA"""
    
    def __init__(self, connection_params: Dict[str, Any]):
        super().__init__(connection_params)
        self.project_id = connection_params.get('project_id')
        self.dataset_id = connection_params.get('dataset_id')
    
    async def connect(self) -> bool:
        """Establish connection to BigQuery"""
        try:
            # Initialize BigQuery client
            if 'credentials_path' in self.connection_params:
                self.connection = bigquery.Client.from_service_account_json(
                    self.connection_params['credentials_path'],
                    project=self.project_id
                )
            else:
                self.connection = bigquery.Client(project=self.project_id)
            
            # Test connection
            list(self.connection.list_datasets(max_results=1))
            return True
        except Exception as e:
            print(f"Failed to connect to BigQuery: {e}")
            return False
    
    async def disconnect(self):
        """Close BigQuery connection"""
        if self.connection:
            self.connection.close()
    
    async def get_table_metadata(self, database: str, schema: str, table: str) -> TableInfo:
        """Get metadata for a specific BigQuery table"""
        
        # In BigQuery: database=project, schema=dataset, table=table
        project_id = database or self.project_id
        dataset_id = schema or self.dataset_id
        
        # Get table reference
        table_ref = self.connection.dataset(dataset_id, project=project_id).table(table)
        
        try:
            table_obj = self.connection.get_table(table_ref)
        except Exception:
            raise ValueError(f"Table {project_id}.{dataset_id}.{table} not found")
        
        # Get column information
        columns = []
        for field in table_obj.schema:
            columns.append(ColumnInfo(
                column_name=field.name,
                data_type=field.field_type,
                is_nullable=field.mode != 'REQUIRED',
                default_value=None,
                comment=field.description
            ))
        
        # Determine table type
        table_type = "BASE TABLE"
        if table_obj.table_type == bigquery.TableType.VIEW:
            table_type = "VIEW"
        elif table_obj.table_type == bigquery.TableType.MATERIALIZED_VIEW:
            table_type = "MATERIALIZED VIEW"
        
        return TableInfo(
            database_name=project_id,
            schema_name=dataset_id,
            table_name=table,
            table_type=table_type,
            columns=columns,
            comment=table_obj.description,
            created_date=table_obj.created,
            last_modified=table_obj.modified
        )
    
    async def get_upstream_lineage(self, database: str, schema: str, table: str, 
                                 column: Optional[str] = None, max_depth: int = 5) -> List[LineageNode]:
        """Get upstream dependencies using BigQuery INFORMATION_SCHEMA"""
        
        nodes = []
        project_id = database or self.project_id
        dataset_id = schema or self.dataset_id
        
        # Query to find view dependencies using INFORMATION_SCHEMA
        view_deps_query = f"""
        SELECT 
            ddl as view_definition
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = '{table}'
        AND table_type = 'VIEW'
        """
        
        try:
            query_job = self.connection.query(view_deps_query)
            results = query_job.result()
            
            for row in results:
                view_definition = row.view_definition.lower() if row.view_definition else ""
                
                # Use a more comprehensive query to find all tables in the project
                all_tables_query = f"""
                SELECT 
                    table_catalog as project_id,
                    table_schema as dataset_id,
                    table_name
                FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
                WHERE table_type IN ('BASE TABLE', 'VIEW')
                """
                
                tables_job = self.connection.query(all_tables_query)
                tables_results = tables_job.result()
                
                # Look for table references in view definition
                for table_row in tables_results:
                    source_project = table_row.project_id
                    source_dataset = table_row.dataset_id
                    source_table = table_row.table_name
                    
                    if source_table != table and source_table.lower() in view_definition:
                        try:
                            source_table_info = await self.get_table_metadata(
                                source_project, source_dataset, source_table
                            )
                            
                            for col in source_table_info.columns:
                                if column is None or col.column_name.lower() in view_definition:
                                    node = LineageNode(
                                        id=f"{source_project}.{source_dataset}.{source_table}.{col.column_name}",
                                        database_name=source_project,
                                        schema_name=source_dataset,
                                        table_name=source_table,
                                        column_name=col.column_name,
                                        data_type=col.data_type,
                                        node_type="source",
                                        table_type=source_table_info.table_type
                                    )
                                    nodes.append(node)
                        
                        except Exception as e:
                            print(f"Error getting metadata for {source_project}.{source_dataset}.{source_table}: {e}")
                            continue
        
        except Exception as e:
            print(f"Error querying BigQuery lineage: {e}")
        
        return nodes
    
    async def get_downstream_lineage(self, database: str, schema: str, table: str,
                                   column: Optional[str] = None, max_depth: int = 5) -> List[LineageNode]:
        """Get downstream dependencies by finding views that reference this table"""
        
        nodes = []
        project_id = database or self.project_id
        dataset_id = schema or self.dataset_id
        
        # Query to find views that might reference this table
        views_query = f"""
        SELECT 
            table_catalog as project_id,
            table_schema as dataset_id,
            table_name,
            ddl as view_definition
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        WHERE table_type = 'VIEW'
        """
        
        try:
            query_job = self.connection.query(views_query)
            results = query_job.result()
            
            for row in results:
                view_project = row.project_id
                view_dataset = row.dataset_id
                view_name = row.table_name
                view_definition = row.view_definition.lower() if row.view_definition else ""
                
                # Check if this table is referenced in the view
                table_reference = f"{project_id}.{dataset_id}.{table}".lower()
                if table_reference in view_definition or table.lower() in view_definition:
                    try:
                        view_table_info = await self.get_table_metadata(view_project, view_dataset, view_name)
                        
                        for col in view_table_info.columns:
                            node = LineageNode(
                                id=f"{view_project}.{view_dataset}.{view_name}.{col.column_name}",
                                database_name=view_project,
                                schema_name=view_dataset,
                                table_name=view_name,
                                column_name=col.column_name,
                                data_type=col.data_type,
                                node_type="transformation",
                                table_type=view_table_info.table_type
                            )
                            nodes.append(node)
                    
                    except Exception as e:
                        print(f"Error getting metadata for view {view_project}.{view_dataset}.{view_name}: {e}")
                        continue
        
        except Exception as e:
            print(f"Error querying BigQuery downstream lineage: {e}")
        
        return nodes
    
    async def get_lineage_edges(self, source_nodes: List[LineageNode], 
                              target_nodes: List[LineageNode]) -> List[LineageEdge]:
        """Get relationships between nodes using view definitions and job history"""
        
        edges = []
        
        # Group nodes by table
        table_nodes = {}
        for node in source_nodes + target_nodes:
            table_key = f"{node.database_name}.{node.schema_name}.{node.table_name}"
            if table_key not in table_nodes:
                table_nodes[table_key] = []
            table_nodes[table_key].append(node)
        
        # Get view definitions for analysis
        unique_datasets = set(f"{node.database_name}.{node.schema_name}" for node in source_nodes + target_nodes)
        
        for dataset_key in unique_datasets:
            project_id, dataset_id = dataset_key.split('.', 1)
            
            views_query = f"""
            SELECT 
                table_name,
                ddl as view_definition
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type = 'VIEW'
            """
            
            try:
                query_job = self.connection.query(views_query)
                results = query_job.result()
                
                for row in results:
                    view_name = row.table_name
                    view_definition = row.view_definition.lower() if row.view_definition else ""
                    
                    view_key = f"{project_id}.{dataset_id}.{view_name}"
                    if view_key in table_nodes:
                        view_nodes = table_nodes[view_key]
                        
                        for view_node in view_nodes:
                            # Look for source tables and columns in the view definition
                            for source_table_key, source_nodes_list in table_nodes.items():
                                if source_table_key != view_key:
                                    source_parts = source_table_key.split('.')
                                    if len(source_parts) >= 3:
                                        source_project, source_dataset, source_table = source_parts[0], source_parts[1], source_parts[2]
                                        
                                        # Check if source table is referenced in view
                                        if (source_table.lower() in view_definition or 
                                            f"{source_project}.{source_dataset}.{source_table}".lower() in view_definition):
                                            
                                            for source_node in source_nodes_list:
                                                # Check if column is referenced
                                                if source_node.column_name.lower() in view_definition:
                                                    transformation_type = self._analyze_transformation_type(
                                                        view_definition, source_node.column_name, view_node.column_name
                                                    )
                                                    
                                                    edge = LineageEdge(
                                                        source_id=source_node.id,
                                                        target_id=view_node.id,
                                                        transformation_type=transformation_type,
                                                        transformation_logic=view_definition[:500],
                                                        confidence_score=0.8
                                                    )
                                                    edges.append(edge)
            
            except Exception as e:
                print(f"Error analyzing view definitions for {dataset_key}: {e}")
                continue
        
        return edges
    
    def _analyze_transformation_type(self, view_definition: str, source_col: str, target_col: str) -> str:
        """Analyze view definition to determine transformation type"""
        definition_lower = view_definition.lower()
        source_col_lower = source_col.lower()
        target_col_lower = target_col.lower()
        
        if f"{source_col_lower} as {target_col_lower}" in definition_lower:
            return "direct"
        elif any(agg in definition_lower for agg in ['sum(', 'count(', 'avg(', 'max(', 'min(', 'array_agg(']):
            return "aggregation"
        elif 'join' in definition_lower:
            return "join"
        elif any(func in definition_lower for func in ['case when', 'if(', 'ifnull(', 'coalesce(', 'safe_cast(']):
            return "calculation"
        else:
            return "transformation"
