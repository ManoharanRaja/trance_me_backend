import pyodbc
from typing import List, Dict, Any, Optional
import pandas as pd
from app.core.base_extractor import BaseLineageExtractor
from app.models.lineage import LineageNode, LineageEdge, TableInfo, ColumnInfo
from datetime import datetime


class SQLServerLineageExtractor(BaseLineageExtractor):
    """SQL Server-specific lineage extractor using sys.objects, sys.columns, and DMV views"""
    
    async def connect(self) -> bool:
        """Establish connection to SQL Server"""
        try:
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.connection_params.get('host')},"
                f"{self.connection_params.get('port', 1433)};"
                f"DATABASE={self.connection_params.get('database')};"
                f"UID={self.connection_params.get('user')};"
                f"PWD={self.connection_params.get('password')}"
            )
            self.connection = pyodbc.connect(connection_string)
            return True
        except Exception as e:
            print(f"Failed to connect to SQL Server: {e}")
            return False
    
    async def disconnect(self):
        """Close SQL Server connection"""
        if self.connection:
            self.connection.close()
    
    async def get_table_metadata(self, database: str, schema: str, table: str) -> TableInfo:
        """Get metadata for a specific SQL Server table"""
        
        # Query to get table information
        table_query = """
        SELECT 
            t.TABLE_NAME,
            t.TABLE_TYPE,
            ep.value as table_comment,
            o.create_date,
            o.modify_date
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN sys.objects o ON o.name = t.TABLE_NAME
        LEFT JOIN sys.extended_properties ep ON ep.major_id = o.object_id 
            AND ep.minor_id = 0 AND ep.name = 'MS_Description'
        WHERE t.TABLE_SCHEMA = ? 
        AND t.TABLE_NAME = ?
        """
        
        # Query to get column information
        columns_query = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            ep.value as column_comment
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN sys.columns sc ON sc.object_id = OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME) 
            AND sc.name = c.COLUMN_NAME
        LEFT JOIN sys.extended_properties ep ON ep.major_id = sc.object_id 
            AND ep.minor_id = sc.column_id AND ep.name = 'MS_Description'
        WHERE c.TABLE_SCHEMA = ? 
        AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        """
        
        cursor = self.connection.cursor()
        
        # Get table info
        cursor.execute(table_query, (schema, table))
        table_result = cursor.fetchone()
        
        if not table_result:
            raise ValueError(f"Table {schema}.{table} not found")
        
        # Get columns info
        cursor.execute(columns_query, (schema, table))
        columns_result = cursor.fetchall()
        
        columns = []
        for col in columns_result:
            columns.append(ColumnInfo(
                column_name=col[0],
                data_type=col[1],
                is_nullable=col[2] == 'YES',
                default_value=col[3],
                comment=col[4]
            ))
        
        return TableInfo(
            database_name=database,
            schema_name=schema,
            table_name=table_result[0],
            table_type=table_result[1],
            columns=columns,
            comment=table_result[2],
            created_date=table_result[3],
            last_modified=table_result[4]
        )
    
    async def get_upstream_lineage(self, database: str, schema: str, table: str, 
                                 column: Optional[str] = None, max_depth: int = 5) -> List[LineageNode]:
        """Get upstream dependencies using SQL Server system views"""
        
        nodes = []
        
        # Query to find view dependencies using sys.sql_dependencies
        view_deps_query = """
        WITH ViewDependencies AS (
            SELECT DISTINCT
                OBJECT_SCHEMA_NAME(d.object_id) as dependent_schema,
                OBJECT_NAME(d.object_id) as dependent_object,
                OBJECT_SCHEMA_NAME(d.referenced_major_id) as referenced_schema,
                OBJECT_NAME(d.referenced_major_id) as referenced_object,
                1 as depth
            FROM sys.sql_dependencies d
            JOIN sys.objects o ON o.object_id = d.object_id
            WHERE OBJECT_SCHEMA_NAME(d.object_id) = ?
            AND OBJECT_NAME(d.object_id) = ?
            AND o.type IN ('V', 'IF', 'TF')  -- Views and functions
            
            UNION ALL
            
            SELECT DISTINCT
                OBJECT_SCHEMA_NAME(d.object_id) as dependent_schema,
                OBJECT_NAME(d.object_id) as dependent_object,
                OBJECT_SCHEMA_NAME(d.referenced_major_id) as referenced_schema,
                OBJECT_NAME(d.referenced_major_id) as referenced_object,
                vd.depth + 1
            FROM sys.sql_dependencies d
            JOIN sys.objects o ON o.object_id = d.object_id
            JOIN ViewDependencies vd ON vd.dependent_schema = OBJECT_SCHEMA_NAME(d.referenced_major_id)
                AND vd.dependent_object = OBJECT_NAME(d.referenced_major_id)
            WHERE vd.depth < ?
            AND o.type IN ('V', 'IF', 'TF')
        )
        SELECT DISTINCT
            referenced_schema,
            referenced_object,
            depth
        FROM ViewDependencies
        WHERE referenced_schema IS NOT NULL 
        AND referenced_object IS NOT NULL
        ORDER BY depth
        """
        
        cursor = self.connection.cursor()
        cursor.execute(view_deps_query, (schema, table, max_depth))
        results = cursor.fetchall()
        
        for result in results:
            source_schema = result[0]
            source_table = result[1]
            
            if source_schema and source_table:
                try:
                    source_table_info = await self.get_table_metadata(database, source_schema, source_table)
                    
                    for col in source_table_info.columns:
                        node = LineageNode(
                            id=f"{database}.{source_schema}.{source_table}.{col.column_name}",
                            database_name=database,
                            schema_name=source_schema,
                            table_name=source_table,
                            column_name=col.column_name,
                            data_type=col.data_type,
                            node_type="source",
                            table_type=source_table_info.table_type
                        )
                        nodes.append(node)
                
                except Exception as e:
                    print(f"Error getting metadata for {source_schema}.{source_table}: {e}")
                    continue
        
        return nodes
    
    async def get_downstream_lineage(self, database: str, schema: str, table: str,
                                   column: Optional[str] = None, max_depth: int = 5) -> List[LineageNode]:
        """Get downstream dependencies using SQL Server system views"""
        
        nodes = []
        
        # Query to find what depends on this table
        downstream_query = """
        WITH DownstreamDependencies AS (
            SELECT DISTINCT
                OBJECT_SCHEMA_NAME(d.referenced_major_id) as referenced_schema,
                OBJECT_NAME(d.referenced_major_id) as referenced_object,
                OBJECT_SCHEMA_NAME(d.object_id) as dependent_schema,
                OBJECT_NAME(d.object_id) as dependent_object,
                1 as depth
            FROM sys.sql_dependencies d
            JOIN sys.objects o ON o.object_id = d.object_id
            WHERE OBJECT_SCHEMA_NAME(d.referenced_major_id) = ?
            AND OBJECT_NAME(d.referenced_major_id) = ?
            AND o.type IN ('V', 'IF', 'TF', 'P')  -- Views, functions, procedures
            
            UNION ALL
            
            SELECT DISTINCT
                OBJECT_SCHEMA_NAME(d.referenced_major_id) as referenced_schema,
                OBJECT_NAME(d.referenced_major_id) as referenced_object,
                OBJECT_SCHEMA_NAME(d.object_id) as dependent_schema,
                OBJECT_NAME(d.object_id) as dependent_object,
                dd.depth + 1
            FROM sys.sql_dependencies d
            JOIN sys.objects o ON o.object_id = d.object_id
            JOIN DownstreamDependencies dd ON dd.dependent_schema = OBJECT_SCHEMA_NAME(d.referenced_major_id)
                AND dd.dependent_object = OBJECT_NAME(d.referenced_major_id)
            WHERE dd.depth < ?
            AND o.type IN ('V', 'IF', 'TF', 'P')
        )
        SELECT DISTINCT
            dependent_schema,
            dependent_object,
            depth
        FROM DownstreamDependencies
        WHERE dependent_schema IS NOT NULL 
        AND dependent_object IS NOT NULL
        ORDER BY depth
        """
        
        cursor = self.connection.cursor()
        cursor.execute(downstream_query, (schema, table, max_depth))
        results = cursor.fetchall()
        
        for result in results:
            dependent_schema = result[0]
            dependent_table = result[1]
            
            if dependent_schema and dependent_table:
                try:
                    dependent_table_info = await self.get_table_metadata(database, dependent_schema, dependent_table)
                    
                    for col in dependent_table_info.columns:
                        node = LineageNode(
                            id=f"{database}.{dependent_schema}.{dependent_table}.{col.column_name}",
                            database_name=database,
                            schema_name=dependent_schema,
                            table_name=dependent_table,
                            column_name=col.column_name,
                            data_type=col.data_type,
                            node_type="transformation",
                            table_type=dependent_table_info.table_type
                        )
                        nodes.append(node)
                
                except Exception as e:
                    print(f"Error getting metadata for {dependent_schema}.{dependent_table}: {e}")
                    continue
        
        return nodes
    
    async def get_lineage_edges(self, source_nodes: List[LineageNode], 
                              target_nodes: List[LineageNode]) -> List[LineageEdge]:
        """Get relationships between nodes using view definitions"""
        
        edges = []
        
        # Query to get view definitions
        view_def_query = """
        SELECT 
            s.name as schema_name,
            o.name as view_name,
            m.definition
        FROM sys.objects o
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        JOIN sys.sql_modules m ON m.object_id = o.object_id
        WHERE o.type = 'V'  -- Views only
        AND s.name = ?
        """
        
        cursor = self.connection.cursor()
        
        # Group nodes by table
        table_nodes = {}
        for node in source_nodes + target_nodes:
            table_key = f"{node.schema_name}.{node.table_name}"
            if table_key not in table_nodes:
                table_nodes[table_key] = []
            table_nodes[table_key].append(node)
        
        # Get view definitions for each schema
        unique_schemas = set(node.schema_name for node in source_nodes + target_nodes)
        for schema in unique_schemas:
            cursor.execute(view_def_query, (schema,))
            view_results = cursor.fetchall()
            
            for view_result in view_results:
                view_schema = view_result[0]
                view_name = view_result[1]
                view_definition = view_result[2].lower() if view_result[2] else ""
                
                view_key = f"{view_schema}.{view_name}"
                if view_key in table_nodes:
                    view_nodes = table_nodes[view_key]
                    
                    for view_node in view_nodes:
                        # Look for source tables and columns in the view definition
                        for source_table_key, source_nodes_list in table_nodes.items():
                            if source_table_key != view_key:
                                source_schema, source_table = source_table_key.split('.')
                                
                                # Check if source table is referenced in view
                                if (source_table.lower() in view_definition or 
                                    f"{source_schema}.{source_table}".lower() in view_definition):
                                    
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
        
        return edges
    
    def _analyze_transformation_type(self, view_definition: str, source_col: str, target_col: str) -> str:
        """Analyze view definition to determine transformation type"""
        definition_lower = view_definition.lower()
        source_col_lower = source_col.lower()
        target_col_lower = target_col.lower()
        
        if f"{source_col_lower} as {target_col_lower}" in definition_lower:
            return "direct"
        elif any(agg in definition_lower for agg in ['sum(', 'count(', 'avg(', 'max(', 'min(']):
            return "aggregation"
        elif 'join' in definition_lower:
            return "join"
        elif any(func in definition_lower for func in ['case when', 'isnull(', 'coalesce(', 'cast(']):
            return "calculation"
        else:
            return "transformation"
