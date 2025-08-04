import pymysql
from typing import List, Dict, Any, Optional
import pandas as pd
from app.core.base_extractor import BaseLineageExtractor
from app.models.lineage import LineageNode, LineageEdge, TableInfo, ColumnInfo
from datetime import datetime


class MySQLLineageExtractor(BaseLineageExtractor):
    """MySQL-specific lineage extractor using information_schema"""
    
    async def connect(self) -> bool:
        """Establish connection to MySQL"""
        try:
            self.connection = pymysql.connect(
                host=self.connection_params.get('host'),
                port=self.connection_params.get('port', 3306),
                database=self.connection_params.get('database'),
                user=self.connection_params.get('user'),
                password=self.connection_params.get('password'),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            return True
        except Exception as e:
            print(f"Failed to connect to MySQL: {e}")
            return False
    
    async def disconnect(self):
        """Close MySQL connection"""
        if self.connection:
            self.connection.close()
    
    async def get_table_metadata(self, database: str, schema: str, table: str) -> TableInfo:
        """Get metadata for a specific MySQL table"""
        
        # In MySQL, database and schema are the same concept
        actual_database = database or schema
        
        # Query to get table information
        table_query = """
        SELECT 
            TABLE_NAME,
            TABLE_TYPE,
            TABLE_COMMENT,
            CREATE_TIME,
            UPDATE_TIME
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_NAME = %s
        """
        
        # Query to get column information
        columns_query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """
        
        cursor = self.connection.cursor()
        
        # Get table info
        cursor.execute(table_query, (actual_database, table))
        table_result = cursor.fetchone()
        
        if not table_result:
            raise ValueError(f"Table {actual_database}.{table} not found")
        
        # Get columns info
        cursor.execute(columns_query, (actual_database, table))
        columns_result = cursor.fetchall()
        
        columns = []
        for col in columns_result:
            columns.append(ColumnInfo(
                column_name=col['COLUMN_NAME'],
                data_type=col['DATA_TYPE'],
                is_nullable=col['IS_NULLABLE'] == 'YES',
                default_value=col['COLUMN_DEFAULT'],
                comment=col['COLUMN_COMMENT']
            ))
        
        return TableInfo(
            database_name=actual_database,
            schema_name=actual_database,  # In MySQL, schema == database
            table_name=table_result['TABLE_NAME'],
            table_type=table_result['TABLE_TYPE'],
            columns=columns,
            comment=table_result['TABLE_COMMENT'],
            created_date=table_result['CREATE_TIME'],
            last_modified=table_result['UPDATE_TIME']
        )
    
    async def get_upstream_lineage(self, database: str, schema: str, table: str, 
                                 column: Optional[str] = None, max_depth: int = 5) -> List[LineageNode]:
        """Get upstream dependencies using MySQL information_schema for views"""
        
        nodes = []
        actual_database = database or schema
        
        # Query to find view dependencies
        view_deps_query = """
        SELECT DISTINCT
            TABLE_NAME as view_name,
            VIEW_DEFINITION
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = %s
        AND TABLE_NAME = %s
        """
        
        cursor = self.connection.cursor()
        cursor.execute(view_deps_query, (actual_database, table))
        view_result = cursor.fetchone()
        
        if view_result:
            view_definition = view_result['VIEW_DEFINITION'].lower()
            
            # Get all tables in the database to check for references
            all_tables_query = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
            AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            """
            
            cursor.execute(all_tables_query, (actual_database,))
            all_tables = cursor.fetchall()
            
            # Look for table references in view definition
            for table_row in all_tables:
                source_table = table_row['TABLE_NAME']
                if source_table != table and source_table.lower() in view_definition:
                    try:
                        source_table_info = await self.get_table_metadata(actual_database, actual_database, source_table)
                        
                        for col in source_table_info.columns:
                            # Check if column is referenced in view definition
                            if column is None or col.column_name.lower() in view_definition:
                                node = LineageNode(
                                    id=f"{actual_database}.{actual_database}.{source_table}.{col.column_name}",
                                    database_name=actual_database,
                                    schema_name=actual_database,
                                    table_name=source_table,
                                    column_name=col.column_name,
                                    data_type=col.data_type,
                                    node_type="source",
                                    table_type=source_table_info.table_type
                                )
                                nodes.append(node)
                    
                    except Exception as e:
                        print(f"Error getting metadata for {source_table}: {e}")
                        continue
        
        return nodes
    
    async def get_downstream_lineage(self, database: str, schema: str, table: str,
                                   column: Optional[str] = None, max_depth: int = 5) -> List[LineageNode]:
        """Get downstream dependencies by finding views that reference this table"""
        
        nodes = []
        actual_database = database or schema
        
        # Query to find views that might reference this table
        views_query = """
        SELECT 
            TABLE_NAME as view_name,
            VIEW_DEFINITION
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = %s
        """
        
        cursor = self.connection.cursor()
        cursor.execute(views_query, (actual_database,))
        views_result = cursor.fetchall()
        
        for view_row in views_result:
            view_name = view_row['view_name']
            view_definition = view_row['VIEW_DEFINITION'].lower()
            
            # Check if this table is referenced in the view
            if table.lower() in view_definition:
                try:
                    view_table_info = await self.get_table_metadata(actual_database, actual_database, view_name)
                    
                    for col in view_table_info.columns:
                        node = LineageNode(
                            id=f"{actual_database}.{actual_database}.{view_name}.{col.column_name}",
                            database_name=actual_database,
                            schema_name=actual_database,
                            table_name=view_name,
                            column_name=col.column_name,
                            data_type=col.data_type,
                            node_type="transformation",
                            table_type=view_table_info.table_type
                        )
                        nodes.append(node)
                
                except Exception as e:
                    print(f"Error getting metadata for view {view_name}: {e}")
                    continue
        
        return nodes
    
    async def get_lineage_edges(self, source_nodes: List[LineageNode], 
                              target_nodes: List[LineageNode]) -> List[LineageEdge]:
        """Get relationships between nodes using view definitions"""
        
        edges = []
        
        # Get all view definitions for analysis
        views_query = """
        SELECT 
            TABLE_NAME as view_name,
            VIEW_DEFINITION
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = %s
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
            cursor.execute(views_query, (schema,))
            views_result = cursor.fetchall()
            
            for view_row in views_result:
                view_name = view_row['view_name']
                view_definition = view_row['VIEW_DEFINITION'].lower()
                
                view_key = f"{schema}.{view_name}"
                if view_key in table_nodes:
                    view_nodes = table_nodes[view_key]
                    
                    for view_node in view_nodes:
                        # Look for source tables and columns in the view definition
                        for source_table_key, source_nodes_list in table_nodes.items():
                            if source_table_key != view_key:
                                source_schema, source_table = source_table_key.split('.')
                                
                                # Check if source table is referenced in view
                                if source_table.lower() in view_definition:
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
        elif any(func in definition_lower for func in ['case when', 'if(', 'ifnull(', 'coalesce(']):
            return "calculation"
        else:
            return "transformation"
