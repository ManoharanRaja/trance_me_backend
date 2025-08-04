import snowflake.connector
from typing import List, Dict, Any, Optional
import pandas as pd
from app.core.base_extractor import BaseLineageExtractor
from app.models.lineage import LineageNode, LineageEdge, TableInfo, ColumnInfo
from datetime import datetime


class SnowflakeLineageExtractor(BaseLineageExtractor):
    """Snowflake-specific lineage extractor using INFORMATION_SCHEMA and ACCESS_HISTORY"""
    
    async def connect(self) -> bool:
        """Establish connection to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(
                account=self.connection_params.get('account'),
                user=self.connection_params.get('user'),
                password=self.connection_params.get('password'),
                warehouse=self.connection_params.get('warehouse'),
                database=self.connection_params.get('database'),
                schema=self.connection_params.get('schema')
            )
            return True
        except Exception as e:
            print(f"Failed to connect to Snowflake: {e}")
            return False
    
    async def disconnect(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
    
    async def get_table_metadata(self, database: str, schema: str, table: str) -> TableInfo:
        """Get metadata for a specific Snowflake table"""
        
        # Query to get table information
        table_query = f"""
        SELECT 
            table_name,
            table_type,
            comment,
            created as created_date,
            last_altered as last_modified
        FROM {database}.INFORMATION_SCHEMA.TABLES 
        WHERE table_schema = '{schema}' 
        AND table_name = '{table}'
        """
        
        # Query to get column information
        columns_query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default as default_value,
            comment
        FROM {database}.INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema = '{schema}' 
        AND table_name = '{table}'
        ORDER BY ordinal_position
        """
        
        cursor = self.connection.cursor()
        
        # Get table info
        cursor.execute(table_query)
        table_result = cursor.fetchone()
        
        if not table_result:
            raise ValueError(f"Table {database}.{schema}.{table} not found")
        
        # Get columns info
        cursor.execute(columns_query)
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
        """Get upstream dependencies using Snowflake's ACCESS_HISTORY and OBJECT_DEPENDENCIES"""
        
        nodes = []
        
        # Query using ACCESS_HISTORY to find source tables
        lineage_query = f"""
        WITH RECURSIVE lineage_cte AS (
            -- Base case: direct dependencies
            SELECT DISTINCT
                objects_modified[0]:objectName::string as target_table,
                objects_modified[0]:objectDomain::string as target_domain,
                value:objectName::string as source_table,
                value:objectDomain::string as source_domain,
                value:columns as columns_used,
                1 as depth
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
                 LATERAL FLATTEN(input => ah.DIRECT_OBJECTS_ACCESSED) f
            WHERE objects_modified[0]:objectName::string = '{database}.{schema}.{table}'
            AND objects_modified[0]:objectDomain::string = 'Table'
            AND query_start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
            
            UNION ALL
            
            -- Recursive case: find dependencies of dependencies
            SELECT DISTINCT
                ah.objects_modified[0]:objectName::string as target_table,
                ah.objects_modified[0]:objectDomain::string as target_domain,
                f.value:objectName::string as source_table,
                f.value:objectDomain::string as source_domain,
                f.value:columns as columns_used,
                lc.depth + 1
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
                 LATERAL FLATTEN(input => ah.DIRECT_OBJECTS_ACCESSED) f,
                 lineage_cte lc
            WHERE ah.objects_modified[0]:objectName::string = lc.source_table
            AND ah.objects_modified[0]:objectDomain::string = 'Table'
            AND lc.depth < {max_depth}
            AND ah.query_start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
        )
        SELECT DISTINCT
            source_table,
            source_domain,
            columns_used,
            depth
        FROM lineage_cte
        WHERE source_domain = 'Table'
        ORDER BY depth, source_table
        """
        
        cursor = self.connection.cursor()
        cursor.execute(lineage_query)
        results = cursor.fetchall()
        
        for result in results:
            source_table_full = result[0]
            columns_used = result[2] if result[2] else []
            
            # Parse table name
            parts = source_table_full.split('.')
            if len(parts) >= 3:
                src_db, src_schema, src_table = parts[0], parts[1], parts[2]
                
                # Get table metadata for source table
                try:
                    source_table_info = await self.get_table_metadata(src_db, src_schema, src_table)
                    
                    # Create nodes for relevant columns
                    relevant_columns = []
                    if column and columns_used:
                        # Filter columns based on the target column analysis
                        relevant_columns = [col for col in source_table_info.columns 
                                          if col.column_name in [c.get('columnName', '') for c in columns_used]]
                    else:
                        relevant_columns = source_table_info.columns
                    
                    for col in relevant_columns:
                        node = LineageNode(
                            id=f"{src_db}.{src_schema}.{src_table}.{col.column_name}",
                            database_name=src_db,
                            schema_name=src_schema,
                            table_name=src_table,
                            column_name=col.column_name,
                            data_type=col.data_type,
                            node_type="source",
                            table_type=source_table_info.table_type
                        )
                        nodes.append(node)
                
                except Exception as e:
                    print(f"Error getting metadata for {source_table_full}: {e}")
                    continue
        
        return nodes
    
    async def get_downstream_lineage(self, database: str, schema: str, table: str,
                                   column: Optional[str] = None, max_depth: int = 5) -> List[LineageNode]:
        """Get downstream dependencies using Snowflake's ACCESS_HISTORY"""
        
        nodes = []
        
        # Query to find tables that depend on this table
        downstream_query = f"""
        WITH RECURSIVE downstream_cte AS (
            -- Base case: direct downstream dependencies
            SELECT DISTINCT
                f.value:objectName::string as source_table,
                objects_modified[0]:objectName::string as target_table,
                objects_modified[0]:objectDomain::string as target_domain,
                1 as depth
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
                 LATERAL FLATTEN(input => ah.DIRECT_OBJECTS_ACCESSED) f
            WHERE f.value:objectName::string = '{database}.{schema}.{table}'
            AND f.value:objectDomain::string = 'Table'
            AND objects_modified[0]:objectDomain::string = 'Table'
            AND query_start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
            
            UNION ALL
            
            -- Recursive case: find dependencies of dependencies
            SELECT DISTINCT
                dc.target_table as source_table,
                ah.objects_modified[0]:objectName::string as target_table,
                ah.objects_modified[0]:objectDomain::string as target_domain,
                dc.depth + 1
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
                 LATERAL FLATTEN(input => ah.DIRECT_OBJECTS_ACCESSED) f,
                 downstream_cte dc
            WHERE f.value:objectName::string = dc.target_table
            AND f.value:objectDomain::string = 'Table'
            AND ah.objects_modified[0]:objectDomain::string = 'Table'
            AND dc.depth < {max_depth}
            AND ah.query_start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
        )
        SELECT DISTINCT
            target_table,
            target_domain,
            depth
        FROM downstream_cte
        WHERE target_domain = 'Table'
        ORDER BY depth, target_table
        """
        
        cursor = self.connection.cursor()
        cursor.execute(downstream_query)
        results = cursor.fetchall()
        
        for result in results:
            target_table_full = result[0]
            
            # Parse table name
            parts = target_table_full.split('.')
            if len(parts) >= 3:
                tgt_db, tgt_schema, tgt_table = parts[0], parts[1], parts[2]
                
                try:
                    target_table_info = await self.get_table_metadata(tgt_db, tgt_schema, tgt_table)
                    
                    for col in target_table_info.columns:
                        node = LineageNode(
                            id=f"{tgt_db}.{tgt_schema}.{tgt_table}.{col.column_name}",
                            database_name=tgt_db,
                            schema_name=tgt_schema,
                            table_name=tgt_table,
                            column_name=col.column_name,
                            data_type=col.data_type,
                            node_type="transformation",
                            table_type=target_table_info.table_type
                        )
                        nodes.append(node)
                
                except Exception as e:
                    print(f"Error getting metadata for {target_table_full}: {e}")
                    continue
        
        return nodes
    
    async def get_lineage_edges(self, source_nodes: List[LineageNode], 
                              target_nodes: List[LineageNode]) -> List[LineageEdge]:
        """Get relationships between nodes using query history analysis"""
        
        edges = []
        
        # Query to find actual column-level dependencies from query history
        edge_query = """
        SELECT DISTINCT
            query_text,
            f.value:objectName::string as source_object,
            f.value:columns as source_columns,
            objects_modified[0]:objectName::string as target_object,
            objects_modified[0]:columns as target_columns
        FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
             LATERAL FLATTEN(input => ah.DIRECT_OBJECTS_ACCESSED) f
        WHERE query_start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
        AND f.value:objectDomain::string = 'Table'
        AND objects_modified[0]:objectDomain::string = 'Table'
        LIMIT 1000
        """
        
        cursor = self.connection.cursor()
        cursor.execute(edge_query)
        results = cursor.fetchall()
        
        # Create a mapping for easier lookup
        node_lookup = {node.id: node for node in source_nodes + target_nodes}
        
        for result in results:
            query_text = result[0]
            source_object = result[1]
            source_columns = result[2] if result[2] else []
            target_object = result[3]
            target_columns = result[4] if result[4] else []
            
            # Try to match source and target columns
            for src_col_info in source_columns:
                src_col_name = src_col_info.get('columnName', '') if isinstance(src_col_info, dict) else ''
                source_id = f"{source_object}.{src_col_name}"
                
                for tgt_col_info in target_columns:
                    tgt_col_name = tgt_col_info.get('columnName', '') if isinstance(tgt_col_info, dict) else ''
                    target_id = f"{target_object}.{tgt_col_name}"
                    
                    if source_id in node_lookup and target_id in node_lookup:
                        # Analyze transformation type from query
                        transformation_type = self._analyze_transformation_type(query_text, src_col_name, tgt_col_name)
                        
                        edge = LineageEdge(
                            source_id=source_id,
                            target_id=target_id,
                            transformation_type=transformation_type,
                            transformation_logic=query_text[:500] if query_text else None,
                            confidence_score=0.9
                        )
                        edges.append(edge)
        
        return edges
    
    def _analyze_transformation_type(self, query_text: str, source_col: str, target_col: str) -> str:
        """Analyze SQL query to determine transformation type"""
        if not query_text:
            return "unknown"
        
        query_lower = query_text.lower()
        
        if f"{source_col.lower()} = {target_col.lower()}" in query_lower or f"{target_col.lower()} = {source_col.lower()}" in query_lower:
            return "direct"
        elif any(agg in query_lower for agg in ['sum(', 'count(', 'avg(', 'max(', 'min(']):
            return "aggregation"
        elif 'join' in query_lower:
            return "join"
        elif any(func in query_lower for func in ['case when', 'if(', 'coalesce(', 'nvl(']):
            return "calculation"
        else:
            return "transformation"
