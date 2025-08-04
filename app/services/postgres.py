import psycopg2
from typing import List, Dict, Any, Optional
import pandas as pd
from app.core.base_extractor import BaseLineageExtractor
from app.models.lineage import LineageNode, LineageEdge, TableInfo, ColumnInfo
from datetime import datetime


class PostgreSQLLineageExtractor(BaseLineageExtractor):
    """PostgreSQL-specific lineage extractor using pg_catalog and information_schema"""
    
    async def connect(self) -> bool:
        """Establish connection to PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                host=self.connection_params.get('host'),
                port=self.connection_params.get('port', 5432),
                database=self.connection_params.get('database'),
                user=self.connection_params.get('user'),
                password=self.connection_params.get('password')
            )
            return True
        except Exception as e:
            print(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    async def disconnect(self):
        """Close PostgreSQL connection"""
        if self.connection:
            self.connection.close()
    
    async def get_table_metadata(self, database: str, schema: str, table: str) -> TableInfo:
        """Get metadata for a specific PostgreSQL table"""
        
        # Query to get table information
        table_query = """
        SELECT 
            t.table_name,
            t.table_type,
            obj_description(c.oid) as comment
        FROM information_schema.tables t
        LEFT JOIN pg_class c ON c.relname = t.table_name
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE t.table_schema = %s 
        AND t.table_name = %s
        AND n.nspname = %s
        """
        
        # Query to get column information
        columns_query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            col_description(pgc.oid, c.ordinal_position) as comment
        FROM information_schema.columns c
        LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
        LEFT JOIN pg_namespace n ON n.oid = pgc.relnamespace
        WHERE c.table_schema = %s 
        AND c.table_name = %s
        AND n.nspname = %s
        ORDER BY c.ordinal_position
        """
        
        cursor = self.connection.cursor()
        
        # Get table info
        cursor.execute(table_query, (schema, table, schema))
        table_result = cursor.fetchone()
        
        if not table_result:
            raise ValueError(f"Table {schema}.{table} not found")
        
        # Get columns info
        cursor.execute(columns_query, (schema, table, schema))
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
            created_date=None,  # PostgreSQL doesn't store creation date by default
            last_modified=None
        )
    
    async def get_upstream_lineage(self, database: str, schema: str, table: str, 
                                 column: Optional[str] = None, max_depth: int = 5) -> List[LineageNode]:
        """Get upstream dependencies using PostgreSQL system catalogs"""
        
        nodes = []
        
        # Query to find view dependencies
        view_deps_query = """
        WITH RECURSIVE view_deps AS (
            -- Base case: find direct view dependencies
            SELECT DISTINCT
                d.refobjid::regclass::text as source_table,
                d.objid::regclass::text as dependent_view,
                1 as depth
            FROM pg_depend d
            JOIN pg_class c ON c.oid = d.objid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE d.objid::regclass::text = %s
            AND d.deptype = 'n'  -- normal dependency
            AND c.relkind IN ('v', 'm')  -- views and materialized views
            
            UNION ALL
            
            -- Recursive case: find dependencies of dependencies
            SELECT DISTINCT
                d.refobjid::regclass::text as source_table,
                d.objid::regclass::text as dependent_view,
                vd.depth + 1
            FROM pg_depend d
            JOIN pg_class c ON c.oid = d.objid
            JOIN view_deps vd ON vd.dependent_view = d.refobjid::regclass::text
            WHERE vd.depth < %s
            AND d.deptype = 'n'
            AND c.relkind IN ('v', 'm')
        )
        SELECT DISTINCT source_table, depth
        FROM view_deps
        WHERE source_table != %s
        ORDER BY depth
        """
        
        cursor = self.connection.cursor()
        full_table_name = f"{schema}.{table}"
        cursor.execute(view_deps_query, (full_table_name, max_depth, full_table_name))
        results = cursor.fetchall()
        
        for result in results:
            source_table_full = result[0]
            
            # Parse schema and table name
            if '.' in source_table_full:
                src_schema, src_table = source_table_full.split('.', 1)
                src_schema = src_schema.strip('"')
                src_table = src_table.strip('"')
            else:
                src_schema = 'public'  # default schema
                src_table = source_table_full.strip('"')
            
            try:
                source_table_info = await self.get_table_metadata(database, src_schema, src_table)
                
                for col in source_table_info.columns:
                    node = LineageNode(
                        id=f"{database}.{src_schema}.{src_table}.{col.column_name}",
                        database_name=database,
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
        """Get downstream dependencies using PostgreSQL system catalogs"""
        
        nodes = []
        
        # Query to find what depends on this table
        downstream_query = """
        WITH RECURSIVE downstream_deps AS (
            -- Base case: find direct dependencies
            SELECT DISTINCT
                d.objid::regclass::text as dependent_object,
                d.refobjid::regclass::text as source_table,
                1 as depth
            FROM pg_depend d
            JOIN pg_class c ON c.oid = d.objid
            WHERE d.refobjid::regclass::text = %s
            AND d.deptype = 'n'
            AND c.relkind IN ('r', 'v', 'm')  -- tables, views, materialized views
            
            UNION ALL
            
            -- Recursive case: find dependencies of dependencies
            SELECT DISTINCT
                d.objid::regclass::text as dependent_object,
                d.refobjid::regclass::text as source_table,
                dd.depth + 1
            FROM pg_depend d
            JOIN pg_class c ON c.oid = d.objid
            JOIN downstream_deps dd ON dd.dependent_object = d.refobjid::regclass::text
            WHERE dd.depth < %s
            AND d.deptype = 'n'
            AND c.relkind IN ('r', 'v', 'm')
        )
        SELECT DISTINCT dependent_object, depth
        FROM downstream_deps
        WHERE dependent_object != %s
        ORDER BY depth
        """
        
        cursor = self.connection.cursor()
        full_table_name = f"{schema}.{table}"
        cursor.execute(downstream_query, (full_table_name, max_depth, full_table_name))
        results = cursor.fetchall()
        
        for result in results:
            dependent_table_full = result[0]
            
            # Parse schema and table name
            if '.' in dependent_table_full:
                dep_schema, dep_table = dependent_table_full.split('.', 1)
                dep_schema = dep_schema.strip('"')
                dep_table = dep_table.strip('"')
            else:
                dep_schema = 'public'
                dep_table = dependent_table_full.strip('"')
            
            try:
                dependent_table_info = await self.get_table_metadata(database, dep_schema, dep_table)
                
                for col in dependent_table_info.columns:
                    node = LineageNode(
                        id=f"{database}.{dep_schema}.{dep_table}.{col.column_name}",
                        database_name=database,
                        schema_name=dep_schema,
                        table_name=dep_table,
                        column_name=col.column_name,
                        data_type=col.data_type,
                        node_type="transformation",
                        table_type=dependent_table_info.table_type
                    )
                    nodes.append(node)
            
            except Exception as e:
                print(f"Error getting metadata for {dependent_table_full}: {e}")
                continue
        
        return nodes
    
    async def get_lineage_edges(self, source_nodes: List[LineageNode], 
                              target_nodes: List[LineageNode]) -> List[LineageEdge]:
        """Get relationships between nodes using view definitions"""
        
        edges = []
        
        # Query to get view definitions for analysis
        view_def_query = """
        SELECT 
            schemaname,
            viewname,
            definition
        FROM pg_views
        WHERE schemaname = %s
        UNION ALL
        SELECT 
            schemaname,
            matviewname as viewname,
            definition
        FROM pg_matviews
        WHERE schemaname = %s
        """
        
        cursor = self.connection.cursor()
        
        # Group nodes by table for easier processing
        table_nodes = {}
        for node in source_nodes + target_nodes:
            table_key = f"{node.schema_name}.{node.table_name}"
            if table_key not in table_nodes:
                table_nodes[table_key] = []
            table_nodes[table_key].append(node)
        
        # Get all view definitions
        unique_schemas = set(node.schema_name for node in source_nodes + target_nodes)
        for schema in unique_schemas:
            cursor.execute(view_def_query, (schema, schema))
            view_results = cursor.fetchall()
            
            for view_result in view_results:
                view_schema = view_result[0]
                view_name = view_result[1]
                view_definition = view_result[2].lower()
                
                view_key = f"{view_schema}.{view_name}"
                if view_key in table_nodes:
                    # Analyze view definition to find column mappings
                    view_nodes = table_nodes[view_key]
                    
                    for view_node in view_nodes:
                        # Look for source tables and columns in the view definition
                        for source_table_key, source_nodes_list in table_nodes.items():
                            if source_table_key != view_key:
                                source_schema, source_table = source_table_key.split('.')
                                
                                # Check if source table is referenced in view
                                if source_table.lower() in view_definition:
                                    for source_node in source_nodes_list:
                                        # Simple heuristic: if column name appears in view definition
                                        if source_node.column_name.lower() in view_definition:
                                            transformation_type = self._analyze_transformation_type(
                                                view_definition, source_node.column_name, view_node.column_name
                                            )
                                            
                                            edge = LineageEdge(
                                                source_id=source_node.id,
                                                target_id=view_node.id,
                                                transformation_type=transformation_type,
                                                transformation_logic=view_definition[:500],
                                                confidence_score=0.8  # Lower confidence for heuristic matching
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
        elif any(func in definition_lower for func in ['case when', 'coalesce(', 'nullif(']):
            return "calculation"
        else:
            return "transformation"
