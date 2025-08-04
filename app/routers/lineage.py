from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
import time
from datetime import datetime

from app.models.lineage import (
    LineageRequest, LineageResponse, DatabaseType, 
    LineageGraph, DatabaseConnection
)
from app.services.snowflake import SnowflakeLineageExtractor
from app.services.postgres import PostgreSQLLineageExtractor
from app.services.mysql import MySQLLineageExtractor
from app.services.bigquery import BigQueryLineageExtractor
from app.services.sqlserver import SQLServerLineageExtractor

router = APIRouter(prefix="/lineage", tags=["lineage"])


def get_lineage_extractor(database_type: DatabaseType, connection_params: Dict[str, Any]):
    """Factory function to get the appropriate lineage extractor"""
    extractors = {
        DatabaseType.SNOWFLAKE: SnowflakeLineageExtractor,
        DatabaseType.POSTGRESQL: PostgreSQLLineageExtractor,
        DatabaseType.MYSQL: MySQLLineageExtractor,
        DatabaseType.BIGQUERY: BigQueryLineageExtractor,
        DatabaseType.SQLSERVER: SQLServerLineageExtractor,
    }
    
    extractor_class = extractors.get(database_type)
    if not extractor_class:
        raise HTTPException(
            status_code=400, 
            detail=f"Unsupported database type: {database_type}"
        )
    
    return extractor_class(connection_params)


@router.post("/extract", response_model=LineageResponse)
async def extract_lineage(request: LineageRequest):
    """Extract column lineage for a specific table/column"""
    
    start_time = time.time()
    
    try:
        # Get the appropriate extractor
        extractor = get_lineage_extractor(request.database_type, request.connection_params)
        
        # Connect to database
        connected = await extractor.connect()
        if not connected:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to connect to {request.database_type} database"
            )
        
        try:
            # Build lineage graph
            lineage_graph = await extractor.build_lineage_graph(
                database=request.target_database,
                schema=request.target_schema,
                table=request.target_table,
                column=request.target_column,
                max_depth=request.max_depth,
                include_upstream=request.include_upstream,
                include_downstream=request.include_downstream
            )
            
            # Calculate summary statistics
            summary = {
                "total_nodes": len(lineage_graph.nodes),
                "total_edges": len(lineage_graph.edges),
                "source_nodes": len([n for n in lineage_graph.nodes if n.node_type == "source"]),
                "transformation_nodes": len([n for n in lineage_graph.nodes if n.node_type == "transformation"]),
                "target_nodes": len([n for n in lineage_graph.nodes if n.node_type == "target"]),
                "database_type": request.database_type,
                "target_table": f"{request.target_database}.{request.target_schema}.{request.target_table}",
                "target_column": request.target_column,
                "max_depth": request.max_depth
            }
            
            # Calculate impact analysis if there are target nodes
            if lineage_graph.nodes:
                target_node_id = None
                if request.target_column:
                    target_node_id = f"{request.target_database}.{request.target_schema}.{request.target_table}.{request.target_column}"
                else:
                    # Use first target node if no specific column
                    target_nodes = [n for n in lineage_graph.nodes if n.node_type == "target"]
                    if target_nodes:
                        target_node_id = target_nodes[0].id
                
                if target_node_id:
                    impact_analysis = extractor.calculate_impact_analysis(lineage_graph, target_node_id)
                    summary["impact_analysis"] = impact_analysis
            
            execution_time = time.time() - start_time
            
            return LineageResponse(
                lineage_graph=lineage_graph,
                summary=summary,
                execution_time=execution_time,
                timestamp=datetime.now()
            )
        
        finally:
            # Always disconnect
            await extractor.disconnect()
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error extracting lineage: {str(e)}"
        )


@router.post("/test-connection")
async def test_connection(connection: DatabaseConnection):
    """Test database connection"""
    
    try:
        extractor = get_lineage_extractor(connection.database_type, connection.connection_params)
        connected = await extractor.connect()
        
        if connected:
            await extractor.disconnect()
            return {
                "success": True,
                "message": f"Successfully connected to {connection.database_type} database",
                "database_type": connection.database_type,
                "connection_name": connection.name
            }
        else:
            return {
                "success": False,
                "message": f"Failed to connect to {connection.database_type} database",
                "database_type": connection.database_type,
                "connection_name": connection.name
            }
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error testing connection: {str(e)}"
        )


@router.get("/supported-databases")
async def get_supported_databases():
    """Get list of supported database types"""
    
    return {
        "supported_databases": [
            {
                "type": DatabaseType.SNOWFLAKE,
                "name": "Snowflake",
                "description": "Uses INFORMATION_SCHEMA, ACCESS_HISTORY, and OBJECT_DEPENDENCIES",
                "required_params": ["account", "user", "password", "warehouse", "database", "schema"]
            },
            {
                "type": DatabaseType.POSTGRESQL,
                "name": "PostgreSQL",
                "description": "Uses pg_catalog, information_schema, and query logs",
                "required_params": ["host", "port", "database", "user", "password"]
            },
            {
                "type": DatabaseType.MYSQL,
                "name": "MySQL",
                "description": "Uses information_schema and general logs",
                "required_params": ["host", "port", "database", "user", "password"]
            },
            {
                "type": DatabaseType.BIGQUERY,
                "name": "Google BigQuery",
                "description": "Uses Metadata APIs, INFORMATION_SCHEMA, and audit logs",
                "required_params": ["project_id", "dataset_id", "credentials_path"]
            },
            {
                "type": DatabaseType.SQLSERVER,
                "name": "Microsoft SQL Server",
                "description": "Uses sys.objects, sys.columns, and DMV views",
                "required_params": ["host", "port", "database", "user", "password"]
            }
        ]
    }


@router.get("/graph-analysis/{database_type}")
async def analyze_lineage_graph(
    database_type: DatabaseType,
    connection_params: Dict[str, Any],
    target_database: str,
    target_schema: str,
    target_table: str,
    target_column: str = None
):
    """Perform advanced graph analysis on lineage data"""
    
    try:
        extractor = get_lineage_extractor(database_type, connection_params)
        connected = await extractor.connect()
        
        if not connected:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to connect to {database_type} database"
            )
        
        try:
            # Build lineage graph
            lineage_graph = await extractor.build_lineage_graph(
                database=target_database,
                schema=target_schema,
                table=target_table,
                column=target_column,
                max_depth=10,
                include_upstream=True,
                include_downstream=True
            )
            
            # Create NetworkX graph for analysis
            network_graph = extractor.create_network_graph(lineage_graph)
            
            # Perform various graph analyses
            analysis_results = {
                "graph_metrics": {
                    "total_nodes": network_graph.number_of_nodes(),
                    "total_edges": network_graph.number_of_edges(),
                    "is_connected": len(list(network_graph.connected_components())) if not network_graph.is_directed() else "N/A (directed graph)",
                    "density": network_graph.density() if network_graph.number_of_nodes() > 1 else 0,
                },
                "centrality_measures": {},
                "node_classifications": {
                    "source_tables": [],
                    "intermediate_transforms": [],
                    "target_tables": []
                }
            }
            
            # Calculate centrality measures if graph has nodes
            if network_graph.number_of_nodes() > 0:
                try:
                    import networkx as nx
                    analysis_results["centrality_measures"] = {
                        "betweenness_centrality": dict(nx.betweenness_centrality(network_graph)),
                        "pagerank": dict(nx.pagerank(network_graph)),
                        "in_degree_centrality": dict(nx.in_degree_centrality(network_graph)),
                        "out_degree_centrality": dict(nx.out_degree_centrality(network_graph))
                    }
                except Exception as e:
                    analysis_results["centrality_measures"]["error"] = f"Could not calculate centrality: {str(e)}"
            
            # Classify nodes
            for node in lineage_graph.nodes:
                if node.node_type == "source":
                    analysis_results["node_classifications"]["source_tables"].append({
                        "id": node.id,
                        "table": f"{node.database_name}.{node.schema_name}.{node.table_name}",
                        "column": node.column_name
                    })
                elif node.node_type == "transformation":
                    analysis_results["node_classifications"]["intermediate_transforms"].append({
                        "id": node.id,
                        "table": f"{node.database_name}.{node.schema_name}.{node.table_name}",
                        "column": node.column_name
                    })
                else:  # target
                    analysis_results["node_classifications"]["target_tables"].append({
                        "id": node.id,
                        "table": f"{node.database_name}.{node.schema_name}.{node.table_name}",
                        "column": node.column_name
                    })
            
            return analysis_results
        
        finally:
            await extractor.disconnect()
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error analyzing lineage graph: {str(e)}"
        )
