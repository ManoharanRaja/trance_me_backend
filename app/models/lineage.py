from pydantic import BaseModel
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum


class DatabaseType(str, Enum):
    SNOWFLAKE = "snowflake"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    BIGQUERY = "bigquery"
    SQLSERVER = "sqlserver"


class ColumnInfo(BaseModel):
    column_name: str
    data_type: str
    is_nullable: bool
    default_value: Optional[str] = None
    comment: Optional[str] = None


class TableInfo(BaseModel):
    database_name: str
    schema_name: str
    table_name: str
    table_type: str  # TABLE, VIEW, MATERIALIZED_VIEW
    columns: List[ColumnInfo]
    comment: Optional[str] = None
    created_date: Optional[datetime] = None
    last_modified: Optional[datetime] = None


class LineageNode(BaseModel):
    id: str  # Unique identifier (database.schema.table.column)
    database_name: str
    schema_name: str
    table_name: str
    column_name: str
    data_type: str
    node_type: str  # source, transformation, target
    table_type: str  # TABLE, VIEW, MATERIALIZED_VIEW


class LineageEdge(BaseModel):
    source_id: str
    target_id: str
    transformation_type: str  # direct, calculation, aggregation, join
    transformation_logic: Optional[str] = None
    query_id: Optional[str] = None
    confidence_score: float = 1.0


class LineageGraph(BaseModel):
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    metadata: Dict[str, Any] = {}


class DatabaseConnection(BaseModel):
    database_type: DatabaseType
    connection_params: Dict[str, Any]
    name: str
    description: Optional[str] = None


class LineageRequest(BaseModel):
    database_type: DatabaseType
    connection_params: Dict[str, Any]
    target_table: str
    target_schema: str
    target_database: str
    target_column: Optional[str] = None
    max_depth: int = 5
    include_downstream: bool = True
    include_upstream: bool = True


class LineageResponse(BaseModel):
    lineage_graph: LineageGraph
    summary: Dict[str, Any]
    execution_time: float
    timestamp: datetime
