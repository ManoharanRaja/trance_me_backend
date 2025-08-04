from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd
import networkx as nx
from app.models.lineage import LineageGraph, LineageNode, LineageEdge, TableInfo


class BaseLineageExtractor(ABC):
    """Abstract base class for database-specific lineage extractors"""
    
    def __init__(self, connection_params: Dict[str, Any]):
        self.connection_params = connection_params
        self.connection = None
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to the database"""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """Close database connection"""
        pass
    
    @abstractmethod
    async def get_table_metadata(self, database: str, schema: str, table: str) -> TableInfo:
        """Get metadata for a specific table"""
        pass
    
    @abstractmethod
    async def get_upstream_lineage(self, database: str, schema: str, table: str, 
                                 column: Optional[str] = None, max_depth: int = 5) -> List[LineageNode]:
        """Get upstream dependencies for a table/column"""
        pass
    
    @abstractmethod
    async def get_downstream_lineage(self, database: str, schema: str, table: str,
                                   column: Optional[str] = None, max_depth: int = 5) -> List[LineageNode]:
        """Get downstream dependencies for a table/column"""
        pass
    
    @abstractmethod
    async def get_lineage_edges(self, source_nodes: List[LineageNode], 
                              target_nodes: List[LineageNode]) -> List[LineageEdge]:
        """Get relationships between nodes"""
        pass
    
    async def build_lineage_graph(self, database: str, schema: str, table: str,
                                column: Optional[str] = None, max_depth: int = 5,
                                include_upstream: bool = True, 
                                include_downstream: bool = True) -> LineageGraph:
        """Build complete lineage graph for a table/column"""
        
        nodes = []
        edges = []
        
        # Get target table info
        target_table = await self.get_table_metadata(database, schema, table)
        
        # Create target nodes
        if column:
            target_column = next((col for col in target_table.columns if col.column_name == column), None)
            if target_column:
                target_node = LineageNode(
                    id=f"{database}.{schema}.{table}.{column}",
                    database_name=database,
                    schema_name=schema,
                    table_name=table,
                    column_name=column,
                    data_type=target_column.data_type,
                    node_type="target",
                    table_type=target_table.table_type
                )
                nodes.append(target_node)
        else:
            for col in target_table.columns:
                target_node = LineageNode(
                    id=f"{database}.{schema}.{table}.{col.column_name}",
                    database_name=database,
                    schema_name=schema,
                    table_name=table,
                    column_name=col.column_name,
                    data_type=col.data_type,
                    node_type="target",
                    table_type=target_table.table_type
                )
                nodes.append(target_node)
        
        # Get upstream lineage
        if include_upstream:
            upstream_nodes = await self.get_upstream_lineage(database, schema, table, column, max_depth)
            nodes.extend(upstream_nodes)
        
        # Get downstream lineage
        if include_downstream:
            downstream_nodes = await self.get_downstream_lineage(database, schema, table, column, max_depth)
            nodes.extend(downstream_nodes)
        
        # Get edges between all nodes
        edges = await self.get_lineage_edges(nodes, nodes)
        
        return LineageGraph(nodes=nodes, edges=edges)
    
    def create_network_graph(self, lineage_graph: LineageGraph) -> nx.DiGraph:
        """Convert lineage graph to NetworkX graph for analysis"""
        
        G = nx.DiGraph()
        
        # Add nodes
        for node in lineage_graph.nodes:
            G.add_node(node.id, **node.dict())
        
        # Add edges
        for edge in lineage_graph.edges:
            G.add_edge(edge.source_id, edge.target_id, **edge.dict())
        
        return G
    
    def calculate_impact_analysis(self, lineage_graph: LineageGraph, 
                                target_node_id: str) -> Dict[str, Any]:
        """Calculate impact analysis for a specific node"""
        
        G = self.create_network_graph(lineage_graph)
        
        # Find all downstream nodes
        downstream_nodes = list(nx.descendants(G, target_node_id))
        
        # Find all upstream nodes
        upstream_nodes = list(nx.ancestors(G, target_node_id))
        
        # Calculate centrality measures
        try:
            betweenness = nx.betweenness_centrality(G)
            pagerank = nx.pagerank(G)
        except:
            betweenness = {}
            pagerank = {}
        
        return {
            "target_node": target_node_id,
            "downstream_count": len(downstream_nodes),
            "upstream_count": len(upstream_nodes),
            "downstream_nodes": downstream_nodes,
            "upstream_nodes": upstream_nodes,
            "betweenness_centrality": betweenness.get(target_node_id, 0),
            "pagerank_score": pagerank.get(target_node_id, 0)
        }
