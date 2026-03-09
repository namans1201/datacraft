from pydantic import BaseModel, Field
from typing import Optional, List

class ModelingRequest(BaseModel):
    catalog: str
    schema_name: str
    schema_view: str  

class ERTableColumn(BaseModel):
    name: str
    data_type: Optional[str] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False

class ERTable(BaseModel):
    name: str
    columns: List[ERTableColumn] = Field(default_factory=list)

class ERRelationship(BaseModel):
    from_table: str
    from_column: str
    to_table: str
    to_column: str

class ERDiagram(BaseModel):
    tables: List[ERTable] = Field(default_factory=list)
    relationships: List[ERRelationship] = Field(default_factory=list)

class DiagramNode(BaseModel):
    table_name: str
    columns: List[ERTableColumn] = Field(default_factory=list)
    table_type: str  # dimension | fact | table

class DiagramEdge(BaseModel):
    from_table: str
    from_column: str
    to_table: str
    to_column: str

class ERDiagramGraph(BaseModel):
    nodes: List[DiagramNode] = Field(default_factory=list)
    edges: List[DiagramEdge] = Field(default_factory=list)

class ModelingResponse(BaseModel):
    success: bool
    modeling_sql: Optional[str] = None
    er_diagram: Optional[ERDiagram] = None
    diagram: Optional[ERDiagramGraph] = None
    message: Optional[str] = None
