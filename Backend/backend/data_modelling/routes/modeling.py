from fastapi import APIRouter
from pydantic import BaseModel

# Import your dimensional modeling agent
from backend.data_modelling.dimensional_modeling_agent import get_dimensional_modeling_agent, build_er_graph


router = APIRouter()

# Import session store
from backend.routes.databricks import session_store

class GenerateModelRequest(BaseModel):
    schema_view: str  # "bronze" or "silver"
    session_id: str

@router.post("/generate")
async def generate_dimensional_model(request: GenerateModelRequest):
    """Generate dimensional model SQL"""
    try:
        if request.session_id not in session_store:
            return {
                "success": False,
                "error": "Session not found"
            }
        
        session_data = session_store[request.session_id]
        state = session_data.get("state")
        
        if not state:
            return {
                "success": False,
                "error": "No state found. Please complete previous steps."
            }
        
        # Get dimensional modeling agent
        modeling_node, _ = get_dimensional_modeling_agent(schema_view=request.schema_view)
        
        # Run the agent
        updated_state = modeling_node(state)
        
        # Store updated state
        session_store[request.session_id]["state"] = updated_state
        
        er_diagram = getattr(updated_state, "modeling_er_diagram", None)
        diagram = build_er_graph(er_diagram) if er_diagram else None
        return {
            "success": True,
            "data": {
                "modeling_sql": updated_state.modeling_sql,
                "er_diagram": er_diagram,
                "diagram": diagram,
                "modeling_schema_view": request.schema_view
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
