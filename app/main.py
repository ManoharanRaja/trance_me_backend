from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from app.routers import lineage
from app.core.config import settings

# Create FastAPI application
app = FastAPI(
    title="Multi-Database Column Lineage Viewer",
    description="""
    A comprehensive tool for visualizing column-level data lineage across multiple database platforms.
    
    ## Supported Databases
    
    * **Snowflake** - Uses INFORMATION_SCHEMA, ACCESS_HISTORY, and OBJECT_DEPENDENCIES
    * **PostgreSQL** - Uses pg_catalog, information_schema, and query logs  
    * **MySQL** - Uses information_schema and general logs
    * **BigQuery** - Uses Metadata APIs, INFORMATION_SCHEMA, and audit logs
    * **SQL Server** - Uses sys.objects, sys.columns, and DMV views
    
    ## Features
    
    * Extract column-level lineage with configurable depth
    * Network graph analysis and visualization
    * Impact analysis and dependency tracking
    * Support for upstream and downstream lineage
    * Cross-database lineage mapping
    * Real-time connection testing
    
    ## API Endpoints
    
    * `/lineage/extract` - Extract lineage for a specific table/column
    * `/lineage/test-connection` - Test database connectivity
    * `/lineage/supported-databases` - Get list of supported databases
    * `/lineage/graph-analysis` - Perform advanced graph analysis
    """,
    version="1.0.0",
    contact={
        "name": "Trance Me Backend",
        "url": "https://github.com/ManoharanRaja/trance_me_backend",
    },
    license_info={
        "name": "MIT",
    }
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(lineage.router, prefix="/api/v1")

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Multi-Database Column Lineage Viewer API",
        "version": "1.0.0",
        "status": "active",
        "supported_databases": [
            "Snowflake",
            "PostgreSQL", 
            "MySQL",
            "Google BigQuery",
            "Microsoft SQL Server"
        ],
        "documentation": "/docs",
        "health_check": "/health"
    }

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": "2025-01-01T00:00:00Z",
        "version": "1.0.0"
    }

# Exception handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={
            "error": "Not Found", 
            "message": f"The requested resource was not found",
            "path": str(request.url.path)
        }
    )

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": "An unexpected error occurred",
            "path": str(request.url.path)
        }
    )

# Main execution
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG
    )
