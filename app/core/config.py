from dotenv import load_dotenv
import os

load_dotenv()


class Settings:
    # Database configurations
    SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
    SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
    SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
    SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "")
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "")
    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "")
    
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
    POSTGRES_USER = os.getenv("POSTGRES_USER", "")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
    POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "")
    
    MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
    MYSQL_USER = os.getenv("MYSQL_USER", "")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "")
    
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "")
    BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "")
    
    SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "localhost")
    SQLSERVER_PORT = int(os.getenv("SQLSERVER_PORT", 1433))
    SQLSERVER_USER = os.getenv("SQLSERVER_USER", "")
    SQLSERVER_PASSWORD = os.getenv("SQLSERVER_PASSWORD", "")
    SQLSERVER_DATABASE = os.getenv("SQLSERVER_DATABASE", "")
    
    # Application settings
    DEBUG = os.getenv("DEBUG", "True").lower() == "true"
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", 8000))


settings = Settings()
