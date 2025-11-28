use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use postgres::{Config, NoTls};
use r2d2_postgres::PostgresConnectionManager;
use r2d2::Pool;
use chrono::Local;
use std::str::FromStr;

// --- Pure Rust Implementation ---

pub struct RustDatabase {
    pub pool: Pool<PostgresConnectionManager<NoTls>>,
    pub db_name: String,
}

impl RustDatabase {
    pub fn new(connection_string: &str) -> Result<Self, String> {
        // 1. Parse the connection string into a Config object
        let mut config = Config::from_str(connection_string)
            .map_err(|e| format!("Invalid connection string: {}", e))?;

        // 2. Connect to 'postgres' database to check/create the target database
        let mut maintenance_config = config.clone();
        maintenance_config.dbname("postgres");

        let target_db_name = Local::now().format("%Y%m%d").to_string();

        {
            let mut client = maintenance_config.connect(NoTls)
                .map_err(|e| format!("Failed to connect to maintenance DB: {}", e))?;
            
            let check_query = "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)";
            let exists: bool = client.query_one(check_query, &[&target_db_name])
                .map_err(|e| format!("Failed to check DB existence: {}", e))?
                .get(0);

            if !exists {
                let create_query = format!("CREATE DATABASE \"{}\"", target_db_name);
                client.batch_execute(&create_query)
                    .map_err(|e| format!("Failed to create database '{}': {}", target_db_name, e))?;
            }
        }

        // 3. Connect to the target database using a connection pool
        config.dbname(&target_db_name);
        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = Pool::builder()
            .max_size(10)
            .build(manager)
            .map_err(|e| format!("Failed to create connection pool: {}", e))?;

        // 4. Create the Records table if it doesn't exist
        let mut conn = pool.get()
            .map_err(|e| format!("Failed to get connection from pool: {}", e))?;
        
        let create_table_query = r#"
            CREATE TABLE IF NOT EXISTS records (
                id BIGSERIAL PRIMARY KEY,
                span_id UUID,
                parent_id UUID,
                type INTEGER,
                timestamp TIMESTAMP,
                message TEXT,
                attr JSONB
            );
            CREATE INDEX IF NOT EXISTS idx_records_parent_id ON records(parent_id);
        "#;

        conn.batch_execute(create_table_query)
            .map_err(|e| format!("Failed to create 'records' table: {}", e))?;

        Ok(RustDatabase {
            pool,
            db_name: target_db_name,
        })
    }
}

// --- Python Bindings ---

#[pyclass(name = "Database")]
struct PyDatabase {
    inner: RustDatabase,
}

#[pymethods]
impl PyDatabase {
    #[new]
    fn new(connection_string: &str) -> PyResult<Self> {
        match RustDatabase::new(connection_string) {
            Ok(db) => Ok(PyDatabase { inner: db }),
            Err(e) => Err(PyRuntimeError::new_err(e)),
        }
    }

    #[getter]
    fn db_name(&self) -> String {
        self.inner.db_name.clone()
    }
}

#[pymodule]
fn longtrace(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    Ok(())
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use uuid::Uuid;
    use serde_json::json;

    fn get_connection_string() -> String {
        env::var("DATABASE_URL").unwrap_or_else(|_| "host=localhost user=postgres".to_string())
    }

    #[test]
    fn test_database_creation_and_schema() {
        let conn_str = get_connection_string();
        
        // Use the Rust implementation directly, avoiding PyO3 context
        let db_result = RustDatabase::new(&conn_str);
        
        match db_result {
            Ok(db) => {
                println!("Successfully connected to DB: {}", db.db_name);
                
                let mut conn = db.pool.get().expect("Failed to get connection from pool");
                
                let table_exists: bool = conn.query_one(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'records')", 
                    &[]
                ).unwrap().get(0);
                
                assert!(table_exists, "records table should exist");
                
                let span_id = Uuid::new_v4();
                let parent_id = Uuid::new_v4();
                let type_val = 1;
                let timestamp = Local::now().naive_local();
                let message = "Unit test message";
                let attr = json!({"test": "data"});
                
                let insert_query = "INSERT INTO records (span_id, parent_id, type, timestamp, message, attr) VALUES ($1, $2, $3, $4, $5, $6)";
                let res = conn.execute(insert_query, &[&span_id, &parent_id, &type_val, &timestamp, &message, &attr]);
                
                assert!(res.is_ok(), "Failed to insert record: {:?}", res.err());
            },
            Err(e) => {
                panic!("Failed to create Database object: {}", e);
            }
        }
    }
}
