use chrono::Local;
use deadpool_postgres::{Config, Pool, Runtime};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime as TokioRuntime;
use tokio_postgres::NoTls;

/// Database class that manages PostgreSQL connections.
///
/// The database name is automatically generated from the current date (YYYYMMDD format).
/// If the database exists, it will be used; otherwise, it will be created along with
/// the Records table.
#[pyclass]
pub struct Database {
    #[allow(dead_code)]
    pool: Arc<Pool>,
    db_name: String,
    #[allow(dead_code)]
    runtime: Arc<TokioRuntime>,
}

impl Database {
    /// SQL to create the Records table
    const CREATE_TABLE_SQL: &'static str = r#"
        CREATE TABLE IF NOT EXISTS Records (
            id BIGSERIAL PRIMARY KEY,
            span_id UUID NOT NULL,
            parent_id UUID,
            type INTEGER NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            message TEXT,
            attr JSONB
        );
        CREATE INDEX IF NOT EXISTS idx_records_parent_id ON Records(parent_id);
    "#;

    /// Get the database name from current date
    fn get_db_name() -> String {
        Local::now().format("%Y%m%d").to_string()
    }

    /// Parse the connection string and extract components
    fn parse_connection_string(conn_str: &str) -> PyResult<(String, String, String, u16)> {
        // Expected format: postgresql://user:password@host:port
        // or postgres://user:password@host:port
        let conn_str = conn_str
            .strip_prefix("postgresql://")
            .or_else(|| conn_str.strip_prefix("postgres://"))
            .ok_or_else(|| {
                PyRuntimeError::new_err(
                    "Invalid connection string format. Expected: postgresql://user:password@host:port",
                )
            })?;

        // Split user:password@host:port
        let parts: Vec<&str> = conn_str.split('@').collect();
        if parts.len() != 2 {
            return Err(PyRuntimeError::new_err(
                "Invalid connection string format. Expected: postgresql://user:password@host:port",
            ));
        }

        let auth_parts: Vec<&str> = parts[0].split(':').collect();
        if auth_parts.len() != 2 {
            return Err(PyRuntimeError::new_err(
                "Invalid connection string format. Expected user:password in auth section",
            ));
        }

        let host_parts: Vec<&str> = parts[1].split(':').collect();
        if host_parts.len() != 2 {
            return Err(PyRuntimeError::new_err(
                "Invalid connection string format. Expected host:port",
            ));
        }

        let user = auth_parts[0].to_string();
        let password = auth_parts[1].to_string();
        let host = host_parts[0].to_string();
        let port: u16 = host_parts[1]
            .parse()
            .map_err(|_| PyRuntimeError::new_err("Invalid port number"))?;

        Ok((user, password, host, port))
    }

    /// Create the database if it doesn't exist
    async fn ensure_database_exists(
        user: &str,
        password: &str,
        host: &str,
        port: u16,
        db_name: &str,
    ) -> Result<(), String> {
        // Connect to the default 'postgres' database to create our target database
        let conn_str = format!(
            "host={} port={} user={} password={} dbname=postgres",
            host, port, user, password
        );

        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| format!("Failed to connect to postgres database: {}", e))?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Check if database exists
        let row = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
                &[&db_name],
            )
            .await
            .map_err(|e| format!("Failed to check database existence: {}", e))?;

        let exists: bool = row.get(0);

        if !exists {
            // Validate database name contains only alphanumeric characters for safety
            if !db_name.chars().all(|c| c.is_ascii_alphanumeric()) {
                return Err("Database name contains invalid characters".to_string());
            }
            // Create the database
            // Note: We can't use parameterized queries for database names
            let create_db_sql = format!("CREATE DATABASE \"{}\"", db_name);
            client
                .execute(&create_db_sql, &[])
                .await
                .map_err(|e| format!("Failed to create database: {}", e))?;
        }

        Ok(())
    }

    /// Create the Records table if it doesn't exist
    async fn ensure_table_exists(pool: &Pool) -> Result<(), String> {
        let client = pool
            .get()
            .await
            .map_err(|e| format!("Failed to get connection from pool: {}", e))?;

        client
            .batch_execute(Self::CREATE_TABLE_SQL)
            .await
            .map_err(|e| format!("Failed to create Records table: {}", e))?;

        Ok(())
    }
}

#[pymethods]
impl Database {
    /// Create a new Database instance.
    ///
    /// Args:
    ///     connection_string: PostgreSQL connection string without database name.
    ///                        Format: postgresql://user:password@host:port
    ///
    /// The database name is automatically generated from the current date (YYYYMMDD format).
    /// If the database doesn't exist, it will be created along with the Records table.
    #[new]
    pub fn new(connection_string: &str) -> PyResult<Self> {
        let (user, password, host, port) = Self::parse_connection_string(connection_string)?;
        let db_name = Self::get_db_name();

        // Create tokio runtime
        let runtime = TokioRuntime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;

        // Ensure database exists
        runtime
            .block_on(Self::ensure_database_exists(
                &user, &password, &host, port, &db_name,
            ))
            .map_err(PyRuntimeError::new_err)?;

        // Create connection pool configuration
        let mut cfg = Config::new();
        cfg.host = Some(host);
        cfg.port = Some(port);
        cfg.user = Some(user);
        cfg.password = Some(password);
        cfg.dbname = Some(db_name.clone());

        // Create the connection pool
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create pool: {}", e)))?;

        let pool = Arc::new(pool);

        // Ensure table exists
        runtime
            .block_on(Self::ensure_table_exists(&pool))
            .map_err(PyRuntimeError::new_err)?;

        let runtime = Arc::new(runtime);

        Ok(Database {
            pool,
            db_name,
            runtime,
        })
    }

    /// Get the name of the database being used.
    pub fn get_database_name(&self) -> String {
        self.db_name.clone()
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn longtrace(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_connection_string() {
        let result =
            Database::parse_connection_string("postgresql://user:pass@localhost:5432").unwrap();
        assert_eq!(result.0, "user");
        assert_eq!(result.1, "pass");
        assert_eq!(result.2, "localhost");
        assert_eq!(result.3, 5432);
    }

    #[test]
    fn test_parse_connection_string_postgres_prefix() {
        let result =
            Database::parse_connection_string("postgres://admin:secret@db.example.com:5433")
                .unwrap();
        assert_eq!(result.0, "admin");
        assert_eq!(result.1, "secret");
        assert_eq!(result.2, "db.example.com");
        assert_eq!(result.3, 5433);
    }

    #[test]
    fn test_parse_connection_string_invalid() {
        assert!(Database::parse_connection_string("invalid").is_err());
        assert!(Database::parse_connection_string("http://user:pass@localhost:5432").is_err());
        assert!(Database::parse_connection_string("postgresql://user@localhost:5432").is_err());
        assert!(Database::parse_connection_string("postgresql://user:pass@localhost").is_err());
    }

    #[test]
    fn test_get_db_name() {
        let db_name = Database::get_db_name();
        // Should be 8 characters (YYYYMMDD)
        assert_eq!(db_name.len(), 8);
        // Should be all digits
        assert!(db_name.chars().all(|c| c.is_ascii_digit()));
    }
}
