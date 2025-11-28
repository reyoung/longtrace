use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use postgres::{Config, NoTls};
use r2d2_postgres::PostgresConnectionManager;
use r2d2::Pool;
use chrono::Local;
use std::str::FromStr;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use uuid::Uuid;

// --- Record Structure ---

#[derive(Debug, Clone)]
pub struct Record {
    pub span_id: Uuid,
    pub parent_id: Uuid,
    pub record_type: i32,
    pub timestamp: chrono::NaiveDateTime,
    pub message: String,
    pub attr: String, // JSON string
}

// --- Pure Rust Implementation ---

pub struct RustDatabase {
    pub pool: Pool<PostgresConnectionManager<NoTls>>,
    pub db_name: String,
    sender: Sender<BatchCommand>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

enum BatchCommand {
    Record(Record),
    Flush,
    Shutdown,
}

impl RustDatabase {
    pub fn new(connection_string: &str, batch_size: Option<usize>) -> Result<Self, String> {
        let batch_size = batch_size.unwrap_or(1024);
        
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

        // 5. Start the batch writer thread
        let (sender, receiver) = channel::<BatchCommand>();
        let pool_clone = pool.clone();
        let batch_size_clone = batch_size;

        let thread_handle = thread::spawn(move || {
            let mut batch: Vec<Record> = Vec::with_capacity(batch_size_clone);
            
            loop {
                match receiver.recv() {
                    Ok(BatchCommand::Record(record)) => {
                        batch.push(record);
                        if batch.len() >= batch_size_clone {
                            Self::flush_batch(&pool_clone, &mut batch);
                        }
                    }
                    Ok(BatchCommand::Flush) => {
                        if !batch.is_empty() {
                            Self::flush_batch(&pool_clone, &mut batch);
                        }
                    }
                    Ok(BatchCommand::Shutdown) => {
                        if !batch.is_empty() {
                            Self::flush_batch(&pool_clone, &mut batch);
                        }
                        break;
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(RustDatabase {
            pool,
            db_name: target_db_name,
            sender,
            thread_handle: Some(thread_handle),
        })
    }

    fn flush_batch(pool: &Pool<PostgresConnectionManager<NoTls>>, batch: &mut Vec<Record>) {
        if batch.is_empty() {
            return;
        }

        match pool.get() {
            Ok(mut conn) => {
                let insert_query = "INSERT INTO records (span_id, parent_id, type, timestamp, message, attr) VALUES ($1, $2, $3, $4, $5, $6::jsonb)";
                
                for record in batch.iter() {
                    // Parse the JSON string into a Value
                    let attr_value: serde_json::Value = match serde_json::from_str(&record.attr) {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("Failed to parse JSON attr: {}", e);
                            continue;
                        }
                    };
                    
                    if let Err(e) = conn.execute(
                        insert_query,
                        &[
                            &record.span_id,
                            &record.parent_id,
                            &record.record_type,
                            &record.timestamp,
                            &record.message,
                            &attr_value,
                        ],
                    ) {
                        eprintln!("Failed to insert record: {}", e);
                    }
                }
                
                batch.clear();
            }
            Err(e) => {
                eprintln!("Failed to get connection from pool: {}", e);
            }
        }
    }

    pub fn report(&self, message: String, span_id: Uuid, parent_id: Uuid, attr: String) -> Result<(), String> {
        let record = Record {
            span_id,
            parent_id,
            record_type: 0, // Default to log type
            timestamp: Local::now().naive_local(),
            message,
            attr,
        };

        self.sender
            .send(BatchCommand::Record(record))
            .map_err(|e| format!("Failed to send record: {}", e))
    }

    pub fn flush(&self) -> Result<(), String> {
        self.sender
            .send(BatchCommand::Flush)
            .map_err(|e| format!("Failed to send flush command: {}", e))
    }
}

impl Drop for RustDatabase {
    fn drop(&mut self) {
        // Send shutdown command
        let _ = self.sender.send(BatchCommand::Shutdown);
        
        // Wait for the thread to finish
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

// --- Python Bindings ---

#[pyclass(name = "Database")]
struct PyDatabase {
    inner: Arc<Mutex<RustDatabase>>,
}

#[pymethods]
impl PyDatabase {
    #[new]
    #[pyo3(signature = (connection_string, batch_size=None))]
    fn new(connection_string: &str, batch_size: Option<usize>) -> PyResult<Self> {
        match RustDatabase::new(connection_string, batch_size) {
            Ok(db) => Ok(PyDatabase { inner: Arc::new(Mutex::new(db)) }),
            Err(e) => Err(PyRuntimeError::new_err(e)),
        }
    }

    #[getter]
    fn db_name(&self) -> PyResult<String> {
        let db = self.inner.lock().map_err(|e| PyRuntimeError::new_err(format!("Lock error: {}", e)))?;
        Ok(db.db_name.clone())
    }

    fn report(&self, message: String, span_id: String, parent_id: String, attr: String) -> PyResult<()> {
        let span_uuid = Uuid::parse_str(&span_id)
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid span_id UUID: {}", e)))?;
        let parent_uuid = Uuid::parse_str(&parent_id)
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid parent_id UUID: {}", e)))?;

        let db = self.inner.lock().map_err(|e| PyRuntimeError::new_err(format!("Lock error: {}", e)))?;
        db.report(message, span_uuid, parent_uuid, attr)
            .map_err(|e| PyRuntimeError::new_err(e))
    }

    fn flush(&self) -> PyResult<()> {
        let db = self.inner.lock().map_err(|e| PyRuntimeError::new_err(format!("Lock error: {}", e)))?;
        db.flush().map_err(|e| PyRuntimeError::new_err(e))
    }
}

#[pymodule]
fn longtrace(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    Ok(())
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use serde_json::json;

    fn get_connection_string() -> String {
        env::var("DATABASE_URL").unwrap_or_else(|_| "host=localhost user=postgres".to_string())
    }

    #[test]
    fn test_database_creation_and_schema() {
        let conn_str = get_connection_string();
        
        // Use the Rust implementation directly, avoiding PyO3 context
        let db_result = RustDatabase::new(&conn_str, None);
        
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

    #[test]
    fn test_batch_reporting() {
        let conn_str = get_connection_string();
        let db = RustDatabase::new(&conn_str, Some(5)).expect("Failed to create database");
        
        let test_span_id = Uuid::now_v7();
        let test_parent_id = Uuid::now_v7();
        let test_id = Uuid::now_v7().to_string(); // Unique test ID for this run
        
        // Report some records
        for i in 0..10 {
            let message = format!("Test message {}", i);
            let attr = json!({"index": i, "test_id": &test_id}).to_string();
            
            db.report(message, test_span_id, test_parent_id, attr.clone()).expect("Failed to report");
        }
        
        // Flush to ensure all records are written
        db.flush().expect("Failed to flush");
        
        // Give some time for the batch to be written
        std::thread::sleep(std::time::Duration::from_millis(200));
        
        // Verify records in database
        let mut conn = db.pool.get().expect("Failed to get connection from pool");
        
        let count: i64 = conn.query_one(
            "SELECT COUNT(*) FROM records WHERE attr->>'test_id' = $1",
            &[&test_id]
        ).expect("Failed to query count").get(0);
        
        assert_eq!(count, 10, "Expected 10 records to be inserted, found {}", count);
        
        // Verify one of the records has correct data
        let row = conn.query_one(
            "SELECT message, span_id, parent_id, attr FROM records WHERE attr->>'test_id' = $1 AND attr->>'index' = '0'",
            &[&test_id]
        ).expect("Failed to query record");
        
        let message: String = row.get(0);
        let span_id: Uuid = row.get(1);
        let parent_id: Uuid = row.get(2);
        let attr: serde_json::Value = row.get(3);
        
        assert_eq!(message, "Test message 0");
        assert_eq!(span_id, test_span_id);
        assert_eq!(parent_id, test_parent_id);
        assert_eq!(attr["index"], 0);
        assert_eq!(attr["test_id"], test_id);
        
        println!("Batch reporting test completed successfully with {} records verified", count);
    }
}
