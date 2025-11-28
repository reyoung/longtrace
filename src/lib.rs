use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use postgres::{Config, NoTls};
use r2d2_postgres::PostgresConnectionManager;
use r2d2::Pool;
use chrono::Local;
use std::str::FromStr;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, ThreadId};
use uuid::Uuid;
use dashmap::DashMap;

// --- Record Structure ---

#[derive(Debug, Clone)]
pub struct Record {
    pub span_id: Uuid,
    pub parent_id: Uuid,
    pub record_type: i32,
    pub timestamp: chrono::NaiveDateTime,
    pub message: String,
    pub attr: Option<String>, // JSON string
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
    pub fn new(connection_string: &str, batch_size: Option<usize>, db_name: Option<String>) -> Result<Self, String> {
        let batch_size = batch_size.unwrap_or(1024);
        
        // 1. Parse the connection string into a Config object
        let mut config = Config::from_str(connection_string)
            .map_err(|e| format!("Invalid connection string: {}", e))?;

        let target_db_name = if let Some(name) = db_name {
            name
        } else {
            // 2. Connect to 'postgres' database to check/create the target database
            let mut maintenance_config = config.clone();
            maintenance_config.dbname("postgres");

            let name = Local::now().format("%Y%m%d").to_string();

            {
                let mut client = maintenance_config.connect(NoTls)
                    .map_err(|e| format!("Failed to connect to maintenance DB: {}", e))?;
                
                let check_query = "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)";
                let exists: bool = client.query_one(check_query, &[&name])
                    .map_err(|e| format!("Failed to check DB existence: {}", e))?
                    .get(0);

                if !exists {
                    let create_query = format!("CREATE DATABASE \"{}\"", name);
                    client.batch_execute(&create_query)
                        .map_err(|e| format!("Failed to create database '{}': {}", name, e))?;
                }
            }
            name
        };

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
                    let attr_value: Option<serde_json::Value> = match &record.attr {
                        Some(s) => match serde_json::from_str(s) {
                            Ok(v) => Some(v),
                            Err(e) => {
                                eprintln!("Failed to parse JSON attr: {}", e);
                                None
                            }
                        },
                        None => None,
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

    pub fn report(&self, message: String, span_id: Uuid, parent_id: Uuid, attr: Option<String>, record_type: i32) -> Result<(), String> {
        let record = Record {
            span_id,
            parent_id,
            record_type,
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

// Global Registry
// Use Mutex<Option<Arc<RustDatabase>>> for a single global instance
static REGISTRY: Mutex<Option<Arc<RustDatabase>>> = Mutex::new(None);

#[pyfunction]
#[pyo3(signature = (connection_string, batch_size=None, candidate_name=None))]
fn initialize(connection_string: &str, batch_size: Option<usize>, candidate_name: Option<String>) -> PyResult<String> {
    let mut guard = REGISTRY.lock().map_err(|e| PyRuntimeError::new_err(format!("Registry lock error: {}", e)))?;
    
    if guard.is_some() {
        // Already initialized. 
        // According to requirements: "init函数只能调用一次".
        // We can either return the existing name or throw an error.
        // Let's return the existing name but log a warning or just return it silently.
        // However, if the user expects to re-initialize with different params, this is a hard error.
        // Let's assume strict single initialization.
        return Err(PyRuntimeError::new_err("Database already initialized"));
    }

    // Create new
    let db = RustDatabase::new(connection_string, batch_size, candidate_name.clone())
        .map_err(PyRuntimeError::new_err)?;
    
    let name = db.db_name.clone();
    *guard = Some(Arc::new(db));
    
    Ok(name)
}

#[pyfunction]
fn flush() -> PyResult<()> {
    let guard = REGISTRY.lock().map_err(|e| PyRuntimeError::new_err(format!("Registry lock error: {}", e)))?;
    if let Some(db) = guard.as_ref() {
        db.flush().map_err(PyRuntimeError::new_err)
    } else {
        // If not initialized, flush does nothing
        Ok(())
    }
}

// --- Tracer Implementation ---

struct TracerInner {
    initial_parent_id: Uuid,
    states: DashMap<ThreadId, Vec<Uuid>>,
}

#[pyclass]
struct Tracer {
    inner: Arc<TracerInner>,
}

#[pymethods]
impl Tracer {
    #[new]
    #[pyo3(signature = (parent_id=None))]
    fn new(parent_id: Option<String>) -> PyResult<Self> {
        let pid = if let Some(s) = parent_id {
            if s.is_empty() {
                Uuid::nil()
            } else {
                Uuid::parse_str(&s).map_err(|e| PyRuntimeError::new_err(format!("Invalid parent_id: {}", e)))?
            }
        } else {
            Uuid::nil()
        };

        Ok(Tracer {
            inner: Arc::new(TracerInner {
                initial_parent_id: pid,
                states: DashMap::new(),
            }),
        })
    }

    #[pyo3(signature = (message, attr=None))]
    fn log(&self, message: String, attr: Option<String>) -> PyResult<()> {
        let current_pid = self.get_current_parent_id();
        let span_id = Uuid::now_v7();
        
        let guard = REGISTRY.lock().map_err(|e| PyRuntimeError::new_err(format!("Registry lock error: {}", e)))?;
        let db = guard.as_ref().ok_or_else(|| PyRuntimeError::new_err("Database not initialized"))?;
        
        db.report(message, span_id, current_pid, attr, 0).map_err(PyRuntimeError::new_err)
    }

    #[pyo3(signature = (message, attr=None))]
    fn span(&self, message: String, attr: Option<String>) -> SpanGuard {
        SpanGuard {
            inner: self.inner.clone(),
            message,
            attr,
            span_id: Uuid::now_v7(),
        }
    }
}

impl Tracer {
    fn get_current_parent_id(&self) -> Uuid {
        let tid = thread::current().id();
        if let Some(stack) = self.inner.states.get(&tid) {
            if let Some(last) = stack.last() {
                return *last;
            }
        }
        self.inner.initial_parent_id
    }
}

#[pyclass]
struct SpanGuard {
    inner: Arc<TracerInner>,
    message: String,
    attr: Option<String>,
    span_id: Uuid,
}

#[pymethods]
impl SpanGuard {
    fn __enter__(&self) -> PyResult<()> {
        let tid = thread::current().id();
        
        // Get current parent ID (before pushing self)
        let current_pid = {
            if let Some(stack) = self.inner.states.get(&tid) {
                if let Some(last) = stack.last() {
                    *last
                } else {
                    self.inner.initial_parent_id
                }
            } else {
                self.inner.initial_parent_id
            }
        };

        // Report Start
        {
            let guard = REGISTRY.lock().map_err(|e| PyRuntimeError::new_err(format!("Registry lock error: {}", e)))?;
            let db = guard.as_ref().ok_or_else(|| PyRuntimeError::new_err("Database not initialized"))?;
            // Type 1 for Span Start
            db.report(self.message.clone(), self.span_id, current_pid, self.attr.clone(), 1)
                .map_err(PyRuntimeError::new_err)?;
        }

        // Push self to stack
        self.inner.states.entry(tid).or_insert_with(Vec::new).push(self.span_id);
        
        Ok(())
    }

    fn __exit__(&self, _exc_type: Option<PyObject>, _exc_value: Option<PyObject>, _traceback: Option<PyObject>) -> PyResult<()> {
        let tid = thread::current().id();
        
        // Pop self from stack
        if let Some(mut stack) = self.inner.states.get_mut(&tid) {
            stack.pop();
        }
        
        // Get current parent ID (after popping)
        let current_pid = {
            if let Some(stack) = self.inner.states.get(&tid) {
                if let Some(last) = stack.last() {
                    *last
                } else {
                    self.inner.initial_parent_id
                }
            } else {
                self.inner.initial_parent_id
            }
        };

        // Report End
        {
            let guard = REGISTRY.lock().map_err(|e| PyRuntimeError::new_err(format!("Registry lock error: {}", e)))?;
            let db = guard.as_ref().ok_or_else(|| PyRuntimeError::new_err("Database not initialized"))?;
            // Type 2 for Span End
            db.report(self.message.clone(), self.span_id, current_pid, self.attr.clone(), 2)
                .map_err(PyRuntimeError::new_err)?;
        }
        
        Ok(())
    }
}

#[pymodule]
fn longtrace(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(initialize, m)?)?;
    m.add_function(wrap_pyfunction!(flush, m)?)?;
    m.add_class::<Tracer>()?;
    m.add_class::<SpanGuard>()?;

    // Register atexit hook for automatic flush
    let py = m.py();
    let atexit = py.import_bound("atexit")?;
    atexit.call_method1("register", (m.getattr("flush")?,))?;

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
        let db_result = RustDatabase::new(&conn_str, None, None);
        
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
        let db = RustDatabase::new(&conn_str, Some(5), None).expect("Failed to create database");
        
        let test_span_id = Uuid::now_v7();
        let test_parent_id = Uuid::now_v7();
        let test_id = Uuid::now_v7().to_string(); // Unique test ID for this run
        
        // Report some records
        for i in 0..10 {
            let message = format!("Test message {}", i);
            let attr = json!({"index": i, "test_id": &test_id}).to_string();
            
            db.report(message, test_span_id, test_parent_id, Some(attr.clone()), 0).expect("Failed to report");
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
