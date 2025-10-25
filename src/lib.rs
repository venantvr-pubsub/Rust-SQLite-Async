use crossbeam_channel::{self, Receiver, Sender};
use log::{error, info};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, Error as RusqliteError, Params, Row, TransactionBehavior};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

// Pour envoyer des paramètres SQL 'Send' (thread-safe)
pub use rusqlite::types::Value;

// Capacité maximale de la file d'attente avant que `execute_write` ne devienne bloquant.
// Prévient la saturation de la RAM (OOM).
const CHANNEL_CAPACITY: usize = 10000;

/// Erreur d'envoi de tâche d'écriture
#[derive(Debug)]
pub struct WriteError;

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Failed to send write task")
    }
}

impl std::error::Error for WriteError {}

// Tâches que le thread worker peut recevoir
// Utilise Arc pour éviter les clones coûteux
#[derive(Clone)]
enum WriteTask {
    Execute { sql: Arc<str>, params: Arc<[Value]> },
    ExecuteScript { script: Arc<str> },
    // Le Sender<()> est utilisé comme un 'threading.Event' pour notifier
    // le demandeur que la tâche 'Sync' est terminée.
    Sync(Sender<()>),
    // Sentinel pour arrêter le thread
    Stop,
}

// Actions que le worker peut entreprendre après avoir traité une tâche
#[derive(PartialEq)]
enum WorkerAction {
    Continue,
    Stop,
}

// État partagé pour le signal "ready"
type ReadyState = Arc<(Mutex<bool>, Condvar)>;

/// Gère une connexion SQLite avec un thread d'écriture dédié.
pub struct AsyncSqlite {
    db_path: String,
    task_sender: Option<Sender<WriteTask>>,
    writer_thread: Option<JoinHandle<Result<(), RusqliteError>>>,
    ready_state: ReadyState,
    // NOUVEAU: Pool de connexions pour les lectures
    read_pool: r2d2::Pool<SqliteConnectionManager>,
}

impl AsyncSqlite {
    /// Crée un nouveau gestionnaire de base de données.
    /// N'active pas le thread worker avant l'appel à .start().
    pub fn new(db_path: &str) -> Self {
        let db_path_str = if db_path == ":memory:" {
            // Équivalent Rust de 'file::memory:?cache=shared' pour le multithreading
            "file:memdb?mode=memory&cache=shared".to_string()
        } else {
            db_path.to_string()
        };

        // --- NOUVEAU: Initialisation du Pool de lecture ---
        // Le pool est partagé par tous les threads qui veulent *lire*.
        // Pour les bases en mémoire, nous devons d'abord créer la connexion principale
        let manager = SqliteConnectionManager::file(db_path_str.clone());
        let read_pool = r2d2::Pool::builder()
            .max_size(10) // 10 connexions de lecture max en parallèle
            .min_idle(Some(2)) // Garde 2 connexions prêtes pour réduire latence
            .connection_timeout(Duration::from_secs(30))
            .build(manager)
            .expect("Failed to create read connection pool");

        Self {
            db_path: db_path_str,
            task_sender: None,
            writer_thread: None,
            ready_state: Arc::new((Mutex::new(false), Condvar::new())),
            read_pool,
        }
    }

    /// Démarre le thread worker en arrière-plan.
    pub fn start(&mut self) {
        if self.writer_thread.is_some() {
            return; // Déjà démarré
        }

        // --- MODIFIÉ: Utilisation d'un canal borné ---
        let (task_sender, task_receiver) =
            crossbeam_channel::bounded::<WriteTask>(CHANNEL_CAPACITY);
        let db_path = self.db_path.clone();
        let ready_state = Arc::clone(&self.ready_state);

        let writer_thread = thread::Builder::new()
            .name("AsyncSQLiteWorker".to_string())
            .spawn(move || database_worker_fn(db_path, task_receiver, ready_state))
            .expect("Failed to spawn database worker thread");

        self.task_sender = Some(task_sender);
        self.writer_thread = Some(writer_thread);
    }

    /// Attend que le thread worker ait initialisé la connexion.
    pub fn wait_for_ready(&self, timeout: Duration) -> bool {
        let (lock, cvar) = &*self.ready_state;
        let ready = lock.lock().unwrap();
        if *ready {
            return true;
        } // Vérification rapide
        cvar.wait_timeout_while(ready, timeout, |ready_val| !*ready_val)
            .unwrap()
            .1
            .timed_out()
            == false
    }

    /// Vérifie si la BDD est prête (non-bloquant).
    fn is_ready(&self) -> bool {
        let (lock, _) = &*self.ready_state;
        *lock.lock().unwrap()
    }

    /// Arrête proprement le thread worker.
    /// Appelé automatiquement lorsque `AsyncSqlite` est "drop" (sort du scope).
    pub fn stop(&mut self) {
        if let Some(thread) = self.writer_thread.take() {
            info!("Stopping database worker...");
            if let Some(sender) = self.task_sender.as_ref() {
                // S'assurer que la file est vide avant d'envoyer 'Stop'
                let (sync_tx, sync_rx) = crossbeam_channel::bounded(1);
                sender.send(WriteTask::Sync(sync_tx)).ok();
                sync_rx.recv_timeout(Duration::from_secs(5)).ok();

                // Envoyer le signal d'arrêt
                sender.send(WriteTask::Stop).ok();
            }
            // Attendre la fin du thread
            thread.join().expect("Worker thread panicked").ok();
            info!("Worker stopped.");
        }
        self.task_sender = None;
    }

    /// Attend que toutes les écritures actuellement dans la file soient terminées.
    pub fn sync(&self, timeout: Duration) -> bool {
        // Canal 'one-shot' pour cet événement de synchro
        let (sync_tx, sync_rx) = crossbeam_channel::bounded(1);
        if let Some(sender) = self.task_sender.as_ref() {
            if sender.send(WriteTask::Sync(sync_tx)).is_err() {
                return false; // Le thread worker est mort
            }
            // Attendre la notification de retour
            sync_rx.recv_timeout(timeout).is_ok()
        } else {
            false
        }
    }

    /// Ajoute une opération d'écriture à la file.
    /// **Bloque si la file est pleine** (capacité = `CHANNEL_CAPACITY`).
    pub fn execute_write(&self, sql: &str, params: Vec<Value>) -> Result<(), WriteError> {
        let task = WriteTask::Execute {
            sql: sql.into(),
            params: params.into(),
        };
        self.task_sender
            .as_ref()
            .ok_or(WriteError)?
            .send(task)
            .map_err(|_| WriteError)
    }

    /// Ajoute un script SQL (lu depuis un fichier) à la file.
    pub fn execute_script(&self, script_path: &Path) -> Result<(), std::io::Error> {
        let script = fs::read_to_string(script_path)?;
        let task = WriteTask::ExecuteScript {
            script: script.into(),
        };

        if let Some(sender) = self.task_sender.as_ref() {
            sender
                .send(task)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Worker thread is not running",
            ))
        }
    }

    // --- LECTURES OPTIMISÉES AVEC LE POOL ---

    /// Ouvre une connexion depuis le pool pour lire les données.
    fn get_read_conn(
        &self,
    ) -> Result<r2d2::PooledConnection<SqliteConnectionManager>, RusqliteError> {
        if !self.is_ready() {
            return Err(RusqliteError::InvalidQuery);
        }
        self.read_pool
            .get()
            .map_err(|_e| RusqliteError::InvalidQuery)
    }

    /// Exécute une lecture et retourne toutes les lignes.
    pub fn query_read_all<T, P, F>(
        &self,
        sql: &str,
        params: P,
        f: F,
    ) -> Result<Vec<T>, RusqliteError>
    where
        P: Params,
        F: FnMut(&Row) -> Result<T, RusqliteError>,
    {
        let conn = self.get_read_conn()?;
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map(params, f)?;

        let mut results = Vec::new();
        for row_result in rows {
            results.push(row_result?);
        }
        Ok(results)
    }

    /// Exécute une lecture et ne retourne que la première ligne.
    pub fn query_read_one<T, P, F>(&self, sql: &str, params: P, f: F) -> Result<T, RusqliteError>
    where
        P: Params,
        F: FnOnce(&Row) -> Result<T, RusqliteError>,
    {
        let conn = self.get_read_conn()?;
        let mut stmt = conn.prepare(sql)?;
        stmt.query_row(params, f)
    }
}

/// Gère la fermeture propre du thread lorsque l'objet `AsyncSqlite` est détruit.
impl Drop for AsyncSqlite {
    fn drop(&mut self) {
        self.stop();
    }
}

// --- WORKER ENTIÈREMENT REFACTORISÉ POUR LE BATCHING ---

/// La fonction exécutée par le thread worker.
fn database_worker_fn(
    db_path: String,
    receiver: Receiver<WriteTask>,
    ready_state: ReadyState,
) -> Result<(), RusqliteError> {
    // 1. Ouvrir la connexion d'écriture (une seule)
    let mut conn = match Connection::open(db_path.clone()) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to open database connection: {}", e);
            return Err(e);
        }
    };

    // 2. Configurer la connexion (WAL, etc.)
    if !db_path.starts_with("file:memdb") {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "busy_timeout", 30000)?;
    }

    info!("Database worker is ready.");

    // 3. Signaler que la BDD est prête
    {
        let (lock, cvar) = &*ready_state;
        let mut ready = lock.lock().unwrap();
        *ready = true;
        cvar.notify_one();
    }

    // 4. Boucle principale de traitement par lots
    let mut sync_notifiers: Vec<Sender<()>> = Vec::new();

    // Attend la première tâche (bloquant)
    while let Ok(first_task) = receiver.recv() {
        sync_notifiers.clear();

        // Ouvre UNE SEULE transaction pour tout le lot
        let tx_result: Result<WorkerAction, RusqliteError> = (|| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

            // 1. Traiter la première tâche
            let mut action = process_task(&tx, first_task, &mut sync_notifiers)?;
            if action == WorkerAction::Stop {
                tx.commit()?;
                return Ok(WorkerAction::Stop);
            }

            // 2. Vider la file (non-bloquant) et traiter le reste du lot
            while let Ok(task) = receiver.try_recv() {
                action = process_task(&tx, task, &mut sync_notifiers)?;
                if action == WorkerAction::Stop {
                    tx.commit()?;
                    return Ok(WorkerAction::Stop);
                }
            }

            // 3. Committer le lot entier
            tx.commit()?;
            Ok(WorkerAction::Continue)
        })(); // Fin de la closure

        // 4. Notifier tous les appelants de `sync()` APRES le commit
        for notify in sync_notifiers.drain(..) {
            notify.send(()).ok(); // Ignorer l'erreur si le receveur est parti
        }

        match tx_result {
            Ok(WorkerAction::Stop) => break,  // Sortir de la boucle 'while let'
            Ok(WorkerAction::Continue) => (), // Continuer à la prochaine boucle
            Err(e) => {
                error!(
                    "Database worker transaction failed. Rollback occurred. Error: {}",
                    e
                );
                // La transaction a échoué, les tâches de ce lot sont perdues.
                // Les 'sync_notifiers' n'ont pas été notifiés, donc les appels à `sync()`
                // vont (correctement) expirer.
            }
        }
    }

    info!("Database worker shutting down.");
    Ok(())
}

/// Fonction helper pour traiter une seule tâche à l'intérieur de la transaction du worker
fn process_task(
    tx: &rusqlite::Transaction,
    task: WriteTask,
    notifiers: &mut Vec<Sender<()>>,
) -> Result<WorkerAction, RusqliteError> {
    match task {
        WriteTask::Execute { sql, params } => {
            // Convertit Vec<Value> en &[&dyn ToSql]
            let params_slice: Vec<&dyn rusqlite::ToSql> =
                params.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
            tx.execute(&sql, params_slice.as_slice())?;
        }
        WriteTask::ExecuteScript { script } => {
            tx.execute_batch(&script)?;
        }
        WriteTask::Sync(notify) => {
            // Ne pas notifier maintenant. Ajouter à la liste pour
            // notification APRES le commit.
            notifiers.push(notify);
        }
        WriteTask::Stop => {
            return Ok(WorkerAction::Stop);
        }
    }
    Ok(WorkerAction::Continue)
}

// ---
// TESTS (Inchangés, mais ils testent maintenant la nouvelle implémentation)
// ---

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use std::path::PathBuf;
    use std::time::Duration;
    use tempfile::NamedTempFile;

    // Équivalent du fixture 'temp_db_path'
    fn temp_db() -> (NamedTempFile, PathBuf) {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        (temp_file, path)
    }

    // Équivalent du fixture 'db_manager'
    fn setup_db() -> (AsyncSqlite, NamedTempFile) {
        let (temp_file, db_path) = temp_db();
        let mut db = AsyncSqlite::new(db_path.to_str().unwrap());
        db.start();
        assert!(
            db.wait_for_ready(Duration::from_secs(5)),
            "La BDD n'a pas pu démarrer à temps."
        );
        (db, temp_file)
    }

    #[test]
    fn test_initialization_file_path() {
        let (_temp_file, db_path) = temp_db();
        let db_path_str = db_path.to_str().unwrap().to_string();
        let db = AsyncSqlite::new(&db_path_str);
        assert_eq!(db.db_path, db_path_str);
    }

    #[test]
    fn test_initialization_in_memory() {
        let db = AsyncSqlite::new(":memory:");
        assert_eq!(db.db_path, "file:memdb?mode=memory&cache=shared");
    }

    #[test]
    fn test_lifecycle_start_wait_stop() {
        let (_temp_file, db_path) = temp_db();
        let mut db = AsyncSqlite::new(db_path.to_str().unwrap());

        assert!(db.writer_thread.is_none());
        db.start();
        assert!(db.writer_thread.is_some());

        let ready = db.wait_for_ready(Duration::from_secs(5));
        assert!(ready);

        db.stop();
        assert!(db.writer_thread.is_none());
    }

    #[test]
    fn test_read_before_ready() {
        let db = AsyncSqlite::new(":memory:");
        // Pas de .start()
        let result: Result<i32, _> = db.query_read_one("SELECT 1", [], |row| row.get(0));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RusqliteError::InvalidQuery));
    }

    #[test]
    fn test_write_and_read() {
        let (db, _temp_file) = setup_db();

        db.execute_write(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            vec![],
        )
        .unwrap();
        db.execute_write(
            "INSERT INTO users (name) VALUES (?)",
            vec![Value::Text("Alice".to_string())],
        )
        .unwrap();
        db.execute_write(
            "INSERT INTO users (name) VALUES (?)",
            vec![Value::Text("Bob".to_string())],
        )
        .unwrap();

        // 'sync' garantit que le lot d'écriture est commit
        assert!(db.sync(Duration::from_secs(5)));

        let results: Result<Vec<String>, _> =
            db.query_read_all("SELECT name FROM users ORDER BY name", [], |row| row.get(0));

        let users = results.unwrap();
        assert_eq!(users.len(), 2);
        assert_eq!(users[0], "Alice");
        assert_eq!(users[1], "Bob");
    }

    #[test]
    fn test_execute_read_fetch_one() {
        let (db, _temp_file) = setup_db();

        db.execute_write("CREATE TABLE settings (key TEXT, value TEXT)", vec![])
            .unwrap();
        db.execute_write(
            "INSERT INTO settings VALUES (?, ?)",
            vec![
                Value::Text("theme".to_string()),
                Value::Text("dark".to_string()),
            ],
        )
        .unwrap();

        assert!(db.sync(Duration::from_secs(5)));

        // Utilise maintenant le pool de connexions
        let result: Result<String, _> = db.query_read_one(
            "SELECT value FROM settings WHERE key=?",
            params!["theme"],
            |row| row.get(0),
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "dark");
    }

    #[test]
    fn test_execute_script() {
        let (db, _temp_file) = setup_db();
        let (_script_file, script_path) = temp_db();

        let script_content = "
        CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO products (name) VALUES ('Laptop');
        INSERT INTO products (name) VALUES ('Mouse');
        ";
        fs::write(&script_path, script_content).unwrap();

        db.execute_script(&script_path).unwrap();

        assert!(db.sync(Duration::from_secs(5)));

        let count: i64 = db
            .query_read_one("SELECT COUNT(*) FROM products", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_concurrent_writes() {
        let (db, _temp_file) = setup_db();

        db.execute_write(
            "CREATE TABLE records (id INTEGER PRIMARY KEY, thread_id INTEGER, value INTEGER)",
            vec![],
        )
        .unwrap();

        assert!(db.sync(Duration::from_secs(5)));

        let num_threads = 10;
        let writes_per_thread = 50;
        let db_arc = Arc::new(db); // Partager 'db' entre les threads
        let mut threads = vec![];

        for i in 0..num_threads {
            let db_clone = Arc::clone(&db_arc);
            let thread = thread::spawn(move || {
                for j in 0..writes_per_thread {
                    db_clone
                        .execute_write(
                            "INSERT INTO records (thread_id, value) VALUES (?, ?)",
                            vec![Value::Integer(i), Value::Integer(j)],
                        )
                        .unwrap();
                }
            });
            threads.push(thread);
        }

        for thread in threads {
            thread.join().unwrap();
        }

        // Attendre que le dernier lot d'écritures soit terminé
        assert!(
            db_arc.sync(Duration::from_secs(10)),
            "La synchronisation a échoué."
        );

        let total_records: i64 = db_arc
            .query_read_one("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(total_records, (num_threads * writes_per_thread) as i64);
    }
}
