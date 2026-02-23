# Rust SQLite Async

[![Crates.io](https://img.shields.io/crates/v/rust_sqlite_async.svg)](https://crates.io/crates/rust_sqlite_async)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

`rust_sqlite_async` est un wrapper Rust pour SQLite3 qui offre une interface thread-safe et non bloquante. Il exécute toutes les opérations d'écriture dans un thread d'
arrière-plan dédié, tandis que les lectures sont effectuées à l'aide d'un pool de connexions pour des performances optimales.

Cette caisse est idéale pour les applications qui nécessitent une interaction avec une base de données SQLite sans bloquer le thread principal, comme dans les
applications GUI ou les serveurs web à haute concurrence.

## Fonctionnalités

* **Écritures Asynchrones**: Les insertions, mises à jour et autres opérations d'écriture sont envoyées à un thread dédié via un canal, évitant de bloquer le thread
  appelant.
* **Pool de Connexions pour les Lectures**: Les requêtes de lecture (`SELECT`) sont gérées par un pool de connexions `r2d2`, permettant des lectures parallèles et
  performantes.
* **Sécurité des Threads (Thread-Safe)**: La structure `AsyncSqlite` peut être partagée en toute sécurité entre plusieurs threads (`Sync` + `Send`).
* **Back-Pressure**: Le canal pour les écritures a une capacité limitée, ce qui empêche une utilisation excessive de la mémoire si les écritures sont produites plus
  rapidement qu'elles ne peuvent être traitées.
* **Gestion du Cycle de Vie**: Le thread d'arrière-plan est démarré et arrêté proprement, avec des méthodes pour attendre la synchronisation.

## Installation

Ajoutez cette ligne à votre `Cargo.toml`:

```toml
[dependencies]
rust_sqlite_async = "0.1.0"
```

*(Note: Remplacez `0.1.0` par la version souhaitée.)*

## Exemple d'Utilisation

Voici un exemple simple pour créer une base de données, y écrire et y lire des données.

```rust
use rust_sqlite_async::AsyncSqlite;
use rusqlite::params;
use std::time::Duration;

fn main() {
    // Utiliser une base de données en mémoire pour cet exemple
    let mut db = AsyncSqlite::new(":memory:");
    db.start();

    // Attendre que la base de données soit prête
    if !db.wait_for_ready(Duration::from_secs(5)) {
        panic!("La base de données n'a pas pu démarrer à temps");
    }

    // Effectuer des écritures de manière asynchrone
    db.execute_write(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        vec![],
    ).unwrap();

    db.execute_write(
        "INSERT INTO users (name) VALUES (?)",
        vec!["Alice".into()],
    ).unwrap();

    // `sync` attend que toutes les écritures en attente soient terminées
    assert!(db.sync(Duration::from_secs(5)), "La synchronisation a échoué");

    // Effectuer des lectures en utilisant une connexion du pool
    let name: String = db.query_read_one(
        "SELECT name FROM users WHERE id = ?",
        params![1],
    ).unwrap();

    println!("Utilisateur trouvé: {}", name);
    assert_eq!(name, "Alice");

    // La méthode `stop` est appelée automatiquement lorsque `db` est détruit (Drop)
}
```

## API Principale

* `AsyncSqlite::new(path)`: Crée une nouvelle instance. Le chemin peut être un fichier ou `":memory:"`.
* `db.start()`: Démarre le thread d'écriture en arrière-plan.
* `db.wait_for_ready(timeout)`: Attend que la connexion à la base de données soit établie.
* `db.execute_write(sql, params)`: Ajoute une opération d'écriture à la file d'attente. Bloque si la file est pleine.
* `db.execute_script(path)`: Exécute un script SQL à partir d'un fichier.
* `db.query_read_one(sql, params)`: Exécute une requête et attend une seule ligne en retour.
* `db.query_read_all(sql, params)`: Exécute une requête et retourne toutes les lignes correspondantes.
* `db.sync(timeout)`: Bloque jusqu'à ce que toutes les écritures en attente soient terminées.
* `db.stop()`: Arrête proprement le thread d'arrière-plan (appelé automatiquement via `Drop`).

## Licence

Ce projet est sous licence MIT. Voir le fichier [LICENSE](./LICENSE) pour plus de détails.

## Stack

[![Stack](https://skillicons.dev/icons?i=rust,sqlite,py&theme=dark)](https://skillicons.dev)
