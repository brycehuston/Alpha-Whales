use lazy_static::lazy_static;
use rusqlite::{params, Connection};
use std::sync::Mutex;

lazy_static! {
    // We use a global mutex for simplicity since DB writes are low-frequency compared to the main loop.
    static ref DB_CONN: Mutex<Option<Connection>> = Mutex::new(None);
}

#[allow(dead_code)]
pub fn init_db() -> Result<(), String> {
    log::info!("[db] init_db: opening trade_telemetry.db");
    let conn = Connection::open("trade_telemetry.db")
        .map_err(|e| format!("Failed to open trade_telemetry.db: {e}"))?;
    log::info!("[db] init_db: connection opened successfully");

    // -- Phase 5 tables -------------------------------------------------------
    //
    // bundles: tracks the full lifecycle of every submitted Jito bundle.
    let create_bundles_sql = "
        CREATE TABLE IF NOT EXISTS bundles (
            bundle_id TEXT PRIMARY KEY,
            region TEXT NOT NULL,
            token_mint TEXT NOT NULL,
            transaction_signature TEXT NOT NULL,
            submitted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            landed_slot INTEGER,
            landed_at DATETIME,
            status TEXT NOT NULL DEFAULT 'submitted',
            failure_reason TEXT
        )
    ";

    // positions: tracks confirmed on-chain positions with full metadata.
    let create_positions_sql = "
        CREATE TABLE IF NOT EXISTS positions (
            token_mint TEXT PRIMARY KEY,
            bundle_id TEXT NOT NULL,
            region TEXT NOT NULL,
            slot INTEGER NOT NULL,
            transaction_signature TEXT NOT NULL,
            amount_in_lamports INTEGER NOT NULL,
            acquired_amount_raw INTEGER NOT NULL,
            entry_price_num INTEGER NOT NULL,
            entry_price_den INTEGER NOT NULL,
            tip_lamports INTEGER NOT NULL,
            pool_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            opened_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            closed_at DATETIME,
            FOREIGN KEY (bundle_id) REFERENCES bundles(bundle_id)
        )
    ";

    // Index on (token_mint, status) for fast active-position lookups.
    let create_position_index_sql = "
        CREATE INDEX IF NOT EXISTS idx_positions_mint_status
        ON positions(token_mint, status)
    ";

    // -- Original tables (Phase 1-4) -----------------------------------------

    let create_table_sql = "
        CREATE TABLE IF NOT EXISTS trade_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            wallet_address TEXT,
            token_mint TEXT,
            trade_direction TEXT,
            trade_size_sol REAL,
            market_cap_usd REAL,
            execution_status TEXT
        )
    ";

    // Migration: open_positions remains for backward compatibility; the new
    // positions table supersedes it.
    let create_open_positions_sql = "
        CREATE TABLE IF NOT EXISTS open_positions (
            token_mint TEXT PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ";

    if let Err(e) = conn.execute(create_bundles_sql, []) {
        eprintln!("⚠️  Failed to create bundles table: {}", e);
    } else {
        log::info!("[db] init_db: bundles table ready");
    }

    if let Err(e) = conn.execute(create_positions_sql, []) {
        eprintln!("⚠️  Failed to create positions table: {}", e);
    } else {
        log::info!("[db] init_db: positions table ready");
    }

    if let Err(e) = conn.execute(create_position_index_sql, []) {
        eprintln!("⚠️  Failed to create positions index: {}", e);
    } else {
        log::info!("[db] init_db: idx_positions_mint_status ready");
    }

    if let Err(e) = conn.execute(create_open_positions_sql, []) {
        eprintln!("⚠️  Failed to create open_positions table: {}", e);
    } else {
        log::info!("[db] init_db: open_positions table ready");
    }

    if let Err(e) = conn.execute(create_table_sql, []) {
        eprintln!("⚠️  Failed to create trade_logs table: {}", e);
    } else {
        log::info!("[db] init_db: trade_logs table ready");
        // Index on (wallet_address, token_mint): get_whale_history() filters
        // on both columns. Without this index the query is a full table scan,
        // which becomes expensive after days of active trading.
        let index_sql = "CREATE INDEX IF NOT EXISTS idx_wallet_mint \
                         ON trade_logs(wallet_address, token_mint)";
        if let Err(e) = conn.execute(index_sql, []) {
            eprintln!("⚠️  Failed to create wallet_mint index: {}", e);
        } else {
            log::info!("[db] init_db: idx_wallet_mint ready");
        }
    }

    // Store the connection in the global singleton. The mutex guard is dropped
    // immediately, releasing the lock. All subsequent DB operations re-acquire
    // the lock themselves, so holding it here is only for the assignment.
    *DB_CONN
        .lock()
        .map_err(|e| format!("DB_CONN mutex poisoned: {e}"))? = Some(conn);
    log::info!("[db] init_db: connection stored in DB_CONN global singleton");

    Ok(())
}

#[allow(dead_code)]
pub fn log_trade_telemetry(
    wallet_address: &str,
    token_mint: &str,
    trade_direction: &str,
    trade_size_sol: f64,
    market_cap_usd: f64,
    execution_status: &str,
) {
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(conn) = lock.as_ref() {
        let sql = "
            INSERT INTO trade_logs (
                wallet_address, token_mint, trade_direction, 
                trade_size_sol, market_cap_usd, execution_status
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ";
        if let Err(e) = conn.execute(
            sql,
            params![
                wallet_address,
                token_mint,
                trade_direction,
                trade_size_sol,
                market_cap_usd,
                execution_status
            ],
        ) {
            eprintln!("⚠️  Failed to insert trade log: {}", e);
        }
    }
}

#[allow(dead_code)]
pub struct WhaleHistory {
    pub buys: i32,
    pub sells: i32,
    pub net_sol: f64,
    pub status: String,
}

#[allow(dead_code)]
fn empty_whale_history() -> WhaleHistory {
    WhaleHistory {
        buys: 0,
        sells: 0,
        net_sol: 0.0,
        status: "Unknown".to_string(),
    }
}

#[allow(dead_code)]
pub fn try_get_whale_history(wallet: &str, mint: &str) -> Result<WhaleHistory, String> {
    let mut history = empty_whale_history();
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    let conn = lock
        .as_ref()
        .ok_or_else(|| "trade telemetry database is unavailable".to_string())?;
    let sql = "
        SELECT trade_direction, trade_size_sol
        FROM trade_logs
        WHERE wallet_address = ?1 AND token_mint = ?2
    ";

    let mut stmt = conn
        .prepare(sql)
        .map_err(|error| format!("failed to prepare whale-history query: {error}"))?;
    let rows = stmt
        .query_map(params![wallet, mint], |row| {
            let direction: String = row.get(0)?;
            let size_sol: f64 = row.get(1)?;
            Ok((direction, size_sol))
        })
        .map_err(|error| format!("failed to query whale history: {error}"))?;

    let mut buy_sol = 0.0;
    let mut sell_sol = 0.0;

    for row in rows {
        let (direction, size) =
            row.map_err(|error| format!("failed to read whale-history row: {error}"))?;
        if direction == "BUY" {
            history.buys += 1;
            buy_sol += size;
        } else if direction == "SELL" {
            history.sells += 1;
            sell_sol += size;
        }
    }

    history.net_sol = buy_sol - sell_sol;

    // Format to 4 decimal places
    history.net_sol = (history.net_sol * 10000.0).round() / 10000.0;

    if history.net_sol > 0.0 {
        history.status = "Holding/Accumulating".to_string();
    } else {
        history.status = "Exited/Sold All".to_string();
    }

    Ok(history)
}

#[allow(dead_code)]
pub fn get_whale_history(wallet: &str, mint: &str) -> WhaleHistory {
    try_get_whale_history(wallet, mint).unwrap_or_else(|_| empty_whale_history())
}

pub fn insert_open_position(mint: &str) {
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(conn) = lock.as_ref() {
        let sql = "INSERT OR REPLACE INTO open_positions (token_mint) VALUES (?1)";
        if let Err(e) = conn.execute(sql, params![mint]) {
            eprintln!("⚠️  Failed to insert open position {}: {}", mint, e);
        }
    }
}

pub fn remove_open_position(mint: &str) {
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(conn) = lock.as_ref() {
        let sql = "DELETE FROM open_positions WHERE token_mint = ?1";
        if let Err(e) = conn.execute(sql, params![mint]) {
            eprintln!("⚠️  Failed to remove open position {}: {}", mint, e);
        }
    }
}

#[allow(dead_code)]
pub fn get_all_open_positions() -> Vec<String> {
    let mut positions = Vec::new();
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(conn) = lock.as_ref() {
        let sql = "SELECT token_mint FROM open_positions ORDER BY timestamp ASC";
        if let Ok(mut stmt) = conn.prepare(sql) {
            if let Ok(rows) = stmt.query_map([], |row| row.get(0)) {
                for row in rows.flatten() {
                    positions.push(row);
                }
            }
        }
    }
    positions
}

// ============================================================================
// Phase 5 — Bundle & Position Tracking DB Functions
// ============================================================================

#[allow(dead_code)]
/// Record a landed bundle in the bundles table.
/// Returns an error string on failure.
pub fn record_landed_bundle(
    bundle_id: &str,
    region: &str,
    token_mint: &str,
    landed_slot: u64,
    transaction_signature: &str,
) -> Result<(), String> {
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    let conn = lock
        .as_ref()
        .ok_or_else(|| "database is unavailable".to_string())?;

    // Insert or update the bundle record.
    let sql = "
        INSERT INTO bundles (bundle_id, region, token_mint, transaction_signature, landed_slot, landed_at, status)
        VALUES (?1, ?2, ?3, ?4, ?5, CURRENT_TIMESTAMP, 'landed')
        ON CONFLICT(bundle_id) DO UPDATE SET
            landed_slot = excluded.landed_slot,
            landed_at = CURRENT_TIMESTAMP,
            status = 'landed'
    ";
    conn.execute(
        sql,
        params![
            bundle_id,
            region,
            token_mint,
            transaction_signature,
            landed_slot
        ],
    )
    .map_err(|e| format!("failed to record landed bundle: {e}"))?;

    Ok(())
}

#[allow(dead_code)]
/// Record a confirmed position in the positions table.
/// This is called after bundle landing is confirmed AND the on-chain balance
/// has been read back successfully.
pub fn record_position(
    token_mint: &str,
    bundle_id: &str,
    region: &str,
    slot: u64,
    transaction_signature: &str,
    amount_in_lamports: u64,
    acquired_amount_raw: u64,
    entry_price_num: u128,
    entry_price_den: u128,
    tip_lamports: u64,
    pool_id: &str,
) -> Result<(), String> {
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    let conn = lock
        .as_ref()
        .ok_or_else(|| "database is unavailable".to_string())?;

    let entry_price_num_i64 = i64::try_from(entry_price_num.min(i64::MAX as u128))
        .map_err(|_| "entry_price_num overflow".to_string())?;
    let entry_price_den_i64 = i64::try_from(entry_price_den.min(i64::MAX as u128))
        .map_err(|_| "entry_price_den overflow".to_string())?;

    let sql = "
        INSERT OR REPLACE INTO positions (
            token_mint, bundle_id, region, slot, transaction_signature,
            amount_in_lamports, acquired_amount_raw,
            entry_price_num, entry_price_den,
            tip_lamports, pool_id, status
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'active')
    ";
    conn.execute(
        sql,
        params![
            token_mint,
            bundle_id,
            region,
            slot,
            transaction_signature,
            amount_in_lamports,
            acquired_amount_raw,
            entry_price_num_i64,
            entry_price_den_i64,
            tip_lamports,
            pool_id,
        ],
    )
    .map_err(|e| format!("failed to record position: {e}"))?;

    // Also record in the legacy open_positions table for backward compat.
    insert_open_position(token_mint);

    Ok(())
}

/// Check whether the database connection is healthy and responsive.
/// Attempts to lock the global mutex and execute a simple SELECT.
/// Returns `false` if the mutex is poisoned, the connection is missing,
/// or the query fails.
pub fn check_db_healthy() -> bool {
    let lock = match DB_CONN.lock() {
        Ok(l) => l,
        Err(e) => {
            log::error!("[db] check_db_healthy: DB_CONN mutex poisoned: {e}");
            return false;
        }
    };
    match lock.as_ref() {
        Some(conn) => {
            // NOTE: must use query_row, not execute — `execute()` returns
            // Error::ExecuteReturnedResults for any statement with a
            // non-zero column count (i.e. any SELECT). That was the bug:
            // this probe failed every call regardless of actual DB health.
            match conn.query_row::<i64, _, _>("SELECT 1", [], |row| row.get(0)) {
                Ok(_) => true,
                Err(e) => {
                    log::error!("[db] check_db_healthy: liveness query failed: {e}");
                    false
                }
            }
        }
        None => {
            log::error!("[db] check_db_healthy: DB uninitialized (DB_CONN is None)");
            false
        }
    }
}

pub fn close_position(token_mint: &str) -> Result<(), String> {
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    let conn = lock
        .as_ref()
        .ok_or_else(|| "database is unavailable".to_string())?;

    let sql = "
        UPDATE positions
        SET status = 'closed', closed_at = CURRENT_TIMESTAMP
        WHERE token_mint = ?1 AND status = 'active'
    ";
    let rows = conn
        .execute(sql, params![token_mint])
        .map_err(|e| format!("failed to close position: {e}"))?;

    // Also remove from the legacy table.
    remove_open_position(token_mint);

    if rows == 0 {
        return Err(format!("no active position found for mint {token_mint}"));
    }
    Ok(())
}

#[allow(dead_code)]
/// Returns the list of currently active (open) positions.
pub fn get_active_positions() -> Vec<String> {
    let mut positions = Vec::new();
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(conn) = lock.as_ref() {
        let sql = "SELECT token_mint FROM positions WHERE status = 'active' ORDER BY opened_at ASC";
        if let Ok(mut stmt) = conn.prepare(sql) {
            if let Ok(rows) = stmt.query_map([], |row| row.get(0)) {
                for row in rows.flatten() {
                    positions.push(row);
                }
            }
        }
    }
    positions
}

#[allow(dead_code)]
/// Record a bundle as failed in the database.
pub fn record_failed_bundle(bundle_id: &str, reason: &str) -> Result<(), String> {
    let lock = DB_CONN.lock().unwrap_or_else(|e| e.into_inner());
    let conn = lock
        .as_ref()
        .ok_or_else(|| "database is unavailable".to_string())?;

    let sql = "
        UPDATE bundles
        SET status = 'failed', failure_reason = ?1
        WHERE bundle_id = ?2
    ";
    let rows = conn
        .execute(sql, params![reason, bundle_id])
        .map_err(|e| format!("failed to record failed bundle: {e}"))?;

    if rows == 0 {
        // Bundle wasn't in the table yet; insert a failed record.
        let insert_sql = "
            INSERT INTO bundles (bundle_id, status, failure_reason)
            VALUES (?1, 'failed', ?2)
        ";
        conn.execute(insert_sql, params![bundle_id, reason])
            .map_err(|e| format!("failed to insert failed bundle: {e}"))?;
    }
    Ok(())
}
