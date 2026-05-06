-- 用户表，支持用户名+密码，首个注册用户为管理员
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user', -- 'admin' 或 'user'
    is_authorized INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS link_sets (
    id TEXT PRIMARY KEY,
    name TEXT DEFAULT '',
    links_json TEXT NOT NULL,
    current_index INTEGER NOT NULL DEFAULT 0,
    click_count INTEGER NOT NULL DEFAULT 0,
    user_id INTEGER,
    user_remark TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS click_logs (
    log_id TEXT PRIMARY KEY,
    set_id TEXT NOT NULL,
    link_index INTEGER NOT NULL,
    url TEXT NOT NULL,
    clicked_at TEXT NOT NULL,
    ua TEXT DEFAULT '',
    ref TEXT DEFAULT '',
    ip_hash TEXT DEFAULT '',
    FOREIGN KEY (set_id) REFERENCES link_sets(id)
);

CREATE TABLE IF NOT EXISTS ip_assignments (
    set_id TEXT NOT NULL,
    ip_hash TEXT NOT NULL,
    link_index INTEGER NOT NULL,
    url TEXT NOT NULL,
    assigned_at TEXT NOT NULL,
    last_clicked_at TEXT NOT NULL,
    PRIMARY KEY (set_id, ip_hash),
    FOREIGN KEY (set_id) REFERENCES link_sets(id)
);

CREATE INDEX IF NOT EXISTS idx_click_logs_set_id ON click_logs(set_id);
CREATE INDEX IF NOT EXISTS idx_click_logs_clicked_at ON click_logs(clicked_at);
CREATE INDEX IF NOT EXISTS idx_click_logs_set_ip_hash_clicked_at ON click_logs(set_id, ip_hash, clicked_at);
CREATE INDEX IF NOT EXISTS idx_ip_assignments_set_last_clicked_at ON ip_assignments(set_id, last_clicked_at);