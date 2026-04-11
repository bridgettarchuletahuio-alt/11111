-- 用户表，支持用户名+密码，首个注册用户为管理员
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user', -- 'admin' 或 'user'
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS link_sets (
    id TEXT PRIMARY KEY,
    name TEXT DEFAULT '',
    links_json TEXT NOT NULL,
    current_index INTEGER NOT NULL DEFAULT 0,
    click_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
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

CREATE INDEX IF NOT EXISTS idx_click_logs_set_id ON click_logs(set_id);
CREATE INDEX IF NOT EXISTS idx_click_logs_clicked_at ON click_logs(clicked_at);