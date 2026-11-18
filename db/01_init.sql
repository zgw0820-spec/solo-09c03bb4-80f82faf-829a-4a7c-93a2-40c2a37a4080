-- 预置机房设备故障日志与只读查询角色。
-- 由 docker-entrypoint-initdb.d 在数据库首次初始化时执行一次。

CREATE TABLE IF NOT EXISTS fault_logs (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    device_id    TEXT        NOT NULL,
    site         TEXT        NOT NULL,
    severity     TEXT        NOT NULL CHECK (severity IN ('INFO','WARN','CRITICAL')),
    fault_code   TEXT        NOT NULL,
    message      TEXT        NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO fault_logs (device_id, site, severity, fault_code, message, occurred_at)
SELECT * FROM (VALUES
 ('SW-CORE-01', '北京-A区', 'CRITICAL', 'PORT_DOWN',       '核心交换机上联端口中断',           now() - interval '3 hours'),
 ('UPS-02',    '北京-A区', 'WARN',     'BATTERY_LOW',     'UPS 电池电量低于 30%',            now() - interval '2 hours'),
 ('AC-11',     '上海-B区', 'WARN',     'TEMP_HIGH',       '精密空调回风温度超阈值',           now() - interval '90 minutes'),
 ('SRV-DB-07', '上海-B区', 'CRITICAL', 'DISK_FAILURE',    '数据库节点磁盘 SMART 故障',        now() - interval '45 minutes'),
 ('FW-EDGE-03','广州-C区', 'INFO',     'SESSION_FLOOD',   '边界防火墙拦截异常会话洪泛',       now() - interval '20 minutes'),
 ('PDU-04',    '广州-C区', 'WARN',     'PHASE_IMBALANCE','PDU 三相负载不均衡',              now() - interval '8 minutes'),
 ('SW-TOR-22', '北京-A区', 'INFO',     'FLAP',            '机柜顶交换机端口抖动后恢复',       now() - interval '2 minutes')
) AS v(device_id, site, severity, fault_code, message, occurred_at)
WHERE NOT EXISTS (SELECT 1 FROM fault_logs);

-- 分析员只读角色：只有 SELECT 权限，且不能建表/写临时对象外的任何数据。
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fault_ro') THEN
        CREATE ROLE fault_ro LOGIN;
    END IF;
END
$$;
ALTER ROLE fault_ro PASSWORD 'fault_ro';
-- Defence in depth: every session opened by this role defaults to read-only,
-- even a direct connection that bypasses the application pool.
ALTER ROLE fault_ro SET default_transaction_read_only = on;

REVOKE ALL ON SCHEMA public FROM fault_ro;
GRANT USAGE ON SCHEMA public TO fault_ro;
GRANT SELECT ON fault_logs TO fault_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM fault_ro;

-- 保留期清理专用的可写角色。它与只读查询角色 fault_ro 完全分开：
-- 应用仅用独立的维护连接以该角色登录，两条只读查询连接不受影响。
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fault_maint') THEN
        CREATE ROLE fault_maint LOGIN;
    END IF;
END
$$;
ALTER ROLE fault_maint PASSWORD 'fault_maint';
-- 明确不设置 default_transaction_read_only，保证该角色可以执行 DELETE。
ALTER ROLE fault_maint RESET default_transaction_read_only;

-- 清理进度与步骤回执的持久化表（应用启动时也会 CREATE IF NOT EXISTS，
-- 这里建表以便在初始化阶段一次性完成授权）。
CREATE TABLE IF NOT EXISTS retention_tasks (
    task_id          TEXT PRIMARY KEY,
    cutoff_at        TIMESTAMPTZ NOT NULL,
    next_step_no     INTEGER NOT NULL DEFAULT 1,
    total_deleted    BIGINT NOT NULL DEFAULT 0,
    completed        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS retention_step_receipts (
    task_id          TEXT NOT NULL,
    step_no          INTEGER NOT NULL,
    cutoff_at        TIMESTAMPTZ NOT NULL,
    step_deleted     BIGINT NOT NULL,
    total_deleted    BIGINT NOT NULL,
    completed        BOOLEAN NOT NULL,
    retried          BOOLEAN NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, step_no),
    FOREIGN KEY (task_id) REFERENCES retention_tasks(task_id)
);

-- 最小够用权限：只能删除/查询故障日志，并维护两张进度表；
-- 碰不到序列、函数或其它对象，也没有 fault_logs 的 INSERT/UPDATE/TRUNCATE。
-- 授予 schema CREATE 仅用于应用启动时 CREATE TABLE IF NOT EXISTS 自愈；
-- 初始化脚本已建好两张表，正常运行不会再建任何对象。
REVOKE ALL ON SCHEMA public FROM fault_maint;
GRANT USAGE, CREATE ON SCHEMA public TO fault_maint;
GRANT SELECT, DELETE ON fault_logs TO fault_maint;
GRANT SELECT, INSERT, UPDATE ON retention_tasks TO fault_maint;
GRANT SELECT, INSERT, UPDATE ON retention_step_receipts TO fault_maint;
-- 若自愈逻辑真的新建了表，默认只给该角色读写自己的表。
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE ON TABLES TO fault_maint;
