--2
CREATE TABLE IF NOT EXISTS stv2025061616__staging.group_log (
    group_id INTEGER,
    user_id INTEGER,
    user_id_from INTEGER,
    event VARCHAR(20),
    datetime TIMESTAMP
);

--4
CREATE TABLE IF not EXISTS stv2025061616__DWH.l_user_group_activity (
    hk_l_user_group_activity BIGINT NOT NULL PRIMARY KEY,
    hk_user_id BIGINT NOT NULL CONSTRAINT fk_l_user_group_activity_user REFERENCES stv2025061616__DWH.h_users (hk_user_id),
    hk_group_id BIGINT NOT NULL CONSTRAINT fk_l_user_group_activity_group REFERENCES stv2025061616__DWH.h_groups (hk_group_id),
    load_dt DATETIME NOT NULL,
    load_src VARCHAR(20) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);



--5
INSERT INTO stv2025061616__DWH.l_user_group_activity (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT
    HASH(hu.hk_user_id, hg.hk_group_id) AS hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    NOW() AS load_dt,
    's3' AS load_src
FROM stv2025061616__STAGING.group_log AS gl
LEFT JOIN stv2025061616__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN stv2025061616__DWH.h_groups AS hg ON gl.group_id = hg.group_id
WHERE 
    hu.hk_user_id IS NOT NULL
    AND hg.hk_group_id IS NOT NULL;


--6
CREATE TABLE stv2025061616__DWH.s_auth_history (
    hk_l_user_group_activity BIGINT NOT NULL CONSTRAINT fk_s_auth_history_l_user_group_activity 
                             REFERENCES stv2025061616__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from INTEGER,
    event VARCHAR(20) NOT NULL,
    event_dt TIMESTAMP NOT NULL,
    load_dt DATETIME NOT NULL,
    load_src VARCHAR(20) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


INSERT INTO stv2025061616__DWH.s_auth_history (
    hk_l_user_group_activity,
    user_id_from,
    event,
    event_dt,
    load_dt,
    load_src
)
SELECT DISTINCT
    luga.hk_l_user_group_activity,
    gl.user_id_from,
    gl.event,
    gl.datetime AS event_dt,
    NOW() AS load_dt,
    's3' AS load_src
FROM stv2025061616__STAGING.group_log AS gl
LEFT JOIN stv2025061616__DWH.h_groups AS hg ON gl.group_id = hg.group_id
LEFT JOIN stv2025061616__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN stv2025061616__DWH.l_user_group_activity AS luga 
    ON hg.hk_group_id = luga.hk_group_id 
    AND hu.hk_user_id = luga.hk_user_id
WHERE 
    luga.hk_l_user_group_activity IS NOT NULL
    AND gl.event IN ('add', 'create', 'leave');

