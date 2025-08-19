--7.1
WITH user_group_messages AS (
    SELECT 
        lgd.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM stv2025061616__DWH.l_groups_dialogs lgd
    LEFT JOIN stv2025061616__DWH.l_user_message lum 
        ON lgd.hk_message_id = lum.hk_message_id
    WHERE lum.hk_user_id IS NOT NULL
    GROUP BY lgd.hk_group_id
)
SELECT 
    hk_group_id,
    cnt_users_in_group_with_messages
FROM user_group_messages
ORDER BY cnt_users_in_group_with_messages
limit 10;


--7.2
WITH user_group_log AS (
    SELECT 
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM stv2025061616__DWH.s_auth_history sah
    JOIN stv2025061616__DWH.l_user_group_activity luga 
        ON sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    JOIN stv2025061616__DWH.h_groups hg 
        ON luga.hk_group_id = hg.hk_group_id
    WHERE 
        sah.event = 'add'
        AND hg.hk_group_id IN (
            -- 10 самых старых групп по registration_dt
            SELECT hk_group_id
            FROM stv2025061616__DWH.h_groups
            ORDER BY registration_dt, group_id  -- добавлен group_id для стабильности
            LIMIT 10
        )
    GROUP BY luga.hk_group_id
)
SELECT 
    hk_group_id,
    cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users
LIMIT 10;

--7.3
WITH user_group_log AS (
    -- Количество пользователей, вступивших в группу (event = 'add')
    SELECT 
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM stv2025061616__DWH.s_auth_history sah
    JOIN stv2025061616__DWH.l_user_group_activity luga 
        ON sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    JOIN stv2025061616__DWH.h_groups hg 
        ON luga.hk_group_id = hg.hk_group_id
    WHERE 
        sah.event = 'add'
        AND hg.hk_group_id IN (
            -- 10 самых старых групп по registration_dt
            SELECT hk_group_id
            FROM stv2025061616__DWH.h_groups
            ORDER BY registration_dt, group_id
            LIMIT 10
        )
    GROUP BY luga.hk_group_id
),
user_group_messages AS (
    -- Количество пользователей, написавших хотя бы одно сообщение в группе
    SELECT 
        lgd.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM stv2025061616__DWH.l_groups_dialogs lgd
    JOIN stv2025061616__DWH.l_user_message lum 
        ON lgd.hk_message_id = lum.hk_message_id
    WHERE lum.hk_user_id IS NOT NULL
    GROUP BY lgd.hk_group_id
)
SELECT 
    ugl.hk_group_id,
    ugl.cnt_added_users,
    COALESCE(ugm.cnt_users_in_group_with_messages, 0) AS cnt_users_in_group_with_messages,
    ROUND(
        COALESCE(ugm.cnt_users_in_group_with_messages, 0)::DECIMAL / NULLIF(ugl.cnt_added_users, 0),
        4
    ) AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm 
    ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC;