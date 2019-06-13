CREATE TABLE IF NOT EXISTS `user_info_log` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `user_id` bigint NOT NULL,
    `auth_type` int(10) NOT NULL DEFAULT 0 comment '0未认证|1身份证认证|2护照认证',
    `auth_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `online_status` int(10) NOT NULL DEFAULT 0 comment '0不在线|1在线',
    `active_status` int(10) NOT NULL DEFAULT 0 comment '0今日无活跃|1今日活跃',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `user_active_log` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `user_id` bigint NOT NULL,
    `device_id` varchar NOT NULL,
    `login_time` timestamp NULL,
    `logout_time` timestamp NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
