ALTER TABLE `user_active_log` ADD UNIQUE (
                                   `user_id`,
                                   `device_id`
)