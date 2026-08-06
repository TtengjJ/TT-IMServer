-- 建表
CREATE TABLE IF NOT EXISTS `offline_msg`
(
    `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
    `message_id` BIGINT NOT NULL,
    `receive_user_id` BIGINT NOT NULL,
    `content` VARCHAR(2000),
    `message_type` INT,
    `status` TINYINT COMMENT '0: PENDING, 1: SENT, 2: ACKED, 3: FAILED',
    `retry_count` INT NOT NULL DEFAULT 0,
    `created_at` DATETIME,
    `properties` VARCHAR(2000),
    INDEX `idx_receive_user_id` (`receive_user_id`),
    INDEX `idx_message_id` (`message_id`)
);
CREATE TABLE offline_msg_last_send_record
(
    receive_user_id BIGINT NOT NULL PRIMARY KEY,
    last_message_id BIGINT,
    updated_at      DATETIME
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
