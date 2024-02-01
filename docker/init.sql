CREATE DATABASE IF NOT EXISTS rill_flow;
USE rill_flow;
CREATE TABLE IF NOT EXISTS `task_template` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `name` varchar(64) NOT NULL DEFAULT '' COMMENT '模板名称',
    `type` tinyint(1) NOT NULL DEFAULT 0 COMMENT '模板类型，0. 函数模板，1. 插件模板，2. 逻辑模板',
    `category` varchar(64) NOT NULL DEFAULT '' COMMENT '模板类别，如 function、foreach 等',
    `icon` TEXT NOT NULL COMMENT 'base64 编码的 icon',
    `task_yaml` TEXT NOT NULL COMMENT '默认填充的任务 yaml 数据',
    `schema` TEXT NOT NULL COMMENT '用来渲染的 json schema',
    `output` TEXT NOT NULL COMMENT '用户输入的 output 字段',
    `enable` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime NOT NULL DEFAULT '1970-01-01 08:00:00' COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `name` (`name`),
    KEY `idx_type_category` (`type`, `category`),
    KEY `idx_update_time` (`update_time`),
    KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COMMENT='Rill Flow 任务模板表';

grant all on *.* to 'root'@'%' identified by 'secret';
flush privileges;