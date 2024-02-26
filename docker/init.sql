/*
 *  Copyright 2021-2023 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

CREATE DATABASE IF NOT EXISTS rill_flow;
USE rill_flow;
CREATE TABLE IF NOT EXISTS `task_template` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `name` varchar(64) NOT NULL DEFAULT '' COMMENT 'template name',
    `type` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'template type: 0. function，1. plugin，2. logic',
    `category` varchar(64) NOT NULL DEFAULT '' COMMENT 'template category: function, foreach, etc.',
    `icon` TEXT NOT NULL COMMENT 'icon base64 string',
    `task_yaml` TEXT NOT NULL COMMENT 'default task yaml configurations in dag',
    `schema` TEXT NOT NULL COMMENT 'json schema for input',
    `output` TEXT NOT NULL COMMENT 'json schema to describe the output of the task',
    `enable` tinyint(1) NOT NULL DEFAULT 1 COMMENT 'whether it is enabled: 0. disabled, 1. enabled',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time of this record',
    `update_time` datetime NOT NULL DEFAULT '1970-01-01 08:00:00' COMMENT 'newly update time of this record',
    PRIMARY KEY (`id`),
    UNIQUE KEY `name` (`name`),
    KEY `idx_type_category` (`type`, `category`),
    KEY `idx_update_time` (`update_time`),
    KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COMMENT='Rill Flow task template table';

grant all on *.* to 'root'@'%' identified by 'secret';
flush privileges;