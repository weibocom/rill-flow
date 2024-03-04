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
    `type` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'template type: 0. function, 1. plugin, 2. logic',
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Rill Flow task template table';

grant all on *.* to 'root'@'%' identified by 'secret';
flush privileges;

DELETE FROM task_template where `name` = 'aliyun-ai-module';
INSERT INTO task_template (`name`, `type`, `category`, `icon`, `task_yaml`, `schema`, `output`, `enable`, `create_time`, `update_time`)
VALUES ('aliyun-ai-module', 1, 'function', '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', 'resourceName: "aliyun_ai://aliyun"\nresourceProtocal: "aliyun_ai"\nname: "aliyunAiTemplate"\ncategory: "function"\npattern: "task_sync"', '{"type":"object","required":["apikey","prompt"],"properties":{"apikey":{"type":"string","title":"apikey"},"model":{"type":"string","title":"model"},"prompt":{"type":"string","title":"prompt"},"message_suffix":{"type":"string","title":"suffix message"},"message_prefix":{"type":"string","title":"prefix message"}}}', '{"type":"object","properties":{"ouput":{"type":"object","title":"output","properties":{"text":{"type":"string","title":"text"}}},"requestId":{"type":"string","title":"requestId"}}}', 1, now(), now());
