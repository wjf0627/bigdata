CREATE KEYSPACE IF NOT EXISTS dmp WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3 };

CREATE TABLE dmp.device_target(
  device_id text PRIMARY KEY,
  age text,
  gender text,
  interest text,
  install_apps set<text>,
  target_offer_list set<text>,
  behavior set<text>
);

BEGIN BATCH
INSERT INTO dmp.device_target (device_id, age, gender) VALUES  ('6d5f1663-89c0-45fc-8cfd-60a373b01622','0~16', 'f');
INSERT INTO dmp.device_target (device_id, age, gender) VALUES  ('38ab64b6-26cc-4de9-ab28-c257cf011659','17~24', 'm');
INSERT INTO dmp.device_target (device_id, age, gender) VALUES  ('9011d3be-d35c-4a8d-83f7-a3c543789ee7','25~40', 'm');
INSERT INTO dmp.device_target (device_id, age, gender) VALUES  ('95addc4c-459e-4ed7-b4b5-472f19a67995','40+', 'f');
INSERT INTO dmp.device_target (device_id, age, gender) VALUES  ('08d52dbe-6ef5-4ed7-a891-c5b5d40adcdd','25~40', 'f');
INSERT INTO dmp.device_target (device_id, age, gender) VALUES  ('3d31f068-81bb-4be6-9c94-d299881238d2','17~24', 'm');
INSERT INTO dmp.device_target (device_id, age, gender) VALUES  ('8a977b3a-d20e-4742-a492-6c4229be785c','25~40', 'm');
INSERT INTO dmp.device_target (device_id, age, gender) VALUES  ('b886bc24-7393-450b-8f2f-cc246db3c1c1','40+', 'f');
INSERT INTO dmp.device_target (device_id, age, gender) VALUES  ('c3cf6667-c3ad-45c0-8792-47e864ebab04','0~16', 'm');
APPLY BATCH;

BEGIN BATCH
UPDATE dmp.device_target SET target_offer_list = target_offer_list + {'1601','1624'} WHERE device_id = '95addc4c-459e-4ed7-b4b5-472f19a67995';
UPDATE dmp.device_target SET target_offer_list = target_offer_list + {'1605','1624'} WHERE device_id = 'fcccbdc2-78f9-45c8-8398-8727d77f4a2b';
UPDATE dmp.device_target SET target_offer_list = target_offer_list + {'1605','1624'} WHERE device_id = 'b61a7892-d097-4643-8b8b-9137e8709b6d';
UPDATE dmp.device_target SET target_offer_list = target_offer_list + {'1605','1624'} WHERE device_id = 'c265aa81-722e-47f6-bae3-9569e23d9848';
UPDATE dmp.device_target SET target_offer_list = target_offer_list + {'1605','1624'} WHERE device_id = 'a7500500-b5ae-4b8e-8daa-e84d865fe5dd';
UPDATE dmp.device_target SET target_offer_list = target_offer_list + {'1605','1624'} WHERE device_id = 'd2249356-e8fb-4071-a6f3-a6c0d904dc2f';
UPDATE dmp.device_target SET target_offer_list = target_offer_list + {'1605','1624'} WHERE device_id = '40d878fa-5663-4cd3-8101-e89b0c0faa78';
UPDATE dmp.device_target SET target_offer_list = target_offer_list + {'1605','1624'} WHERE device_id = 'd69fffa1-24de-4fa5-b93b-387220f7062d';
UPDATE dmp.device_target SET target_offer_list = target_offer_list + {'1605','1624'} WHERE device_id = '1d7646e7-4adb-4109-8900-d23a806f841e';
UPDATE dmp.device_target SET target_offer_list = target_offer_list + {'1605','1624'} WHERE device_id = 'd22efd96-6782-45a0-b9e4-b3b74b50e6e4';
APPLY BATCH;

UPDATE dmp.device_target SET target_offer_list = target_offer_list - {'1605'} WHERE target_offer_list CONTAINS '1605';

CREATE KEYSPACE IF NOT EXISTS dmp WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3 };

ALTER KEYSPACE dmp WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 2};

ALTER KEYSPACE dmp WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE test.device(
  device_id text PRIMARY KEY
);

CREATE INDEX target_offer ON dmp.device_target(target_offer_list);

CREATE KEYSPACE IF NOT EXISTS cycling WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TABLE cycling.route (race_id int, race_name text, point_id int, lat_long tuple<text, tuple<float,float>>, PRIMARY KEY (race_id, point_id));



CREATE KEYSPACE dmp_realtime_service WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};

CREATE KEYSPACE test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};

CREATE TYPE test.address (city text, street text, number int);
CREATE TABLE test.companies (name text PRIMARY KEY, address SET<FROZEN<address>>);

CREATE TABLE dmp_realtime_service.dmp_user_features
 (
    device_id text PRIMARY KEY,
    age text ,
    gender text,
    interest set<text>,
    frequency map<text,int>,
    install_apps set<text>,
    target_offer_list set<bigint>
);

CREATE TABLE IF NOT EXISTS flink.batches_tuple (id text PRIMARY KEY, counter int, batch_id int, val tuple<text,text>);
CREATE TABLE IF NOT EXISTS flink.batches (id text PRIMARY KEY, counter int, batch_id int);

CREATE TABLE IF NOT EXISTS test.message_tuple
    (
        body text PRIMARY KEY,
        tuple_value text
    )

alter table dmp_realtime_service.dmp_user_features rename dmp_user_features to dmp_user_features_v1;


CREATE TABLE dmp_realtime_service.dmp_user_feature (
    device_id text PRIMARY KEY,
    age int,
    gender int,
    frequency frozen<set<struct>>,
    install_apps set<text>,
    interest set<text>,
    target_offer_list set<bigint>
);

CREATE TYPE dmp_realtime_service.struct (tag text, cnt text);

insert into dmp_realtime_service.dmp_user_features2 (device_id,frequency) values('123456',{('abc',1),('erf',3)});

insert into dmp_realtime_service.dmp_user_features1 (device_id,frequency) values('123456',{('abc',1),('erf',3)});

insert into dmp_realtime_service.dmp_user_features4 (device_id,install_apps) values('123456',FFF801C0-9BA6-46B7-A442-C24408F1759B,3,1,['com.smallgiantgames.empires', 'com.psiphon3.subscription']);

ALTER TABLE dmp_realtime_service.dmp_user_features ALTER age TYPE int;

insert into dmp_realtime_service.dmp_user_features (device_id, age, gender, interest, frequency, install_apps, target_offer_list)
    values('d771178e-acc1-4157-a032-6b7e7e218cc3',1,2,'22,36',{('22',1),('36',6)},'120,512',{2,25,121});


CREATE TABLE wangjf.flink_test (device_id String,status UInt64)ENGINE = Log;

CREATE TABLE wangjf.flink_test (device_id String,dt DATE,flag INT) ENGINE = Log;


CREATE TABLE kafka_demo (timestamp UInt64,level String, message String) ENGINE = Kafka('localhost:9092', 'kafka_topic', 'kafka_group', 'JSONEachRow');

CREATE TABLE kafka_demo1 (timestamp UInt64,level String, message String) ENGINE = Kafka SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 'kafka_topic1', kafka_group_name = 'kafka_group1', kafka_format = 'JSONEachRow', kafka_num_consumers = 1;

CREATE TABLE kafka_proto (userId String,level UInt64,score UInt64, timestamp String) ENGINE = Kafka SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 'kafka_proto', kafka_group_name = 'kafka_group', kafka_format = 'Protobuf', kafka_num_consumers = 1;
