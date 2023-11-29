-- Clean DB
DROP TABLE IF EXISTS Peers,
Tasks,
Checks,
P2P,
Verter,
TransferredPoints,
Friends,
Recommendations,
XP,
TimeTracking;
-- Create the structure
CREATE TABLE IF NOT EXISTS Peers (
  Nickname VARCHAR PRIMARY KEY,
  Birthday DATE NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
  Title VARCHAR PRIMARY KEY,
  ParentTask VARCHAR,
  MaxXP INTEGER CHECK (maxxp >= 0),
  CONSTRAINT fk_tasks_parenttask FOREIGN KEY (ParentTask) REFERENCES Tasks (Title)
);
DO $ $ BEGIN IF NOT EXISTS (
  SELECT
    1
  FROM
    pg_type
  WHERE
    typname = 'enum_check_status'
) THEN CREATE TYPE enum_check_status AS ENUM ('Start', 'Success', 'Failure');
END IF;
END $ $;
CREATE TABLE IF NOT EXISTS Checks (
  ID serial PRIMARY KEY,
  Peer VARCHAR NOT NULL,
  Task VARCHAR NOT NULL,
  "Date" DATE NOT NULL DEFAULT CURRENT_DATE,
  CONSTRAINT fk_checks_peer FOREIGN key (Peer) REFERENCES peers (Nickname),
  CONSTRAINT fk_checks_task FOREIGN key (Task) REFERENCES tasks (Title)
);
CREATE TABLE IF NOT EXISTS P2P (
  ID serial PRIMARY KEY,
  "Check" INT NOT NULL,
  Checkingpeer VARCHAR NOT NULL,
  State enum_check_status NOT NULL,
  "Time" TIME WITHOUT TIME ZONE NOT NULL,
  CONSTRAINT fk_p2p_check FOREIGN key ("Check") REFERENCES Checks (ID),
  CONSTRAINT fk_p2p_checkingpeer FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname)
);
CREATE TABLE IF NOT EXISTS Verter (
  ID SERIAL PRIMARY KEY,
  "Check" INT NOT NULL,
  State enum_check_status NOT NULL,
  "Time" TIME WITHOUT TIME ZONE NOT NULL,
  CONSTRAINT fk_verter_check FOREIGN KEY ("Check") REFERENCES Checks (ID)
);
CREATE TABLE IF NOT EXISTS TransferredPoints (
  ID SERIAL PRIMARY KEY,
  CheckingPeer VARCHAR NOT NULL,
  CheckedPeer VARCHAR CHECK (CheckedPeer != CheckingPeer) NOT NULL,
  PointsAmount INT NOT NULL,
  CONSTRAINT fr_transferedpoints_checkingpeer FOREIGN KEY (CheckingPeer) REFERENCES Peers (Nickname),
  CONSTRAINT fr_transferedpoints_checkedpeer FOREIGN KEY (CheckedPeer) REFERENCES Peers (Nickname)
);
CREATE TABLE IF NOT EXISTS Friends (
  ID SERIAL PRIMARY KEY,
  Peer1 VARCHAR NOT NULL,
  Peer2 VARCHAR NOT NULL CHECK (Peer1 != Peer2),
  CONSTRAINT fk_friends_peer1 FOREIGN KEY (Peer1) REFERENCES Peers (Nickname),
  CONSTRAINT fk_friends_peer2 FOREIGN KEY (Peer2) REFERENCES Peers (Nickname)
);
CREATE TABLE IF NOT EXISTS Recommendations (
  ID SERIAL PRIMARY KEY,
  Peer VARCHAR NOT NULL,
  RecommendedPeer VARCHAR NOT NULL,
  CONSTRAINT fk_recommendations_peer FOREIGN KEY (Peer) REFERENCES Peers (Nickname),
  CONSTRAINT fk_recommendations_recommendedpeer FOREIGN KEY (RecommendedPeer) REFERENCES Peers (Nickname)
);
CREATE TABLE IF NOT EXISTS XP (
  ID SERIAL PRIMARY KEY,
  "Check" INT NOT NULL,
  XPAmount INT CHECK (XPAmount >= 0),
  CONSTRAINT fk_xp_check FOREIGN KEY ("Check") REFERENCES Checks (ID)
);
CREATE TABLE IF NOT EXISTS TimeTracking (
  ID SERIAL PRIMARY KEY,
  Peer VARCHAR NOT NULL,
  "Date" date NOT NULL,
  "Time" TIME WITHOUT TIME ZONE,
  State INT CHECK (State in (1, 2)),
  FOREIGN KEY (Peer) REFERENCES Peers (Nickname)
);
-- Import
SET
  import_path.const TO '/tmp/import/';
DROP PROCEDURE IF EXISTS import_from_csv;
CREATE
OR REPLACE PROCEDURE import_from_csv(
  IN table_name TEXT,
  IN file_name TEXT,
  IN delimiter TEXT
) AS $ $ BEGIN EXECUTE format(
  'COPY %s FROM %L WITH CSV DELIMITER %L HEADER;',
  $ 1,
  current_setting('import_path.const') || $ 2,
  $ 3
);
END;
$ $ LANGUAGE plpgsql;
CALL import_from_csv('peers', 'peers.csv', ',');
CALL import_from_csv('tasks', 'tasks.csv', ',');
CALL import_from_csv('checks', 'checks.csv', ',');
CALL import_from_csv('friends', 'friends.csv', ',');
CALL import_from_csv('p2p', 'p2p.csv', ',');
CALL import_from_csv('recommendations', 'recommendations.csv', ',');
CALL import_from_csv('timetracking', 'timetracking.csv', ',');
CALL import_from_csv('transferredpoints', 'transferredpoints.csv', ',');
CALL import_from_csv('verter', 'verter.csv', ',');
CALL import_from_csv('xp', 'xp.csv', ',');
-- Export
SET
  export_path.const TO '/tmp/export/';
DROP PROCEDURE IF EXISTS export_to_csv;
CREATE
OR REPLACE PROCEDURE export_to_csv(
  IN table_name text,
  IN file_name text,
  IN delimeter text
) AS $ $ BEGIN EXECUTE format(
  'COPY %s TO %L DELIMITER ''%s'' CSV HEADER;',
  $ 1,
  current_setting('export_path.const') || $ 2,
  $ 3
);
END;
$ $ LANGUAGE plpgsql;
CALL export_to_csv('peers', 'peers.csv', ',');
CALL export_to_csv('tasks', 'tasks.csv', ',');
CALL export_to_csv('checks', 'checks.csv', ',');
CALL export_to_csv('friends', 'friends.csv', ',');
CALL export_to_csv('p2p', 'p2p.csv', ',');
CALL export_to_csv('recommendations', 'recommendations.csv', ',');
CALL export_to_csv('timetracking', 'timetracking.csv', ',');
CALL export_to_csv('transferredpoints', 'transferredpoints.csv', ',');
CALL export_to_csv('verter', 'verter.csv', ',');
CALL export_to_csv('xp', 'xp.csv', ',');
-- Clean tables
TRUNCATE TABLE Peers,
Tasks,
Checks,
P2P,
Verter,
TransferredPoints,
Friends,
Recommendations,
XP,
TimeTracking;
ALTER SEQUENCE public.checks_id_seq MINVALUE 0 RESTART WITH 0 INCREMENT BY 1;
-- Fill tables out
INSERT INTO
  Peers (Nickname, Birthday)
VALUES
  ('Alex', '1986-04-30'),
  ('Yan', '2000-02-29'),
  ('Oleg', '1999-12-21'),
  ('Maria', '1995-10-20'),
  ('Eugeny', '1940-12-31'),
  ('Sasha', '1996-03-03'),
  ('Yana', '1960-06-26'),
  ('Daria', '2015-07-27');
INSERT INTO
  Tasks (Title, ParentTask, MaxXP)
VALUES
  ('C2_SimpleBashUtils', NULL, 250),
  ('C3_s21_string+', 'C2_SimpleBashUtils', 500),
  ('C5_s21_decimal', 'C3_s21_string+', 350),
  ('C6_s21_matrix', 'C5_s21_decimal', 200),
  ('C7_SmartCalc_v1.0', 'C6_s21_matrix', 500),
  ('C8_3DViewer_v1.0', 'C7_SmartCalc_v1.0', 750),
  ('CPP1_s21_matrix+', 'C8_3DViewer_v1.0', 300),
  ('CPP2_s21_containers', 'CPP1_s21_matrix+', 350),
  ('D01_Linux', 'C2_SimpleBashUtils', 300),
  ('DO2_Linux_Network', 'D01_Linux', 250),
  ('DO3_Linux_Monitoring', 'DO2_Linux_Network', 350),
  ('DO5_SimpleDocker', 'DO3_Linux_Monitoring', 300),
  ('DO6_CI/CD', 'DO5_SimpleDocker', 300);
INSERT INTO
  Checks (Peer, Task, "Date")
VALUES
  ('Yan', 'C2_SimpleBashUtils', '2022-09-01'),
  ('Oleg', 'C2_SimpleBashUtils', '2022-9-03'),
  ('Maria', 'C2_SimpleBashUtils', '2022-09-04'),
  ('Eugeny', 'C2_SimpleBashUtils', '2022-09-05'),
  ('Alex', 'C3_s21_string+', '2022-09-15'),
  ('Yan', 'C3_s21_string+', '2022-09-15'),
  ('Daria', 'C2_SimpleBashUtils', '2022-09-15'),
  ('Alex', 'C5_s21_decimal', '2022-09-25'),
  ('Alex', 'C6_s21_matrix', '2022-09-26'),
  ('Alex', 'C7_SmartCalc_v1.0', '2022-10-01'),
  ('Alex', 'C8_3DViewer_v1.0', '2022-10-10'),
  ('Maria', 'C2_SimpleBashUtils', '2022-10-20'),
  ('Alex', 'C2_SimpleBashUtils', '2022-08-30');
INSERT INTO
  P2P ("Check", CheckingPeer, State, "Time")
VALUES
  (0, 'Yan', 'Start', '13:00'),
  (0, 'Yan', 'Success', '13:30'),
  (1, 'Oleg', 'Start', '15:00'),
  (1, 'Oleg', 'Success', '15:30'),
  (2, 'Maria', 'Start', '19:00'),
  (2, 'Maria', 'Success', '19:30'),
  (3, 'Alex', 'Start', '11:00'),
  (3, 'Alex', 'Failure', '11:30'),
  (4, 'Daria', 'Start', '10:00'),
  (4, 'Daria', 'Success', '11:00'),
  (5, 'Yana', 'Start', '20:25'),
  (5, 'Yana', 'Success', '21:00'),
  (6, 'Alex', 'Start', '10:10'),
  (6, 'Alex', 'Success', '10:40'),
  (7, 'Eugeny', 'Start', '12:15'),
  (7, 'Eugeny', 'Success', '12:30'),
  (8, 'Yana', 'Start', '2:00'),
  (8, 'Yana', 'Success', '2:30'),
  (9, 'Yan', 'Start', '15:00'),
  (9, 'Yan', 'Success', '15:30'),
  (10, 'Maria', 'Start', '16:00'),
  (10, 'Maria', 'Success', '16:50'),
  (11, 'Daria', 'Start', '10:00'),
  (11, 'Daria', 'Success', '11:00'),
  (12, 'Yana', 'Start', '14:00'),
  (12, 'Yana', 'Success', '14:30');
INSERT INTO
  Verter ("Check", State, "Time")
VALUES
  (0, 'Start', '12:31'),
  (0, 'Success', '12:35'),
  (1, 'Start', '15:31'),
  (1, 'Success', '15:35'),
  (2, 'Start', '19:31'),
  (2, 'Failure', '19:33'),
  (4, 'Start', '11:32'),
  (4, 'Success', '11:40'),
  (5, 'Start', '21:02'),
  (5, 'Success', '21:10'),
  (6, 'Start', '10:41'),
  (6, 'Success', '10:45'),
  (7, 'Start', '12:31'),
  (7, 'Success', '12:33'),
  (8, 'Start', '18:31'),
  (8, 'Success', '18:33'),
  (9, 'Start', '15:31'),
  (9, 'Success', '15:33'),
  (12, 'Start', '14:30'),
  (12, 'Failure', '14:33');
INSERT INTO
  TransferredPoints (CheckingPeer, CheckedPeer, PointsAmount)
VALUES
  ('Yan', 'Alex', 1),
  ('Alex', 'Yan', 1),
  ('Oleg', 'Yan', 1),
  ('Maria', 'Oleg', 1),
  ('Alex', 'Maria', 1),
  ('Daria', 'Eugeny', 1),
  ('Yana', 'Alex', 1),
  ('Alex', 'Daria', 1),
  ('Daria', 'Alex', 1),
  ('Eugeny', 'Daria', 1),
  ('Yan', 'Oleg', 1),
  ('Oleg', 'Maria', 1),
  ('Alex', 'Yana', 1),
  ('Maria', 'Yana', 1);
INSERT INTO
  Friends (Peer1, Peer2)
VALUES
  ('Alex', 'Yan'),
  ('Alex', 'Eugeny'),
  ('Yana', 'Eugeny'),
  ('Sasha', 'Yan'),
  ('Daria', 'Sasha');
INSERT INTO
  Recommendations (Peer, RecommendedPeer)
VALUES
  ('Alex', 'Yan'),
  ('Yan', 'Alex'),
  ('Yan', 'Oleg'),
  ('Oleg', 'Maria'),
  ('Maria', 'Alex'),
  ('Eugeny', 'Daria'),
  ('Alex', 'Yana'),
  ('Daria', 'Alex'),
  ('Alex', 'Daria'),
  ('Daria', 'Eugeny'),
  ('Oleg', 'Yan'),
  ('Maria', 'Oleg'),
  ('Yana', 'Alex');
INSERT INTO
  XP ("Check", XPAmount)
VALUES
  (0, 250),
  (1, 250),
  (4, 250),
  (5, 500),
  (6, 500),
  (7, 250),
  (8, 350),
  (9, 200),
  (10, 500),
  (11, 750);
INSERT INTO
  TimeTracking (Peer, "Date", "Time", State)
VALUES
  ('Oleg', '2022-10-09', '18:32', 1),
  ('Oleg', '2022-10-09', '19:32', 2),
  ('Oleg', '2022-10-09', '20:32', 1),
  ('Oleg', '2022-10-09', '22:32', 2),
  ('Maria', '2022-10-09', '10:32', 1),
  ('Maria', '2022-10-09', '12:32', 2),
  ('Maria', '2022-10-09', '13:02', 1),
  ('Maria', '2022-10-09', '21:32', 2),
  ('Eugeny', '2022-05-09', '10:32', 1),
  ('Eugeny', '2022-05-09', '12:32', 2),
  ('Sasha', '2022-06-09', '11:02', 1),
  ('Sasha', '2022-06-09', '21:32', 2),
  ('Alex', '2022-09-21', '15:00', 1),
  ('Alex', '2022-09-21', '22:00', 2),
  ('Yan', '2022-09-21', '08:00', 1),
  ('Yan', '2022-09-21', '20:00', 2),
  ('Maria', '2022-09-21', '12:00', 1),
  ('Maria', '2022-09-21', '19:00', 2);