-- 1
CREATE
OR REPLACE PROCEDURE p2p_check(
  IN checked VARCHAR,
  IN checking VARCHAR,
  IN taskName VARCHAR,
  IN state enum_check_status,
  IN P2Ptime TIME
) AS $ $ DECLARE id_check INTEGER := 0;
BEGIN IF state = 'Start' THEN
INSERT INTO
  checks (peer, task)
VALUES
  (checked, taskName) RETURNING id INTO id_check;
ELSE id_check = (
  SELECT
    c.id
  FROM
    p2p p
    INNER JOIN checks c ON c.id = p."Check"
  WHERE
    checkingpeer = checking
    AND peer = checked
    AND task = taskName
  ORDER BY
    c.id DESC
  LIMIT
    1
);
END IF;
INSERT INTO
  p2p ("Check", checkingpeer, state, "Time")
VALUES
  (id_check, checking, state, P2Ptime);
END $ $ LANGUAGE plpgsql;
-- 2
CREATE
OR REPLACE PROCEDURE verter_check(
  IN nickname VARCHAR,
  IN taskName VARCHAR,
  IN verterState enum_check_status,
  IN checkTime TIME
) AS $ $ DECLARE id_check integer := (
  SELECT
    c.id
  FROM
    p2p p
    INNER JOIN checks c ON c.id = p."Check"
    AND p.state = 'Success'
    AND checks.task = taskName
    AND checks.peer = nickname
  ORDER BY
    p."Time" DESC
  LIMIT
    1
);
BEGIN
INSERT INTO
  verter ("Check", state, "Time")
VALUES
  (id_check, verterState, checkTime);
END $ $ LANGUAGE plpgsql;
-- 3
CREATE
OR REPLACE FUNCTION p2p_transferred_points() RETURNS TRIGGER AS $ $ BEGIN IF NEW.state = 'Start' THEN WITH addq AS (
  SELECT
    DISTINCT NEW.checkingpeer,
    c.peer as checkedpeer
  FROM
    p2p p
    INNER JOIN checks c ON c.id = NEW."Check"
  GROUP BY
    p.checkingpeer,
    checkedpeer
)
UPDATE
  transferredpoints
SET
  pointsamount = pointsamount + 1,
  id = id
FROM
  addq a
WHERE
  transferredpoints.checkingpeer = a.checkingpeer
  AND transferredpoints.checkedpeer = a.checkedpeer;
RETURN NEW;
ELSE RETURN NULL;
END IF;
END;
$ $ LANGUAGE plpgsql;
CREATE
OR REPLACE TRIGGER transferred_points
AFTER
INSERT
  ON p2p FOR EACH ROW EXECUTE PROCEDURE p2p_transferred_points();
--4
CREATE
OR REPLACE FUNCTION check_xp() RETURNS TRIGGER AS $ $ DECLARE status VARCHAR(20);
max_xp INTEGER;
BEGIN
SELECT
  t.maxxp INTO max_xp
FROM
  checks c
  JOIN tasks t ON t.title = c.task;
SELECT
  p.state INTO status
FROM
  checks c
  JOIN p2p p ON c.id = p."Check";
IF new.xpamount > max_xp THEN RAISE EXCEPTION 'XP amount is exceeded';
ELSEIF status = 'Failure' THEN RAISE EXCEPTION 'Check is not successful';
ELSE RETURN NEW;
END IF;
END;
$ $ LANGUAGE plpgsql;
CREATE
OR REPLACE TRIGGER xp_start BEFORE
INSERT
  ON xp FOR EACH ROW EXECUTE PROCEDURE check_xp();