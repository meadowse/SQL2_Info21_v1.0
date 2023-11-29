-- 1
CREATE
OR REPLACE FUNCTION transferred_points() RETURNS TABLE (
  Peer1 varchar,
  Peer2 varchar,
  PointsAmount integer
) AS $ $ WITH tmp AS (
  SELECT
    tp.checkingPeer,
    tp.checkedPeer,
    tp.pointsamount
  FROM
    TransferredPoints tp
    INNER JOIN TransferredPoints t2 ON tp.checkingPeer = t2.checkedPeer
    AND tp.checkedPeer = t2.checkingPeer
) (
  SELECT
    checkingPeer,
    checkedPeer,
    sum(result.pointsamount)
  FROM
    (
      SELECT
        tp.checkingPeer,
        tp.checkedPeer,
        tp.pointsamount
      FROM
        TransferredPoints tp
      UNION
      SELECT
        t.checkedPeer,
        t.checkingPeer,
        - t.pointsamount
      FROM
        tmp t
    ) AS result
  GROUP BY
    1,
    2
)
EXCEPT
SELECT
  tmp.checkingPeer,
  tmp.checkedPeer,
  tmp.pointsamount
FROM
  tmp;
$ $ LANGUAGE sql;
SELECT
  *
FROM
  transferred_points();
--2
CREATE
OR REPLACE FUNCTION good_checks() RETURNS TABLE (
  peer VARCHAR,
  task VARCHAR,
  xpamount INTEGER
) AS $ $ BEGIN RETURN QUERY WITH q AS (
  SELECT
    DISTINCT c.id
  FROM
    checks c
    JOIN p2p p ON c.id = p."Check"
    LEFT JOIN Verter v ON c.id = v."Check"
  WHERE
    (
      p.state = 'Success'
      AND c.task > 'C6_s21_matrix'
    )
    OR (
      p.state = 'Success'
      AND v.state = 'Success'
    )
)
SELECT
  c.peer,
  c.task,
  x.xpamount
FROM
  q
  JOIN checks c ON q.id = c.id
  JOIN XP x ON q.id = x."Check";
END $ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  good_checks();
--3
CREATE
OR REPLACE FUNCTION remaining_peers(IN in_date DATE) RETURNS SETOF VARCHAR AS $ $ BEGIN RETURN QUERY (
  SELECT
    DISTINCT peer
  FROM
    timetracking t1
    JOIN (
      SELECT
        peer,
        MAX("Time") "Time"
      FROM
        timetracking
      WHERE
        "Date" = in_date
      GROUP BY
        peer
    ) t2 USING (peer, "Time")
  WHERE
    t1.state = 1
);
END;
$ $ LANGUAGE plpgsql;
-- extra: former 4
CREATE
OR REPLACE PROCEDURE checks_status(
  OUT SuccessfulChecks REAL,
  OUT UnsuccessfulChecks REAL
) AS $ $ BEGIN
SELECT
  ROUND(SUM(t.c1) / COUNT(*) :: real * 100) :: real,
  ROUND(
    (COUNT(*) :: real - SUM(t.c1)) / COUNT(*) :: real * 100
  ) :: real
FROM
  (
    SELECT
      CASE
        WHEN p.state = COALESCE(v.state, 'Success') THEN 1
        ELSE 0
      END c1
    FROM
      checks c
      JOIN p2p p ON c.id = p."Check"
      LEFT JOIN verter v ON c.id = v."Check"
    WHERE
      p.state != 'Start'
      AND (
        v.state != 'Start'
        OR v.state IS NULL
      )
  ) t INTO SuccessfulChecks,
  UnsuccessfulChecks;
END;
$ $ LANGUAGE plpgsql;
CALL checks_status(NULL, NULL);
--4
CREATE
OR REPLACE FUNCTION get_points() RETURNS TABLE (Peer VARCHAR, PointsChange BIGINT) AS $ $ BEGIN RETURN QUERY (
  WITH sum_checkingpeer AS (
    SELECT
      checkingpeer,
      ABS(SUM(pointsamount)) AS sum_points
    FROM
      transferredpoints
    GROUP BY
      checkingpeer
  ),
  sum_checkedpeer AS (
    SELECT
      checkedpeer,
      ABS(SUM(pointsamount)) AS sum_points
    FROM
      transferredpoints
    GROUP BY
      checkedpeer
  )
  SELECT
    checkingpeer AS Peer,
    (
      (COALESCE(s1.sum_points, 0)) - (COALESCE(s2.sum_points, 0))
    ) AS PointsChange
  FROM
    sum_checkingpeer s1
    JOIN sum_checkedpeer s2 ON s1.checkingpeer = s2.checkedpeer
  ORDER BY
    PointsChange DESC
);
END;
$ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  get_points();
--5
CREATE
OR REPLACE FUNCTION get_points_der() RETURNS TABLE (Peer VARCHAR, PointsChange BIGINT) AS $ $ BEGIN RETURN QUERY (
  WITH p1 AS (
    SELECT
      Peer1 AS peer,
      SUM(PointsAmount) PointsAmount
    FROM
      fnc_transferred_points()
    GROUP BY
      peer
  ),
  p2 AS (
    SELECT
      Peer2 AS peer,
      SUM(PointsAmount) PointsAmount
    FROM
      fnc_transferred_points()
    GROUP BY
      peer
  )
  SELECT
    COALESCE(p1.peer, p2.peer) AS peer,
    (
      COALESCE(p1.PointsAmount, 0) - COALESCE(p2.PointsAmount, 0)
    ) AS PointsChange
  FROM
    p1 FULL
    JOIN p2 ON p1.peer = p2.peer
  ORDER BY
    PointsChange DESC
);
END;
$ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  get_points_der();
--6
CREATE
OR REPLACE FUNCTION get_top() RETURNS TABLE (
  day DATE,
  task VARCHAR
) AS $ $ WITH t1 AS (
  SELECT
    c."Date" AS day,
    c.task,
    COUNT(c.task) AS tc
  FROM
    checks c
  GROUP BY
    day,
    c.task
),
t2 AS (
  SELECT
    t1.task,
    t1.day,
    rank() OVER (
      PARTITION BY t1.day
      ORDER BY
        tc DESC
    ) AS rank
  FROM
    t1
)
SELECT
  t2.day,
  t2.task
FROM
  t2
WHERE
  rank = 1;
$ $ LANGUAGE sql;
SELECT
  *
FROM
  get_top() --extra: former 8
  CREATE
  OR REPLACE FUNCTION get_duration() RETURNS TIME LANGUAGE plpgsql AS $ $ DECLARE wid INT := (
    SELECT
      p."Check"
    FROM
      p2p p
    WHERE
      state = 'Success'
      OR state = 'Failure'
    ORDER BY
      id DESC
    LIMIT
      1
  );
start_time TIME := (
  SELECT
    "Time"
  FROM
    p2p
  WHERE
    state = 'Start'
    AND "Check" = wid
);
end_time TIME := (
  SELECT
    "Time"
  FROM
    p2p
  WHERE
    (
      state = 'Success'
      OR state = 'Failure'
    )
    AND "Check" = wid
);
BEGIN IF end_time IS NOT NULL
AND start_time IS NOT NULL THEN RETURN end_time - start_time;
ELSE RETURN NULL;
END IF;
END $ $;
--7
CREATE
OR REPLACE FUNCTION get_last_date(topic VARCHAR) RETURNS TABLE (
  Peer VARCHAR,
  Day DATE
) AS $ $ BEGIN RETURN query WITH lt AS (
  SELECT
    MAX(title) AS title
  FROM
    tasks
  WHERE
    title SIMILAR TO concat('C', '[0-9]_%')
),
dates AS (
  SELECT
    c.peer,
    c.task,
    c."Date"
  FROM
    checks c
    JOIN p2p p ON c.id = p."Check"
    LEFT JOIN verter v ON c.id = v."Check"
    AND v.state = 'Success'
  WHERE
    p.state = 'Success'
)
SELECT
  d.peer AS Peer,
  d."Date" AS Day
FROM
  dates d
  INNER JOIN lt l ON d.task = l.title;
END $ $ LANGUAGE plpgsql;
-- Тестовый запрос.
SELECT
  *
FROM
  get_last_date('C');
--8
CREATE
OR REPLACE FUNCTION get_rec() RETURNS TABLE (
  peer VARCHAR,
  recommendedpeer VARCHAR
) AS $ $ BEGIN RETURN QUERY WITH af AS (
  SELECT
    nickname,
    (
      CASE
        WHEN nickname = f.peer1 THEN f.peer2
        ELSE f.peer1
      END
    ) AS friends
  FROM
    peers p
    JOIN friends f ON p.nickname = f.peer1
    OR p.nickname = f.peer2
),
ar AS (
  SELECT
    nickname,
    COUNT(r.recommendedpeer) AS count_rec,
    r.recommendedpeer
  FROM
    af a
    JOIN recommendations r ON a.friends = r.peer
  WHERE
    a.nickname != r.recommendedpeer
  GROUP BY
    nickname,
    r.recommendedpeer
),
gm AS (
  SELECT
    nickname,
    MAX(count_rec) AS max_count
  FROM
    ar
  GROUP BY
    nickname
)
SELECT
  a.nickname AS Peer,
  a.RecommendedPeer
FROM
  ar a
  JOIN gm g ON a.nickname = g.nickname
  AND a.count_rec = g.max_count
ORDER BY
  1,
  2;
END;
$ $ LANGUAGE plpgsql;
--9
CREATE
OR REPLACE FUNCTION determine_percentage(block1 VARCHAR, block2 VARCHAR) RETURNS TABLE (
  StartedBlock1 BIGINT,
  StartedBlock2 BIGINT,
  StartedBothBlocks BIGINT,
  DidntStartAnyBlock BIGINT
) AS $ $ DECLARE count_peers INT := (
  SELECT
    COUNT(peers.nickname)
  FROM
    peers
);
BEGIN RETURN QUERY WITH startedblock1 AS (
  SELECT
    DISTINCT c.peer
  FROM
    Checks c
  WHERE
    c.task SIMILAR TO concat(block1, '[0-9]_%')
),
startedblock2 AS (
  SELECT
    DISTINCT peer
  FROM
    Checks c
  WHERE
    c.task SIMILAR TO concat(block2, '[0-9]_%')
),
startedboth AS (
  SELECT
    DISTINCT s1.peer
  FROM
    startedblock1 s1
    JOIN startedblock2 s2 USING(peer)
),
startedoneof AS (
  SELECT
    DISTINCT peer
  FROM
    (
      (
        SELECT
          peer
        FROM
          startedblock1
      )
      UNION
      (
        SELECT
          peer
        FROM
          startedblock2
      )
    ) AS foo
),
count_startedblock1 AS (
  SELECT
    COUNT(*) AS count_startedblock1
  FROM
    startedblock1
),
count_startedblock2 AS (
  SELECT
    COUNT(*) AS count_startedblock2
  FROM
    startedblock2
),
count_startedboth AS (
  SELECT
    COUNT(*) AS count_startedboth
  FROM
    startedboth
),
count_startedoneof AS (
  SELECT
    COUNT(*) AS count_startedoneof
  FROM
    startedoneof
)
SELECT
  (
    (
      SELECT
        count_startedblock1 :: BIGINT
      FROM
        count_startedblock1
    ) * 100 / count_peers
  ) AS StartedBlock1,
  (
    (
      SELECT
        count_startedblock2 :: BIGINT
      FROM
        count_startedblock2
    ) * 100 / count_peers
  ) AS StartedBlock2,
  (
    (
      SELECT
        count_startedboth :: BIGINT
      FROM
        count_startedboth
    ) * 100 / count_peers
  ) AS StartedBothBlocks,
  (
    (
      SELECT
        count_peers - count_startedoneof :: BIGINT
      FROM
        count_startedoneof
    ) * 100 / count_peers
  ) AS DidntStartAnyBlock;
END $ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  determine_percentage('C', 'D');
--extra: former 12
CREATE
OR REPLACE FUNCTION get_friends(min_count INT) RETURNS TABLE (
  Peer VARCHAR,
  FriendsCount BIGINT
) AS $ $ BEGIN RETURN QUERY WITH all_friends AS (
  SELECT
    p.nickname,
    CASE
      WHEN p.nickname = f.peer1 THEN peer2
      ELSE peer1
    END AS friends
  FROM
    peers p
    JOIN friends f ON p.nickname = f.peer1
    OR p.nickname = f.peer2
)
SELECT
  nickname AS Peer,
  COUNT(friends) AS FriendsCount
FROM
  all_friends
GROUP BY
  nickname
ORDER BY
  FriendsCount DESC
LIMIT
  min_count;
END $ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  get_friends(5);
--10
CREATE
OR REPLACE FUNCTION get_birthday() RETURNS TABLE (
  SuccessfulChecks BIGINT,
  UnsuccessfulChecks BIGINT
) AS $ $ DECLARE count_peers BIGINT := (
  SELECT
    COUNT(peers.nickname)
  FROM
    peers
);
BEGIN RETURN QUERY WITH date_peers AS (
  SELECT
    nickname,
    EXTRACT(
      MONTH
      FROM
        birthday
    ) AS p_month,
    EXTRACT(
      DAY
      FROM
        birthday
    ) AS p_day
  FROM
    peers
),
date_checks AS (
  SELECT
    checks.id,
    peer,
    EXTRACT(
      MONTH
      FROM
        "Date"
    ) AS c_month,
    EXTRACT(
      day
      FROM
        "Date"
    ) AS c_day,
    p2p.state AS p_state,
    verter.state AS v_state
  FROM
    checks
    JOIN p2p ON checks.id = p2p."Check"
    LEFT JOIN verter ON checks.id = verter."Check"
  WHERE
    p2p.state IN ('Success', 'Failure')
    AND (
      (
        verter.state IN ('Success', 'Failure')
        OR verter.state IS NULL
      )
    )
),
both_tables AS (
  SELECT
    *
  FROM
    date_peers t1
    JOIN date_checks t2 ON t1.p_day = t2.c_day
    AND t1.p_month = t2.c_month
),
success AS (
  SELECT
    COUNT(*) AS s_count
  FROM
    both_tables
  WHERE
    p_state = 'Success'
    AND (
      v_state = 'Success'
      OR v_state IS NULL
    )
),
failure AS (
  SELECT
    COUNT(*) AS f_count
  FROM
    both_tables
  WHERE
    p_state = 'Failure'
    AND (
      v_state = 'Failure'
      OR v_state IS NULL
    )
)
SELECT
  (
    (
      SELECT
        s_count
      FROM
        success
    ) * 100
  ) / count_peers AS SuccessfulChecks,
  (
    (
      SELECT
        f_count
      FROM
        failure
    ) * 100
  ) / count_peers AS UnsuccessfulChecks;
END $ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  get_birthday();
--extra: former 14
CREATE
OR REPLACE FUNCTION get_xp() RETURNS TABLE (
  Peer VARCHAR,
  XP BIGINT
) AS $ $ BEGIN RETURN QUERY WITH max_xp AS (
  SELECT
    c.peer,
    MAX(x.xpamount) AS maxxp
  FROM
    checks c
    JOIN xp x ON c.id = x."Check"
  GROUP BY
    c.peer,
    c.task
)
SELECT
  max_xp.peer AS Peer,
  SUM(maxxp) AS XP
FROM
  max_xp
GROUP BY
  max_xp.peer
ORDER BY
  XP DESC;
END $ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  get_xp();
--11
CREATE
OR REPLACE FUNCTION get_peers(
  IN task1 VARCHAR,
  IN task2 VARCHAR,
  IN task3 VARCHAR
) RETURNS TABLE (peer VARCHAR) AS $ $ BEGIN RETURN QUERY (
  SELECT
    g.peer
  FROM
    good_checks() g
  WHERE
    task = task1
)
INTERSECT
(
  SELECT
    g.peer
  FROM
    good_checks() g
  WHERE
    task = task2
)
INTERSECT
(
  SELECT
    g.peer
  FROM
    good_checks() g
  WHERE
    task != task3
);
END $ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  get_peers(
    'C3_s21_string+',
    'C2_SimpleBashUtils',
    'C6_s21_matrix'
  );
--12
CREATE
OR REPLACE FUNCTION get_parents() RETURNS TABLE (
  Task varchar,
  PrevCount integer
) AS $ $ BEGIN RETURN QUERY WITH RECURSIVE r AS (
  SELECT
    (
      CASE
        WHEN tasks.parenttask IS NULL THEN 0
        ELSE 1
      END
    ) AS counter,
    tasks.title,
    tasks.parenttask AS current_task,
    tasks.parenttask
  FROM
    tasks
  UNION ALL
  SELECT
    (
      CASE
        WHEN child.parenttask IS NOT NULL THEN counter + 1
        ELSE counter
      END
    ) AS counter,
    child.title AS title,
    child.parenttask AS current_task,
    parrent.title AS parrenttask
  FROM
    tasks AS child
    CROSS JOIN r AS parrent
  WHERE
    parrent.title = child.parenttask
)
SELECT
  title AS Task,
  MAX(counter) AS PrevCount
FROM
  r
GROUP BY
  title
ORDER BY
  task;
END $ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  get_parents();
--13
CREATE
OR REPLACE FUNCTION get_row(IN days INT) RETURNS TABLE ("Date" DATE) AS $ $ BEGIN RETURN QUERY (
  SELECT
    c."Date"
  FROM
    checks c
    JOIN p2p p ON c.id = p."Check"
    LEFT JOIN verter v ON c.id = v."Check"
    JOIN tasks t ON c.task = t.title
    JOIN xp x ON c.id = x."Check"
  WHERE
    p.state = 'Success'
    AND (
      v.state = 'Success'
      OR v.state IS NULL
    )
    AND x.xpamount >= t.maxxp * 0.8
  GROUP BY
    c."Date"
  HAVING
    COUNT(c."Date") >= days
)
EXCEPT
  (
    SELECT
      c."Date"
    FROM
      checks c
      JOIN p2p p ON c.id = p."Check"
      LEFT JOIN verter v ON c.id = v."Check"
    WHERE
      p.state = 'Failure'
      OR v.state = 'Failure'
    GROUP BY
      c."Date"
  );
END;
$ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  get_row(2);
--extra: former 18
CREATE
OR REPLACE FUNCTION get_thebest() RETURNS TABLE (Peer VARCHAR, CompletedTasksCount BIGINT) AS $ $ BEGIN RETURN QUERY
SELECT
  c.peer,
  COUNT(*) AS CompletedTasksCount
FROM
  xp x
  JOIN checks c ON x."Check" = c.id
GROUP BY
  c.peer
ORDER BY
  CompletedTasksCount DESC
LIMIT
  1;
END;
$ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  get_thebest();
--14
CREATE
OR REPLACE PROCEDURE get_megapeer(INOUT thebest VARCHAR) LANGUAGE 'plpgsql' AS $ $ BEGIN
SELECT
  c.peer INTO thebest
FROM
  xp x
  JOIN checks c ON x."Check" = c.id
GROUP BY
  c.peer
ORDER BY
  SUM(x.xpamount) DESC
LIMIT
  1;
RETURN;
END;
$ $;
CALL get_megapeer(NULL);
--extra: former 20
CREATE
OR REPLACE PROCEDURE get_interval(
  OUT out_peer VARCHAR,
  IN c_date DATE DEFAULT current_date
) AS $ $ BEGIN WITH time_in AS (
  SELECT
    t.peer,
    SUM("Time") AS time_in_campus
  FROM
    timetracking t
  WHERE
    "Date" = c_date
    AND state = 1
  GROUP BY
    t.peer
),
time_out AS (
  SELECT
    t.peer,
    SUM("Time") AS time_out_campus
  FROM
    timetracking t
  WHERE
    "Date" = c_date
    AND state = 2
  GROUP BY
    peer
),
diff_time AS (
  SELECT
    time_in.peer,
    (time_out_campus - time_in_campus) AS full_time
  FROM
    time_in
    JOIN time_out ON time_in.peer = time_out.peer
)
SELECT
  d.peer INTO out_peer
FROM
  diff_time d
ORDER BY
  full_time DESC
LIMIT
  1;
END;
$ $ LANGUAGE plpgsql;
CALL get_interval(NULL);
CALL get_interval(NULL, '2022-10-09');
--15
CREATE
OR REPLACE FUNCTION early_risers(IN checktime TIME, IN N INT) RETURNS TABLE (peer varchar) AS $ $
SELECT
  peer
FROM
  (
    SELECT
      peer,
      MIN("Time") min_time,
      "Date"
    FROM
      timetracking
    WHERE
      state = 1
    GROUP BY
      "Date",
      peer
  ) t
WHERE
  min_time < checktime
GROUP BY
  peer
HAVING
  COUNT(peer) >= N;
$ $ LANGUAGE sql;
SELECT
  *
FROM
  early_risers('09:00:00', 1);
--16
CREATE
OR REPLACE FUNCTION get_leaving(IN n_days INT, IN n_times INT) RETURNS TABLE (peer VARCHAR) AS $ $
SELECT
  peer
FROM
  timetracking
WHERE
  state = 2
  AND "Date" > (current_date - n_days)
GROUP BY
  peer
HAVING
  COUNT(*) > n_times $ $ LANGUAGE sql;
SELECT
  *
FROM
  get_leaving(500, 1);
--extra: former 23
CREATE
OR REPLACE FUNCTION get_first(IN c_date DATE DEFAULT current_date) --INOUT out_peer VARCHAR DEFAULT NULL)--,
RETURNS TABLE (
  "Peer" VARCHAR
) AS $ $
SELECT
  peer --INTO out_peer
FROM
  timetracking
WHERE
  state = 1
  AND "Date" = c_date
GROUP BY
  peer
ORDER BY
  MIN("Time")
LIMIT
  1;
$ $ LANGUAGE sql;
SELECT
  *
FROM
  get_first();
SELECT
  *
FROM
  get_first('2022-09-21');
--extra: former 24
CREATE
OR REPLACE FUNCTION to_minutes(t time without time zone) RETURNS integer AS $ BODY $ DECLARE hs INTEGER := (
  SELECT
(
      EXTRACT(
        HOUR
        FROM
          t :: time
      ) * 60
    )
);
ms INTEGER := (
  SELECT
    (
      EXTRACT(
        MINUTES
        FROM
          t :: time
      )
    )
);
BEGIN
SELECT
  (hs + ms) INTO ms;
RETURN ms;
END;
$ BODY $ LANGUAGE 'plpgsql';
CREATE
OR REPLACE FUNCTION get_left(
  IN n_min INTEGER,
  IN y_date date DEFAULT current_date
) RETURNS TABLE (
  "Peer1" VARCHAR
) AS $ $ WITH tt AS (
  (
    SELECT
      t.peer,
      t."Date",
      t."Time",
      t.state
    FROM
      timetracking t
  )
  EXCEPT
    (
      SELECT
        t.peer,
        t."Date",
        MIN(t."Time"),
        t.state
      FROM
        timetracking t
      WHERE
        t.state = 1
      GROUP BY
        1,
        2,
        4
    )
  EXCEPT
    (
      SELECT
        t.peer,
        t."Date",
        MAX(t."Time"),
        t.state
      FROM
        timetracking t
      WHERE
        t.state = 2
      GROUP BY
        1,
        2,
        4
    )
),
tm AS(
  SELECT
    DISTINCT t2.peer,
    "Date",
    (
      SELECT
        SUM("Time")
      FROM
        tt
      WHERE
        tt.state = 1
        AND tt.peer = t2.peer
      GROUP BY
        peer,
        "Date"
    ) - (
      SELECT
        sum("Time")
      FROM
        tt
      WHERE
        tt.state = 2
        AND tt.peer = t2.peer
      GROUP BY
        peer,
        "Date"
    ) AS n_minutes
  FROM
    tt t2
)
SELECT
  tm.peer
FROM
  tm
WHERE
  to_minutes(tm.n_minutes :: time) > n_min
  AND "Date" = y_date $ $ LANGUAGE sql;
-- Тестовый запрос.
SELECT
  *
FROM
  get_left(30);
SELECT
  *
FROM
  get_left(30, '2022-10-09');
--17
CREATE
OR REPLACE FUNCTION get_risers() RETURNS TABLE (
  "Month" VARCHAR,
  EarlyEntries BIGINT
) AS $ $ BEGIN RETURN QUERY WITH m AS (
  SELECT
    p.nickname,
    COUNT(t.state) AS morning
  FROM
    peers p
    JOIN timetracking t ON p.nickname = t.peer
  WHERE
    t."Time" < '12:00:00'
  GROUP BY
    1
),
al AS (
  SELECT
    p.nickname,
    TO_CHAR(p.birthday, 'Month') AS months,
    COUNT(t.state) AS ma
  FROM
    peers p
    JOIN timetracking t ON p.nickname = t.peer
  GROUP BY
    1,
    2
),
inter AS (
  SELECT
    al.months AS Months,
    SUM(COALESCE(m.morning, 0)) AS morning,
    SUM(al.ma) AS ma
  FROM
    m
    RIGHT JOIN al USING(nickname)
  GROUP BY
    1
)
SELECT
  i.Months :: VARCHAR AS "Month",
  ROUND(100 * COALESCE(i.morning, 0) / i.ma) :: BIGINT AS EarlyEntries
FROM
  inter i
ORDER BY
  2 DESC;
END $ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  get_risers();