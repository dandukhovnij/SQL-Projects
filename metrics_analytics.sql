-- I use CTE for greater convenience and readability.
WITH
-- I calculate metrics for emails through aggregate functions and group them
  email_data AS (
  SELECT
    DATE_ADD(date, INTERVAL sent_date DAY) AS date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    COUNT(es.id_message) AS sent_msg,
    COUNT(eo.id_message) AS open_msg,
    COUNT(ev.id_message) AS visit_msg
  FROM
    `data-analytics-mate.DA.account` a
  JOIN
    `data-analytics-mate.DA.account_session` acs
  ON
    a.id = acs.account_id
  JOIN
    `data-analytics-mate.DA.session` s
  USING
    (ga_session_id)
  JOIN
    `data-analytics-mate.DA.session_params` sp
  USING
    (ga_session_id)
  JOIN
    `data-analytics-mate.DA.email_sent` es
  ON
    a.id = es.id_account
  LEFT JOIN
    `data-analytics-mate.DA.email_open` eo
  USING
    (id_message)
  LEFT JOIN
    `data-analytics-mate.DA.email_visit` ev
  USING
    (id_message)
  GROUP BY
    1,
    2,
    3,
    4,
    5 ),
  -- I calculate metrics for accounts through aggregate functions and group them
  account_data AS (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    COUNT(a.id) AS account_cnt
  FROM
    `data-analytics-mate.DA.account` a
  JOIN
    `data-analytics-mate.DA.account_session` acs
  ON
    a.id = acs.account_id
  JOIN
    `data-analytics-mate.DA.session` s
  USING
    (ga_session_id)
  JOIN
    `data-analytics-mate.DA.session_params` sp
  USING
    (ga_session_id)
  GROUP BY
    1,
    2,
    3,
    4,
    5 ),
  -- I combine the results via UNION, following the rules for working with this operator.
  union_data AS (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    0 AS account_cnt,
    sent_msg,
    open_msg,
    visit_msg
  FROM
    email_data
  UNION ALL
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    0 AS sent_msg,
    0 AS open_msg,
    0 AS visit_msg
  FROM
    account_data ),
  -- In this query I get rid of extra zeros using aggregate functions and grouping
  data_prep AS (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    SUM(account_cnt) AS account_cnt,
    SUM(sent_msg) AS sent_msg,
    SUM(open_msg) AS open_msg,
    SUM(visit_msg) AS visit_msg,
  FROM
    union_data
  GROUP BY
    1,
    2,
    3,
    4,
    5),
  -- I count the number of emails and accounts by country using window functions.
  total_cnt AS (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    sent_msg,
    open_msg,
    visit_msg,
    SUM(account_cnt) OVER(PARTITION BY country) AS total_country_account_cnt,
    SUM(sent_msg) OVER(PARTITION BY country) AS total_country_sent_cnt
  FROM
    data_prep ),
  -- I add ranking to the query by the number of emails and accounts
  rank_total_cnt AS (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    sent_msg,
    open_msg,
    visit_msg,
    total_country_account_cnt,
    total_country_sent_cnt,
    DENSE_RANK() OVER(ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt,
    DENSE_RANK() OVER(ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt
  FROM
    total_cnt)
-- Final resulting query with filtering
SELECT
  date,
  country,
  send_interval,
  is_verified,
  is_unsubscribed,
  account_cnt,
  sent_msg,
  open_msg,
  visit_msg,
  total_country_account_cnt,
  total_country_sent_cnt,
  rank_total_country_sent_cnt,
  rank_total_country_account_cnt
FROM
  rank_total_cnt
WHERE
  rank_total_country_sent_cnt <= 10
  OR rank_total_country_account_cnt <= 10
