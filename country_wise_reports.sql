CREATE OR REPLACE VIEW `PROJECT_ID.DATASET.VIEW_NAME` AS(
  WITH LAST_MONTH_DATA AS (
  SELECT * FROM `PROJECT_ID.DATASET.DESTINATION_TABLE_NAME`
    WHERE date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 month) AND CURRENT_DATE()
),

PREVIOUS_MONTH_DATA AS (
  SELECT * FROM `PROJECT_ID.DATASET.DESTINATION_TABLE_NAME`
    WHERE date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -2 month) AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 month)
)

  SELECT country_region, last_month_confirmed_cases, previous_month_confirmed_cases  last_month_total_deaths,previous_month_total_deaths, last_month_recoveries, previous_month_recoveries,

    IFNULL(round(100.0 * safe_divide((last_month_total_deaths-previous_month_total_deaths),previous_month_total_deaths), 2), last_month_total_deaths) as percentage_change_in_deaths,
    IFNULL(round(100.0 * safe_divide((last_month_confirmed_cases-previous_month_confirmed_cases),previous_month_confirmed_cases), 2),last_month_confirmed_cases)  as percentage_change_in_confirm_cases,
    IFNULL(round(100.0 * safe_divide((last_month_recoveries-previous_month_recoveries), previous_month_recoveries), 2),last_month_recoveries) as percentage_change_in_recoveries


      FROM (SELECT  country_region, SUM(confirmed) AS last_month_confirmed_cases, SUM(deaths) AS last_month_total_deaths, SUM(recovered) AS last_month_recoveries
              FROM  LAST_MONTH_DATA
              GROUP BY
                country_region
      ) JOIN (
          SELECT  country_region, SUM(confirmed) AS previous_month_confirmed_cases, SUM(deaths) AS previous_month_total_deaths, SUM(recovered) AS previous_month_recoveries
          FROM  PREVIOUS_MONTH_DATA
          GROUP BY
            country_region
      )  USING(country_region)
      ORDER BY last_month_confirmed_cases
)