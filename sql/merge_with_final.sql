MERGE INTO `PROJECT_ID.hopkins_covid.hopkins_covid_final` FINAL
USING `PROJECT_ID.hopkins_covid.hopkins_covid_staging` STAGING
ON (
    STAGING.date = FINAL.date AND STAGING.country_region = FINAL.country_region AND STAGING.province_state = FINAL.province_state
  )
WHEN NOT MATCHED THEN
  INSERT (date, country_region, province_state, confirmed, deaths, recovered)
  VALUES (date, country_region, province_state, confirmed, deaths, recovered)
