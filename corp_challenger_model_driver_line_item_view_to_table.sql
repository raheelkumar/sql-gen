truncate table 
`{{params.PROJECT_ID_ENRICH}}.{{params.PUBLISH_DATASET}}.{{params.publish_bq_table}}`;
insert into `{{params.PROJECT_ID_ENRICH}}.{{params.PUBLISH_DATASET}}.{{params.publish_bq_table}}`
 SELECT * FROM `{{params.PROJECT_ID_ENRICH}}.{{params.RAW_DATASET}}.{{params.raw_bq_table}}`