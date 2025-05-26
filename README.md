# dbt_test

profiles.yml set under .dbt
`cp profiles.yml /home/xxx/.dbt/profiles.yml`
VS codeで開き、schema:を修正してから ctrl+S(保存)しctrl+X(出る)
`nano /home/xxx/.dbt/profiles.yml`
DBT가 BigQuery를 사용할 때, method: oauth이면 Application Default Credentials(ADC) 를 사용해서 인증합니다.
`gcloud auth application-default login`