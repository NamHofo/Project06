

  create or replace view `peppy-primacy-455413-d8`.`summary`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `peppy-primacy-455413-d8`.`summary`.`my_first_dbt_model`
where id = 1;

