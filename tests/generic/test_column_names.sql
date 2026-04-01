{% test column_names(model,column_name,silver_model,exclude_columns=[]) %}

{% set silver_columns = select_star(from=ref(silver_model),test_mode='true')  %}
{% set bronze_columns = star_renamed(from=model,test_mode='true',except=exclude_columns)  %}

    select * from ( {{bronze_columns}} ) bronze_columns
    except
    select * from ( {{silver_columns}}  ) silver_columns

{% endtest %}