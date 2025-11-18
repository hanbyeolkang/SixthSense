{% macro generate_schema_name(custom_schema_name, node) %}

    {# raw_data.stg_ #}
    {% if node.path.startswith('staging') %}
        {{ return('raw_data') }}

    {# analytics.dim_ & analytics.fact_ #}
    {% elif node.path.startswith('marts') %}
        {{ return('analytics') }}

    {# 디폴트 스키마 (profiles.yml에 지정) #}
    {% else %}
        {{ return(custom_schema_name) }}
    {% endif %}

{% endmacro %}