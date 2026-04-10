FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    dbt-core==1.11.7 \
    dbt-postgres==1.10.0

COPY dbt_analytics/ ./dbt_analytics/
COPY data_build/ ./data_build/
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

# profiles.yml lives inside dbt_analytics/ and uses env vars for all secrets
ENV DBT_PROFILES_DIR=/app/dbt_analytics
ENV DBT_TARGET=prod

ENTRYPOINT ["/entrypoint.sh"]
# Default command — override per ECS task definition
CMD ["run"]
