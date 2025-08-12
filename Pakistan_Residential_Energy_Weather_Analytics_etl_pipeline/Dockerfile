FROM astrocrpublic.azurecr.io/runtime:3.0-6
# Install ODBC Driver 18 for SQL Server
USER root

RUN apt-get update && \
    apt-get install -y curl gnupg && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

USER astro
