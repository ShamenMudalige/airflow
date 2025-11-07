FROM astrocrpublic.azurecr.io/runtime:3.1-3
# Install Microsoft SQL Server tools (includes dtexec)
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl -sSL https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y mssql-tools unixodbc-dev

ENV PATH="$PATH:/opt/mssql-tools/bin"

