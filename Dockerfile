# Use the base Python image with a specific version
ARG PYTHON_VERSION=3.11.3
FROM python:${PYTHON_VERSION}-slim as base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Create a working directory inside the container
WORKDIR /app

# Install required packages
RUN apt-get update && apt-get install -y \
    curl apt-transport-https debconf-utils build-essential

# Install SQL Server drivers and tools
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | tee /etc/apt/sources.list.d/msprod.list
RUN apt-get update
ENV ACCEPT_EULA=y DEBIAN_FRONTEND=noninteractive
RUN apt-get install -y mssql-tools18 msodbcsql18 unixodbc-dev

# Install python and necessary libraries
RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-dev python3-setuptools locales

# Add the directory containing bcp to the PATH
ENV PATH="/opt/mssql-tools18/bin:${PATH}"

# Install Azure SDK dependencies
RUN pip install azure-identity azure-keyvault-secrets

# Install Azure CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# Set environment variables
ENV AZURE_CLIENT_ID=92e1c26c-5d4d-476c-af27-0e356a2990da
ENV AZURE_CLIENT_SECRET=WHz8Q~3jv5ATt9JzRONf.Z4kBJW660ehADdQ1bSw
ENV AZURE_TENANT_ID=8f0e4642-c00b-4f6f-95ab-f8a52d41781b

# Create a non-privileged user that the app will run under.
# See https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#user
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.cache/pip to speed up subsequent builds.
# Leverage a bind mount to requirements.txt to avoid having to copy them into
# into this layer.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python -m pip install -r requirements.txt

# Switch to the non-privileged user to run the application.
USER appuser

# Copy the source code into the container.
COPY . /app

# Expose the port that the application listens on.
EXPOSE 5000

# Run the application.
CMD ["python", "app.py"]

