# 🐳 Docker Setup for Oracle to PostgreSQL Migration Tool

## 📋 Prerequisites

- Docker Engine 20.10+
- Docker Compose v2.0+
- At least 2GB RAM available for containers

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Copy environment template
cp .env.example .env

# Edit with your database credentials
nano .env
```

### 2. Build and Run

```bash
# Build the Docker image
docker-compose build

# Run migration (using external databases)
docker-compose up migrator

# Or run with local PostgreSQL for testing
docker-compose up -d postgres
sleep 10  # Wait for PostgreSQL to start
docker-compose up migrator
```

### 3. Using Helper Script

```bash
# Make script executable
chmod +x docker-run.sh

# Run migration
./docker-run.sh migrate

# Run with local PostgreSQL
./docker-run.sh migrate-with-postgres

# View logs
./docker-run.sh logs

# Stop services
./docker-run.sh stop
```

## 🏗️ Docker Files Overview

### Dockerfile (Development)

- Multi-layer build for development
- Includes dev dependencies
- Optimized for rebuilding during development

### Dockerfile.production

- Multi-stage build for smaller image
- Production-only dependencies
- Optimized for deployment

### docker-compose.yml

- Complete stack with migration tool
- Optional PostgreSQL container
- Adminer for database management
- Volume mounts for persistence

## 🔧 Configuration

### Environment Variables

```env
# Oracle Database
ORACLE_USER=your_username
ORACLE_PASSWORD=your_password
ORACLE_HOST=oracle_host
ORACLE_PORT=1521
ORACLE_SID=your_sid

# PostgreSQL Database
POSTGRES_USER=postgres_username
POSTGRES_PASSWORD=postgres_password
POSTGRES_HOST=postgres_host
POSTGRES_PORT=5432
POSTGRES_DB=database_name

# Migration Settings
BATCH_SIZE=1000
LOG_LEVEL=info
MAX_RETRIES=3
CONCURRENT_BATCHES=4
```

### Volume Mounts

- `./logs:/app/logs` - Persist migration logs
- `./migration_checkpoint.json:/app/migration_checkpoint.json` - Resume capability
- `postgres_data:/var/lib/postgresql/data` - PostgreSQL data persistence

## 📊 Usage Scenarios

### Scenario 1: External Databases

When both Oracle and PostgreSQL are external:

```bash
# Update .env with external database details
ORACLE_HOST=102.123.90.201
POSTGRES_HOST=your-postgres-server.com

# Run migration
docker-compose up migrator
```

### Scenario 2: Local PostgreSQL

When you want to use a local PostgreSQL for testing:

```bash
# PostgreSQL will be created automatically
POSTGRES_HOST=postgres  # Use service name

# Run with local PostgreSQL
docker-compose up -d postgres
docker-compose up migrator
```

### Scenario 3: Production Deployment

```bash
# Build production image
docker build -f Dockerfile.production -t migrator:prod .

# Run with production settings
docker run --env-file .env migrator:prod
```

## 🔍 Monitoring and Debugging

### View Logs

```bash
# Live logs
docker-compose logs -f migrator

# PostgreSQL logs
docker-compose logs postgres

# All services
docker-compose logs -f
```

### Access Containers

```bash
# Migration container shell
docker-compose exec migrator sh

# PostgreSQL shell
docker-compose exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB
```

### Database Management

Access Adminer at http://localhost:8080

- Server: postgres
- Username: your POSTGRES_USER
- Password: your POSTGRES_PASSWORD
- Database: your POSTGRES_DB

## 🚨 Troubleshooting

### Common Issues

#### Container Won't Start

```bash
# Check logs
docker-compose logs migrator

# Verify environment variables
docker-compose config
```

#### Database Connection Issues

```bash
# Test Oracle connection
docker-compose exec migrator node -e "
const oracledb = require('oracledb');
oracledb.getConnection({
  user: process.env.ORACLE_USER,
  password: process.env.ORACLE_PASSWORD,
  connectString: process.env.ORACLE_HOST + ':' + process.env.ORACLE_PORT + '/' + process.env.ORACLE_SID
}).then(() => console.log('Oracle: OK')).catch(console.error);
"

# Test PostgreSQL connection
docker-compose exec migrator node -e "
const { Client } = require('pg');
const client = new Client({
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  host: process.env.POSTGRES_HOST,
  port: process.env.POSTGRES_PORT,
  database: process.env.POSTGRES_DB
});
client.connect().then(() => console.log('PostgreSQL: OK')).catch(console.error);
"
```

#### Permission Issues

```bash
# Fix log directory permissions
sudo chown -R $USER:$USER ./logs

# Reset container permissions
docker-compose down
docker-compose up --build
```

#### Out of Memory

```bash
# Increase Docker memory limit in Docker Desktop
# Or reduce batch size
BATCH_SIZE=500
```

## 🔄 Migration Resume

The Docker setup preserves migration state:

```bash
# Migration state is saved in migration_checkpoint.json
# If migration stops, restart with:
docker-compose up migrator

# To start fresh, remove checkpoint:
rm migration_checkpoint.json
docker-compose up migrator
```

## 🧹 Cleanup

### Stop Services

```bash
docker-compose down
```

### Remove Volumes

```bash
docker-compose down -v
```

### Complete Cleanup

```bash
# Remove everything including images
docker-compose down -v --rmi all
docker system prune -a
```

## 📈 Performance Tuning

### Resource Limits

Add to docker-compose.yml:

```yaml
services:
  migrator:
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: "1.0"
        reservations:
          memory: 1G
          cpus: "0.5"
```

### Optimize for Large Datasets

```env
BATCH_SIZE=2000
CONCURRENT_BATCHES=6
```

## 🔐 Security Best Practices

1. **Never commit .env files** with real credentials
2. **Use Docker secrets** for production:
   ```yaml
   secrets:
     oracle_password:
       external: true
   ```
3. **Run as non-root user** (already configured)
4. **Limit container capabilities**
5. **Use specific image tags** instead of `latest`

## 📦 Distribution

### Save Image for Distribution

```bash
# Build and save
docker build -f Dockerfile.production -t migrator:v1.0.0 .
docker save migrator:v1.0.0 | gzip > migrator-v1.0.0.tar.gz

# Load on target system
gunzip -c migrator-v1.0.0.tar.gz | docker load
```

### Push to Registry

```bash
# Tag for registry
docker tag migrator:v1.0.0 your-registry.com/migrator:v1.0.0

# Push
docker push your-registry.com/migrator:v1.0.0
```
