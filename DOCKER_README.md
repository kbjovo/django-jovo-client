# Django CDC Replication - Docker Setup

Complete Dockerized environment for Django Change Data Capture (CDC) replication system.

## 🏗️ Architecture

### Services

| Service | Container | Port | Description |
|---------|-----------|------|-------------|
| **Django** | `django-web` | 8000 | Web application (development mode with hot reload) |
| **Celery Worker** | `celery-worker` | - | Main task queue (4 workers) |
| **Celery Consumer** | `celery-consumer` | - | CDC consumer queue (2 workers) |
| **Celery Beat** | `celery-beat` | - | Periodic task scheduler |
| **MySQL** | `mysql_wsl` | 3306 | Source database with binlog |
| **Redis** | `redis` | 6379 | Message broker & cache |
| **Kafka** | `kafka` | 9092, 9093 | Message streaming platform (KRaft mode) |
| **Kafka Connect** | `kafka-connect` | 8083 | Debezium CDC connector |
| **Schema Registry** | `schema-registry` | 8081 | Kafka schema management |
| **Kafka UI** | `kafka-ui` | 8080 | Web UI for Kafka |
| **Adminer** | `adminer` | 8082 | Database management UI |

### Network

All services run on the `replication-network` Docker bridge network, enabling seamless communication using service names as hostnames.

## 🚀 Quick Start

### Prerequisites

- Docker Desktop or Docker Engine (20.10+)
- Docker Compose (v2.0+)
- At least 8GB RAM available for Docker

### 1. Setup Environment

```bash
# Copy environment template
cp .env.docker .env.docker

# Edit configuration (optional - defaults work for development)
nano .env.docker
```

### 2. Start All Services

```bash
# Option A: Use the startup script
./docker-start.sh

# Option B: Manual start
cd debezium-setup
docker-compose up -d --build
```

### 3. Run Migrations

```bash
docker-compose exec django python manage.py migrate
```

### 4. Create Superuser

```bash
docker-compose exec django python manage.py createsuperuser
```

### 5. Access the Application

- **Django Admin**: http://localhost:8000/admin
- **Kafka UI**: http://localhost:8080
- **Adminer (DB)**: http://localhost:8082

## 📦 Technology Stack

### Python Dependencies

Managed via `uv` package manager using `pyproject.toml`:

- **Django 5.2.7**: Web framework
- **Celery 5.5.3+**: Distributed task queue
- **django-tailwind 4.2.0**: Tailwind CSS integration
- **SQLAlchemy 2.0.44**: Database toolkit
- **confluent-kafka 2.12.1+**: Kafka client
- **mysqlclient 2.2.7**: MySQL adapter
- **redis 7.0.1+**: Redis client

### Frontend

- **Tailwind CSS**: Utility-first CSS framework (auto-compiled in Docker build)
- **Alpine.js**: Lightweight JavaScript framework
- **Django Cotton**: Component library

## 🔧 Development Workflow

### Hot Reload

Code changes are automatically detected:
- Django: Auto-reloads on code changes
- Celery: Restart containers for code changes
- Tailwind: Rebuild assets if needed

### Viewing Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f django
docker-compose logs -f celery-consumer

# Last 100 lines
docker-compose logs --tail=100 celery-worker
```

### Restarting Services

```bash
# Restart specific service
docker-compose restart django
docker-compose restart celery-consumer

# Rebuild and restart after dependency changes
docker-compose up -d --build django
```

### Shell Access

```bash
# Django shell
docker-compose exec django python manage.py shell

# Bash shell
docker-compose exec django bash

# Database shell
docker-compose exec django python manage.py dbshell
```

## 🗄️ Data Persistence

### Volumes

- `mysql-data`: MySQL database files
- `redis-data`: Redis persistence
- `kafka-data`: Kafka logs and data
- `django-static`: Collected static files
- `django-media`: User-uploaded media

### Backup Data

```bash
# Backup MySQL
docker-compose exec mysql mysqldump -uroot -proot client > backup.sql

# Restore MySQL
docker-compose exec -T mysql mysql -uroot -proot client < backup.sql
```

## 🧪 Testing CDC Replication

### 1. Verify Kafka is Running

```bash
# Check Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check Kafka Connect
curl http://localhost:8083/connectors
```

### 2. Test CDC Flow

```bash
# Insert test data in MySQL
docker-compose exec mysql mysql -uroot -proot -e "USE client; INSERT INTO test_table (name) VALUES ('test');"

# Check Kafka UI
open http://localhost:8080

# View consumer logs
docker-compose logs -f celery-consumer
```

## 🛠️ Troubleshooting

### Consumer Can't Connect to Kafka

**Symptom**: `Failed to resolve 'kafka:29092'`

**Solution**: Ensure all services are on `replication-network`
```bash
docker network inspect replication-network
```

### Django Static Files Missing

```bash
docker-compose exec django python manage.py collectstatic --noinput
```

### Tailwind Not Building

```bash
# Rebuild with no cache
docker-compose build --no-cache django

# Or manually build Tailwind
docker-compose exec django python manage.py tailwind build
```

### Permission Errors

```bash
# Fix ownership (run on host)
sudo chown -R $USER:$USER .
```

### Services Not Starting

```bash
# Check service health
docker-compose ps

# View specific service logs
docker-compose logs kafka-connect

# Restart unhealthy services
docker-compose restart kafka-connect
```

## 🔐 Production Deployment

### 1. Update Environment

```bash
# .env.docker
DEBUG=False
SECRET_KEY=<generate-strong-key>
ALLOWED_HOSTS=yourdomain.com,www.yourdomain.com
```

### 2. Use Production Server

Update `docker-compose.yaml` Django command:
```yaml
command: gunicorn jovoclient.wsgi:application --bind 0.0.0.0:8000 --workers 4
```

### 3. Enable HTTPS

Add Nginx or Traefik as reverse proxy with SSL termination.

### 4. Secure Secrets

Use Docker secrets or external secret management (AWS Secrets Manager, HashiCorp Vault).

## 📊 Monitoring

### Health Checks

```bash
# Check all service health
docker-compose ps

# Kafka Connect health
curl http://localhost:8083/

# Schema Registry health
curl http://localhost:8081/
```

### Resource Usage

```bash
# View resource consumption
docker stats

# Specific container
docker stats django-web
```

## 🔄 Scaling

### Scale Celery Workers

```bash
# Scale consumer workers to 3
docker-compose up -d --scale celery-consumer=3

# Scale main workers to 5
docker-compose up -d --scale celery-worker=5
```

### Scale Kafka

Update `docker-compose.yaml` to add more Kafka brokers and adjust replication factors.

## 🧹 Cleanup

### Stop Services

```bash
# Stop all containers
docker-compose down

# Stop and remove volumes (⚠️ deletes all data)
docker-compose down -v
```

### Remove Images

```bash
# Remove project images
docker rmi debezium-setup_django

# Prune unused images
docker image prune -a
```

## 📚 Additional Resources

- [Django Documentation](https://docs.djangoproject.com/)
- [Celery Documentation](https://docs.celeryproject.org/)
- [Debezium Documentation](https://debezium.io/documentation/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Docker Compose Reference](https://docs.docker.com/compose/compose-file/)

## 🤝 Contributing

When making changes:

1. Test locally with `docker-compose up --build`
2. Update this README if adding services
3. Update `.env.docker` template if adding environment variables
4. Document breaking changes in commit messages

## 📝 License

[Your License Here]
