# DE-Final-Project

Итоговый проект по учебной дисциплине **«Python для инженерии данных»**.

Проект разворачивается как мультиконтейнерное приложение в Docker и включает:
- **PostgreSQL** — основная БД для нормализованных таблиц и витрин;
- **pgAdmin** — интерфейс для проверки таблиц и выполнения запросов;
- **Apache Airflow** — оркестратор ETL/ELT-процессов.

## Структура проекта

- `sql/init` — SQL-скрипты инициализации PostgreSQL (создание схем и таблиц)
- `dags` — DAG'и Airflow
- `src/etl` — загрузка данных в `core`
- `src/datamarts` — расчет витрин `dm`
- `data/raw` — исходные parquet-файлы
- `logs` — логи Airflow
- `plugins` — плагины Airflow
- `scripts` — вспомогательные shell-скрипты для контейнеров
- `Dockerfile` — образ для сервисов Airflow
- `docker-compose.yml` — описание сервисов проекта

## Требования

Перед запуском убедитесь, что у вас установлены:
- Docker Desktop
- Docker Compose (или встроенная команда `docker compose`)
- Git

## Подготовка окружения

### 1. Клонирование репозитория

```bash
git clone <URL_репозитория>
cd DE-Final-Project
```

### 2. Создание `.env`

Если в проекте есть файл `.env.example`, создайте `.env` на его основе:

```bash
cp .env.example .env
```

Если `.env.example` ещё не добавлен, создайте `.env` вручную со следующими переменными:

```env
POSTGRES_DB=final_project
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_PORT=5433

PGADMIN_DEFAULT_EMAIL=admin@example.com
PGADMIN_DEFAULT_PASSWORD=admin123

AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin123
AIRFLOW_ADMIN_FIRSTNAME=Admin
AIRFLOW_ADMIN_LASTNAME=User
AIRFLOW_ADMIN_EMAIL=admin@example.com
```

## Запуск проекта

Основной способ запуска:

```bash
docker compose up --build
```

Если у вас используется старая форма команды, можно запустить так:

```bash
docker-compose up --build
```

При первом запуске:
- поднимется PostgreSQL;
- будут выполнены SQL-скрипты из `sql/init`;
- выполнится инициализация Airflow;
- будет создан администратор Airflow;
- после этого стартуют `airflow-webserver` и `airflow-scheduler`.

## Остановка проекта

Остановить контейнеры:

```bash
docker compose down
```

Остановить контейнеры и удалить volumes:

```bash
docker compose down -v
```

Полная пересборка проекта:

```bash
docker compose down -v
docker compose up --build
```

## Сервисы

После успешного запуска будут доступны:

- **Airflow**: http://localhost:8080
- **pgAdmin**: http://localhost:5050
- **PostgreSQL**: `localhost:5433`

## Учетные данные

### Airflow
Используются значения из `.env`:
- логин: `AIRFLOW_ADMIN_USERNAME`
- пароль: `AIRFLOW_ADMIN_PASSWORD`

### pgAdmin
Используются значения из `.env`:
- email: `PGADMIN_DEFAULT_EMAIL`
- пароль: `PGADMIN_DEFAULT_PASSWORD`

### PostgreSQL
Используются значения из `.env`:
- база данных: `POSTGRES_DB`
- пользователь: `POSTGRES_USER`
- пароль: `POSTGRES_PASSWORD`
- порт: `POSTGRES_PORT`

## DAG'и

В проекте предусмотрены следующие DAG'и:

- `etl_core_dag` — чтение parquet-файлов и загрузка нормализованных данных в слой `core`
- `build_datamarts_dag` — построение витрин слоя `dm`

## Проверка после запуска

После старта проекта рекомендуется проверить:

1. Что все контейнеры поднялись:

```bash
docker ps
```

2. Что Airflow открылся в браузере на `http://localhost:8080`

3. Что pgAdmin открылся в браузере на `http://localhost:5050`

4. Что в Airflow появились DAG'и проекта

## Полезные команды

Логи всех сервисов:

```bash
docker compose logs
```

Логи конкретного сервиса:

```bash
docker compose logs postgres
docker compose logs airflow-webserver
docker compose logs airflow-scheduler
docker compose logs pgadmin
```

Запуск в фоне:

```bash
docker compose up --build -d
```
