# DE-Final-Project

Итоговый проект по учебной дисциплине "Python для Инженерии Данных"

Каркас проекта для ветки `develop`.

## Структура

- `sql/init` — создание схем и таблиц
- `dags` — DAG'и Airflow
- `src/etl` — загрузка данных в `core`
- `src/datamarts` — расчет витрин `dm`
- `data/raw` — исходные parquet-файлы

## Быстрый старт

```bash
docker compose up --build
```

## Сервисы

- Airflow: http://localhost:8080
- pgAdmin: http://localhost:5050
- PostgreSQL: localhost:5433

## Учетные данные по умолчанию

Креды ко всем сервисам для Docker указаны в .env.example. Перед началом работы необходимо создать .env от .env.example:

```ba
cp .env.example .env
```

## DAG'и

- `etl_core_dag` — загрузка parquet в `core`
- `build_datamarts_dag` — расчет витрин `dm`
