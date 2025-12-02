### Приложение с RabbitMQ (producer / consumer / DLQ)

Простое приложение на Python, которое генерирует и обрабатывает сообщения через RabbitMQ, используя Docker Compose.

- **Producer**: периодически отправляет сообщения с операциями для расчёта.
- **Consumer**: обрабатывает сообщения, пишет результат в лог и при ошибке отправляет сообщение в DLQ.

### Требования

- Установленный Docker
- Установленный Docker Compose

### Запуск

В корне репозитория выполните:

```bash
docker-compose up --build
```

Будут подняты три сервиса:

- RabbitMQ (AMQP на `5672`, веб‑интерфейс на `15672`, логин/пароль: `user` / `password`)
- Producer (отправляет сообщения в exchange `calc_exchange`)
- Consumer (чтение из очереди `calc_queue`, DLQ — очередь `calc_dlq`)

Остановить и удалить контейнеры:

```bash
docker-compose down
```

### Просмотр логов

Логи producer:

```bash
docker-compose logs -f producer
```

Логи consumer:

```bash
docker-compose logs -f consumer
```

В логах producer будут видны отправленные сообщения (`id`, `operation`, `a`, `b`),
в логах consumer — успешные расчёты и ошибки.

### Проверка DLQ

В producer специально иногда генерируется операция `unknown`, которую consumer не умеет обрабатывать.
Такие сообщения отклоняются (`basic_reject` с `requeue=False`) и попадают в DLQ.

Как посмотреть DLQ:

1. Откройте веб‑интерфейс RabbitMQ: `http://localhost:15672`
2. Войдите: логин `user`, пароль `password`
3. Перейдите во вкладку **Queues** и найдите очередь `calc_dlq`
4. Убедитесь, что в ней появляются сообщения после ошибок обработки
