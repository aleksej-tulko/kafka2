# Описание

kafka_streams фильтрует сообщения, приходящие в топик *messages*, сверяет со списком блокировок отправителей и перемещает в топик *filtered messages*. 

Отправители и получатели: авторами и получателями будут пользователи *clown*, *payaso*, *dodik*, *spammer*.

Запрещенные слова: *spam*,*skam*,*windows*.

Цензура сообщений: списки блокировок для каждого получателя перманетно хранится на диске в RocksDB. Данные для восстановления БД в случае сбоя и перезапуска программы хранятся в топике *blocked-users-changelog*.

Обработка сообщений: при перемещении из топика *messages* в *filtered_messages* сообщения обрабатываются с помощью процессоров *lower_str_input* (переводит строки в сообщении в нижний регистр) и *mask_bad_words* (маскирует запрещенные слова).

Логгирование: в стандартный вывод передается информация о создании или изменении блокировки и сообщение о количестве отправленных сообщений в топик *messages* за время жизни окна.

Веб UI для отслеживания работы программы доступен по адресу *http://$server_ip:8080*.

## Требования

- **OS**: Linux Ubuntu 24.04 aarm
- **Python**: Python 3.12.3
- **Docker**: 28.2.2 - https://docs.docker.com/engine/install/ubuntu/

## Подготовка к запуску

1. Склонировать репозиторий:
    ```bash
    git clone git@github.com:aleksej-tulko/kafka2.git
    ```
2. Добавить сабмодуль для генерации сообщений:
    ```bash
    cd kafka2
    git submodule add https://github.com/aleksej-tulko/kafka_1.git compose
    ```
3. Перейти в директорию с сабмодулем:
    ```bash
    cd compose
    ```
4. Создать файл .env c переменными:
    ```env
    ACKS_LEVEL='all'
    AUTOOFF_RESET='earliest'
    ENABLE_AUTOCOMMIT=False
    FETCH_MIN_BYTES=400
    FETCH_WAIT_MAX_MS=100
    RETRIES=3
    SESSION_TIME_MS=6000
    TOPIC='messages'
    ```
5. Создать папки на хосте:
    ```bash
    mkdir -p /opt/kafka/kafka_1/
    mkdir -p /opt/kafka/kafka_2/
    mkdir -p /opt/kafka/kafka_3/
    mkdir -p /opt/zookeeper/data
    mkdir -p /opt/zookeeper/log
    chown 1000:1000 /opt/kafka/ -R
    chown 1000:1000 /opt/zookeeper/ -R
    ```

6. Создать docker network:
    ```bash
    docker network create kafka-network
    ```

7. Запустить сервисы:
    ```bash
    docker compose up zookeeper kafka_1 kafka_2 kafka_3 kafka-ui
    ```
    При первом запуске до создания топика программу **app** запускать не надо. После создания топика при последуюший перезапусках можно использовать
    ```bash
    docker compose up -d
    ```

8. Создать нужные топики:
    ```bash
    sudo docker exec -it compose-kafka_1-1 kafka-topics --create --topic filtered_messages --partitions 2 --replication-factor 2 --bootstrap-server localhost:9092 && sudo docker exec -it compose-kafka_1-1 kafka-topics --create --topic blocked_users --partitions 2 --replication-factor 2 --bootstrap-server localhost:9092 && sudo docker exec -it compose-kafka_1-1 kafka-topics --create --topic messages --partitions 2 --replication-factor 2 --bootstrap-server localhost:9092 && sudo docker exec -it compose-kafka_1-1 kafka-topics --create --topic pract-task-3-messages_frequency-key_index-changelog --partitions 2 --replication-factor 2 --bootstrap-server localhost:9092
    ```

10. Добавить списки блокировок:
    ```bash
    echo '{"blocker":"clown", "blocked":["dodik", "spammer"]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic blocked_users
    echo '{"blocker":"spammer", "blocked":["dodik"]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic blocked_users
    echo '{"blocker":"dodik", "blocked":["clown", "payaso"]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic blocked_users
    echo '{"blocker":"payaso", "blocked":["spammer"]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic blocked_users
    ```

10. Запустить генератор сообщений.
    ```bash
    docker compose up app -d
    ```

11. Создать рабочее окружение и активировать его:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
12. Установить зависимости:
    ```bash
    pip install -r requirements.txt
    ```

13. Запустить программу для сортировки и цензуры сообщений.
    ```bash
    faust -A kafka_streams worker -l INFO
    ```

## Автор
[Aliaksei Tulko](https://github.com/aleksej-tulko)