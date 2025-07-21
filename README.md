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
    sudo mkdir -p /opt/kafka/kafka_1/
    sudo mkdir -p /opt/kafka/kafka_2/
    sudo mkdir -p /opt/kafka/kafka_3/
    sudo mkdir -p /opt/zookeeper/data
    sudo mkdir -p /opt/zookeeper/log
    sudo chown 1000:1000 /opt/kafka/ -R
    sudo chown 1000:1000 /opt/zookeeper/ -R
    ```

6. Создать docker network:
    ```bash
    sudo docker network create kafka-network
    ```

7. Запустить сервисы:
    ```bash
    sudo docker compose up zookeeper kafka_1 kafka_2 kafka_3 kafka-ui -d
    ```
    При первом запуске до создания топика программу **app** запускать не надо. После создания топика при последуюший перезапусках можно использовать
    ```bash
    sudo docker compose up -d
    ```

8. Создать нужные топики:
    ```bash
    sudo docker exec -it compose-kafka_1-1 kafka-topics --create --topic filtered_messages --partitions 1 --replication-factor 2 --bootstrap-server localhost:9092 && sudo docker exec -it compose-kafka_1-1 kafka-topics --create --topic blocked_users --partitions 1 --replication-factor 2 --bootstrap-server localhost:9092 && sudo docker exec -it compose-kafka_1-1 kafka-topics --create --topic messages --partitions 1 --replication-factor 2 --bootstrap-server localhost:9092
    ```

9. Спуститься обратно в директорию kafka2/ и оздать рабочее окружение и активировать его:
    ```bash
    cd ../
    python3 -m venv venv
    source venv/bin/activate
    ```
10. Установить зависимости:
    ```bash
    pip install -r requirements.txt
    ```

11. Запустить программу для сортировки и цензуры сообщений:
    ```bash
    faust -A kafka_streams worker -l INFO
    ```

12. В другом окне терминала добавить запрещенные слова:

    ```bash
    echo '{"words": ["loh"]}' | docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic bad_words
    echo '{"words": ["durak"]}' | docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic bad_words
    echo '{"words": ["chert"]}' | docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic bad_words
    ```

    Списки можно менять, это просто пример.

13. Добавить список блокировок:
    ```bash
    echo '{"blocker":"clown", "blocked":["dodik", "spammer"]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic blocked_users
    echo '{"blocker":"spammer", "blocked":[]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic blocked_users
    echo '{"blocker":"dodik", "blocked":["spammer", "payaso"]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic blocked_users
    echo '{"blocker":"payaso", "blocked":["spammer"]}' | sudo docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic blocked_users
    ```

    Списки можно менять, это просто пример.


14. Проверить фильтрацию сообщений:

    ```bash
    echo '{"sender_id":228,"sender_name":"clown","recipient_id":69,"recipient_name":"dodik","amount":1.75,"content":"loh"}' | docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic messages
    echo '{"sender_id":228,"sender_name":"dodik","recipient_id":69,"recipient_name":"payaso","amount":1.75,"content":"durak"}' | docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic messages
    echo '{"sender_id":228,"sender_name":"payaso","recipient_id":69,"recipient_name":"spammer","amount":1.75,"content":"chert"}' | docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic messages
    echo '{"sender_id":228,"sender_name":"clown","recipient_id":69,"recipient_name":"dodik","amount":1.75,"content":"labubu"}' | docker exec -i compose-kafka_1-1 kafka-console-producer --broker-list localhost:9092 --topic messages
    ```

    Ожидаемый результат: три первый сообщения будут зацензурированы, четвертое - нет.

15. Запустить генератор сообщений.
    ```bash
    sudo docker compose up app -d
    ```

16. Проверить работу блокировок из пункта 13, открыв топик filtered_messages. Сообщения от отправителя spammer не доходят никому, до получателя spammer доходят сообщения от всех отправителей.


## Автор
[Aliaksei Tulko](https://github.com/aleksej-tulko)