# ДЗ № 4. Feature Store

В роли источника данных использован фрагмент данных из ДЗ №3 в виде файла parquet (`feature_store/feature_repo/data/fraud_snap.parquet`). Файл содержит 10000 записей о транзакциях пользователей.

Все примеры создания витрины и обновления схем данных в репозитории Feature Store находятся в файле `feature_store/feature_repo/example_repo.py`.

Конфигурация **Feast** в файле `feature_store/feature_repo/feature_store.yaml` содержит настройки для подключения к базе данных и определения пути к репозиторию Feature Store. В данном случае используется файловая система хранения данных в файлах **SQLite**.

В ноутбуке `example.ipynb` приведен пример использования Feature Service для получения консистентных признаков.

Для запуска примера необходимо выполнить следующие действия:

1. Инициализировать Poetry с помощью команды `poetry env use python3.11` и установить зависимости с помощью `poetry install`.
2. В папке `feature_store/feature_repo` запустить команду `poetry run feast apply` для применения изменений в репозитории Feature Store.
3. Запустить ноутбук `example.ipynb` в Jupyter Notebook.
4. Для переноса данных из offline в online хранилище необходимо выполнить команду `poetry run feast materialize --end-date=now`.
5. Для запуска UI необходимо выполнить команду `poetry run feast ui`. Затем открыть ссылку `http://localhost:8888` в браузере.