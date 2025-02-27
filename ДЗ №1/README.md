# Домашнее задание №1

## 1. Формулирование целей антифрод-системы

Основная цель — выявлять мошеннические транзакции в реальном времени, минимизируя потери компании и клиентов.

Ключевые требования и целевые показатели:

* Обеспечить покрытие, сопоставимое с конкурентами: не менее 2% мошеннических транзакций среди всех обработанных, приводящих к потере денежных средств.
* Снизить месячный ущерб от мошенничества до 500 тыс. руб. или ниже.
* Минимизировать долю ложных срабатываний (False Positive Rate, FPR) ≤ 5%, чтобы избежать оттока клиентов.
* Масштабируемость: система должна обрабатывать от 50 до 400 транзакций в секунду + 15% (с учетом запаса на увеличение клиентской базы в ближайшие пол-года).
* Соответствие требованиям безопасности: защита конфиденциальных данных клиентов.
* Размещение в облаке, так как представители компании не готовы размещать разрабатываемый модуль на
собственных вычислительных ресурсах.

## 2. Метрики антифрод-системы

### 1. Офлайн-метрики

    Эти метрики используются на этапе разработки и тестирования модели на исторических данных.

* **Precision (Точность)** – показывает, насколько предсказанные мошеннические транзакции действительно являются мошенническими. Метрика важна, потому что высокая точность уменьшает количество ложных срабатываний (False Positives).

$$ Precision = \frac{TP}{TP + FP} $$

* **Recall (Полнота)** – показывает, какую долю мошеннических транзакций модель смогла правильно выявить. Метрика важна, потому что низкая полнота означает, что система пропускает мошенников.

$$ Recall = \frac{TP}{TP + FN} $$

* **F1-Score** – баланс между Precision и Recall. Метрика важна, так как учитывает одновременно и ложные срабатывания, и пропущенные мошеннические транзакции

$$ F1 = 2 \times \frac{Precision \times Recall}{Precision + Recall} $$

* **PR-AUC (Precision-Recall AUC)** – интегральная метрика, которая полезнее ROC-AUC при сильном дисбалансе классов (как раз в нашем случае), так как акцентирует внимание на редком классе (мошенничестве). Данная метрика позволяет оценить качество модели в целом вне зависимости от установления порога вероятности для отнесения транзакции к мошеннической.

$$ PR-AUC = \int_{0}^{1} Precision(Recall) \, d(Recall) $$

* **False Positive Rate (FPR, доля ложных тревог)** - одна из основных метрик качества модели, так как при FPR > 5% начнется отток клиентов.

$$ FPR = \frac{FP}{FP + TN} $$

Для подбора гиперпараметров модели предлагается использовать метрику **F1**. Для оценки модели на тестовой выборке предлагается к метрике **PR-AUC** добавить метрику **FPR**.

### 2. Прокси-метрики (метрики влияния модели на бизнес)

* **Cost per Fraudulent Transaction Detected** - цена обнаружения одной мошеннической транзакции.Включает стоимость инфраструктуры и ложных блокировок.

* **Conversion Rate** - конверсия в успешные транзакции. Не должна снижаться после внедрения антифрод-модели.

$$ Conversion\_Rate = \frac{Approved\_Transactions}{Total\_Transactions} $$

* **Churn Rate** - отток клиентов. Если система слишком `агрессивно` блокирует платежи, клиенты могут уйти.

$$ Churn\_Rate = \frac{Customers_{start} - Customers_{end}}{Customers_{start}} $$

* **Customer Support Requests** - количество запросов в поддержку, связанных с выполнением транзакций (долгие транзакции, частое блокирование и т.д.). Рост числа таких обращений может сигнализировать о проблемах с моделью.

$$ Support\_Requests\_Rate = \frac{Total\_Support\_Requests}{Total\_Transactions} $$

* **Fraudster Adaptation Rate** - скорость адаптации мошенников. Если после внедрения системы процент мошенничества снова начинает расти, значит, мошенники нашли способы обхода.

$$ Fraudster\_Adaptation\_Rate = \frac{Fraudulent\_Transactions_{new}}{Fraudulent\_Transactions_{old}} $$

### 3. Онлайн-метрики (метрики для мониторинга модели в проде)

* **Request count** - количество транзакций в секунду времени. 

* **Response Time** - время отклика системы. Должно быть ≤ 1 секунда, чтобы не замедлять платежи.

* **Accuracy** — доля правильно определенных мошеннических транзакций среди всех проводимых транзакций. До 2%.

* **False Positive Rate (FPR, доля ложных тревог)** - одна из основных метрик качества модели, так как при FPR > 5% начнется отток клиентов.

* **Cumulative Missed Fraud Cost** - суммарный ущерб от пропущенных моделью мошеннических транзакций в месяц.

* **Cumulative Save Fraud Cost** - суммарный возможный ущерб от правильно определенных моделью мошеннических транзакций в месяц.

## 3. Анализ с использованием MISSION Canvas

* `MISSION Canvas` помогает систематизировать ключевые аспекты проекта и выделить его особенности. 
Процесс анализа будет включать следующие компоненты:

**Миссия:** 

* Cоздать систему машинного обучения для предсказания мошеннических транзакций в реальном времени, которая поможет предотвратить финансовые потери клиентов и компании.

**Цели:**

* Минимизация убытков от мошенничества (до 500 тыс. руб. в месяц).
* Снижение количества ложных срабатываний (False Positive) до 5%.
* Обработка до 460 транзакций в секунду в пиковые моменты.
* Защита данных клиентов.


**Технологии:**

* Машинное обучение для классификации транзакций (например, логистическая регрессия, случайный лес, нейронные сети).
* Облачные технологии для масштабируемости (например, VK Cloud, Yandex Cloud).
* Инструменты: Python, TensorFlow/PyTorch, Docker, Kubernetes для развертывания модели в облаке.

**Ожидаемые результаты:**

* Система, способная классифицировать транзакции как мошеннические с минимальными ложными срабатываниями и максимальной точностью.
* Уменьшение количества мошенничества до уровня конкурентов или ниже.
* Низкая задержка при обработке транзакций.

**Оценка успеха:**

* Высокие показатели по метрикам: Точность модели, F1-score, PR-AUC, FPR.
* Время отклика системы на обработку одной транзакции (в реальном времени).
* Низкий процент ложных срабатываний (False Positive Rate) <= 5%.
* Сумма ущерба от пропущенных моделью транзакций в месяц в руб. ниже чем у конкурентов (500 т.руб./мес)

## 4. Декомпозиция системы и определение функциональных частей

Проект можно разбить на несколько ключевых компонентов:

1. Обработка данных:

* Загрузка и очистка данных о транзакциях.
* Анонимизация и защита конфиденциальных данных клиентов.

2. Модуль классификации:

* Модели машинного обучения для классификации транзакций как мошеннические или нет.
* Применение алгоритмов для обработки дисбаланса классов.

3. Интерфейс с пользователем и системой:

* Интеграция с текущей системой платежей для мониторинга транзакций в реальном времени.
* Оповещения о подозрительных транзакциях.

4. Облачная инфраструктура:

* Облачное размещение модели для масштабируемости.
* Мониторинг производительности и точности системы.

## 5. Задачи для реализации с учетом принципа S.M.A.R.T.

Задачи определены и занесены на Kanban-доску -> https://github.com/users/Vanarty/projects/1
