### 01/03/2026
# Медицинское DICOM-приложение, техническое задание 

• - FCM — Firebase Cloud Messaging (Google): push-уведомления для Android (и частично iOS через Firebase).
  - APNS — Apple Push Notification service: официальный push-канал Apple для iOS.
  - in-app — внутренние уведомления внутри самого приложения (баннер/бейдж/обновление списка), когда приложение уже открыто.

  Как это работает вместе:

  - сервер отправляет событие,
  - через FCM/APNS “будит” устройство или показывает системное уведомление,
  - приложение открывается/просыпается и делает нужные API-запросы,
  - in-app обновляет интерфейс сразу внутри приложения.

  Зачем все 3:

  - FCM/APNS — доставка, когда приложение в фоне/закрыто,
  - in-app — лучший UX, когда приложение уже на экране.

# Описание сервиса в целом

Система предназначена для быстрой и надежной доставки медицинских DICOM-исследований врачу на мобильное устройство без стриминга, с приоритетом мгновенного открытия уже загруженных данных. На больничном компьютере Python-скрипт автоматически (по расписанию) и вручную (через CLI) получает исследования из PACS, выполняет полный разбор DICOM и заранее рассчитывает все необходимые данные для просмотра (серии, порядок срезов, windowing, геометрию для MPR, контрольные суммы). После этого формируется единый архив исследования в формате `zstd`, который загружается в S3.

Бекенд на Go принимает метаданные, хранит данные исследований в PostgreSQL, управляет доступом к архивам, а также публикует события о новых исследованиях, отчетах и планах операций в Kafka. Отчеты о дежурстве и планы операций принимаются от скрипта через backend API и сохраняются в MongoDB, откуда отдаются в мобильное приложение через backend. Для мобильных клиентов используется отдельный слой `Notification Gateway`: он читает события из Kafka, фильтрует их по пользователю и доставляет уведомления в приложение через push/in-app канал. Мобильное приложение не подключается напрямую к Kafka.

Если в настройках пользователя включен флаг `downloading_auto`, приложение автоматически запрашивает у бэкенда короткоживущий signed URL и скачивает архив, затем распаковывает его и подготавливает к просмотру в фоне. Если флаг выключен, исследование только появляется в листинге и загружается вручную по команде врача через тот же механизм signed URL. После завершения подготовки пользователь получает уведомление, а исследование открывается мгновенно, без повторного тяжелого анализа DICOM на телефоне.

Дополнительно система поддерживает отправку отчетов о дежурстве и недельных планов операций с больничного ПК на бэкенд и в мобильное приложение, а также операции управления исследованиями (повторное скачивание, удаление локальной копии, удаление из S3, отправка на удаленный PACS). Хранение архивов в S3 ограничено 7 днями, после чего выполняется автоматическая очистка.


## Executive Summary

- Система доставляет DICOM-исследования на мобильное устройство без стриминга, с офлайн-доступом.
- Полный DICOM-парсинг и предрасчеты выполняются на больничном Python-скрипте до архивации.
- Исследование упаковывается в единый `zstd`-архив с `manifest.json` и загружается в S3.
- Бэкенд на Go хранит метаданные исследований в PostgreSQL и управляет доступом к архивам.
- Отчеты о дежурстве и планы операций идут по цепочке `Script -> Backend -> MongoDB -> Mobile App`.
- События публикуются в Kafka, мобильные клиенты не подключаются к Kafka напрямую.
- `Notification Gateway` читает Kafka и доставляет события на устройства через FCM/APNS/in-app.
- При `downloading_auto=true` архив скачивается автоматически; при `false` — только ручная загрузка из листинга.
- В архиве фиксируются `manifest_version`, `dicom_order` и обязательные предрасчеты для мгновенного открытия.
- Повторная загрузка доступна через on-demand short-lived signed URL, выдаваемый бэкендом.
- Срок хранения архивов в S3 — 7 дней с автоматической очисткой.

---

## Питч (2 минуты)

Мы строим медицинский мобильный DICOM-вьюер, в котором врач получает исследование быстро, открывает его мгновенно и может работать даже без сети.

Ключевой принцип — не нагружать телефон тяжелой обработкой. Весь сложный этап мы переносим на больничный компьютер: Python-скрипт получает исследование из PACS, разбирает DICOM, заранее рассчитывает данные для просмотра (включая порядок срезов, параметры windowing и геометрию для MPR) и формирует готовый архив.

Далее бэкенд на Go сохраняет метаданные в PostgreSQL, хранит архив в S3 и публикует событие в Kafka. Для отчетов и планов операций backend сохраняет данные в MongoDB и отдает их мобильному приложению через API. Отдельный `Notification Gateway` читает события и доставляет их на мобильные устройства через push/in-app каналы. Это надежная и безопасная модель: мобильное приложение не работает с Kafka напрямую.

Если у пользователя включен автоматический режим, архив загружается и подготавливается в фоне. Если выключен — исследование появляется в списке и скачивается вручную. В обоих случаях после подготовки исследование открывается сразу, без повторного анализа DICOM на телефоне.

В результате получаем практичную и масштабируемую систему: высокая скорость работы для врача, предсказуемая архитектура для команды и контролируемая стоимость хранения за счет политики 7 дней в S3.

## Оглавление

0. [Executive Summary](#executive-summary)
1. [Общая концепция](#1-общая-концепция)
2. [Основные особенности приложения](#2-основные-особенности-приложения)
3. [Проблемы и решения](#3-проблемы-и-решения)
4. [Архитектура системы](#4-архитектура-системы)
5. [Компоненты системы](#5-компоненты-системы)
6. [База данных](#6-база-данных)
7. [API Endpoints](#7-api-endpoints)
8. [Форматы данных](#8-форматы-данных)
9. [Потоки данных](#9-потоки-данных)
10. [Технологический стек](#10-технологический-стек)
11. [Нефункциональные требования](#11-нефункциональные-требования)
12. [Мониторинг и метрики](#12-мониторинг-и-метрики)
13. [Обработка ошибок](#13-обработка-ошибок)
14. [Безопасность](#14-безопасность)
15. [Roadmap](#15-roadmap)
16. [Риски и митигация](#16-риски-и-митигация)
17. [Ключевые решения](#17-ключевые-решения)
18. [Приложения](#18-приложения)

---

## 1. Общая концепция

### 1.1. Описание системы

**Мобильный DICOM-вьюер для врачей** с автоматической доставкой исследований, предварительной обработкой на стороне больницы и мгновенным офлайн-доступом.

### 1.2. Ключевые принципы

- **Обработка на источнике**: Весь DICOM-парсинг выполняется на больничном скрипте
- **Асинхронность**: событийная доставка через Kafka + Notification Gateway
- **Офлайн-первый**: Полная загрузка исследований на устройство
- **Мгновенный доступ**: Пре-рендеринг после загрузки
- **Автоматизация**: Авто-загрузка исследований по настройкам

### 1.3. Целевая аудитория

- Врачи-рентгенологи
- Неврологи
- Нейрохирурги
- Кардиологи


### 1.4. Ключевые метрики успеха

| Метрика | Целевое значение |
|---------|------------------|
| Время от PACS до мобилки | < 5 минут |
| Открытие готового исследования | Мгновенно для пользователя |


---

## 2. Основные особенности приложения

### 2.1. Больничный Python скрипт

#### 2.1.1. Автоматический и ручной режим
- **Запуск**: По расписанию (ежедневно в 08:00)
- **Что выгружает**: Все исследования пациента КТ мозга (не все КТ натив подряд, обязательно наличие КТ ангиографии или КТ перфузии) за текущую дату
- **Источник**: PACS больницы (DICOM Q/R)
- **Действие**: Автоматическая выгрузка исследований без участия пользователя
- **Действие**: Ручная выгрузка определенных исследований
- **Действие**: Генерация отчета по дежурству, сохранение локально и отправка на бэкенд в JSON (доставка в мобильное приложение через брокер событий)
- **Действие**: Отправка плана операций на неделю на бэкенд в JSON (доставка в мобильное приложение через брокер событий)

#### 2.1.2. CLI команды
```bash
# Выгрузить по UID
python script.py upload --study-uid 1.2.3.4.5

# Отправить отчет о дежурстве
python script.py upload --report --preiod 3 --time 14.00

# Отправить план операций
python script.py upload --plan 
```

#### 2.1.3. Предварительная обработка DICOM
- Парсинг всех DCM файлов исследования
- Извлечение метаданных (теги, пиксельные данные)
- Вычисление min/max значений для нормализации
- Построение гистограмм для быстрого windowing
- Структурирование данных по сериям

#### 2.1.4. Сжатие исследований
- **Формат**: Zstandard (zstd)
- **Уровень сжатия**: 3 (баланс скорости и размера)
- **Ожидаемый результат**: 50-60% от исходного размера

### 2.2. Хранение данных

#### 2.2.1. S3 (Yandex Storage)
- **Что хранит**: Сжатые DICOM-исследования
- **Политика хранения**: 7 дней (авто-очистка)
- **Доступ**: Через короткоживущий signed URL, который выдается бэкендом по запросу (`GET /api/v1/studies/{id}/download-link`)

#### 2.2.2. База данных бэкенда (PostgreSQL)
- **Что хранит**: Метаданные и предобработанные данные исследований, пользователей

#### 2.2.3. Документы (MongoDB)
- **Что хранит**: Отчеты о дежурстве и планы операций
- **Путь данных**: Скрипт отправляет JSON в backend API, backend сохраняет в MongoDB
- **Доступ для мобилки**: Через backend endpoints `GET /api/v1/reports` и `GET /api/v1/plans/current`

### 2.3. Бекенд (API сервер)

#### 2.3.1. Основные функции
- Прием метаданных от скрипта
- Выдача короткоживущего signed URL для скачивания архива исследования
- Управление пользователями и настройками
- Очистка старых исследований (проверка и удаление ежедневно в 02:00)
- Отправка плана операций на мобильное приложение через брокер сообщений
- Отправка отчета по дежурству на мобильное приложение через брокер сообщений
- Отправка dicom исследований на мобильное приложение через брокер сообщений

### 2.4. Message Broker

#### 2.4.1. Топики
- `studies.new` - новые исследования
- `reports.new` - новые отчеты
- `plans.new` - новые планы операций

#### 2.4.2. Слой доставки Kafka → мобильное приложение
- Мобильное приложение **не подключается напрямую к Kafka**
- События из Kafka читает backend-компонент `Notification Gateway`
- `Notification Gateway` фильтрует события по `user_id`, проверяет ACL, логирует доставку
- Для доставки на устройство использует push-каналы (FCM/APNS) и in-app канал
- Мобилка по событию берет `study_id`, запрашивает signed URL у бэкенда и далее скачивает архив

### 2.5. Мобильное приложение

#### 2.5.1. Подписка на сообщения
- **Протокол**: Kafka topics (через backend notification service)
- **Топик**: `user.{user_id}.studies.new`
- **Действие**: Авто-загрузка или обновление списка
- `reports.new` - новые отчеты
- `plans.new` - новые планы операций

#### 2.5.2. Автоматическая загрузка (`downloading_auto=true`)
1. Получить `study_id` из сообщения брокера и запросить signed URL у бэкенда
2. Скачать сжатое исследование из S3
3. Распаковать (zstd)
4. Сохранить локально
5. Выполнить пре-рендеринг в фоне
6. Показать push-уведомление

#### 2.5.3. Ручная загрузка (`downloading_auto=false`)
1. Исследование появляется только в листинге
2. Пользователь нажимает "Скачать"
3. Запросить signed URL у бэкенда (`GET /studies/{id}/download-link`)
4. Скачать сжатое исследование из S3
5. Распаковать (zstd)
6. Сохранить локально
7. Выполнить пре-рендеринг в фоне
8. Показать push-уведомление

#### 2.5.4. Функции просмотра
- Навигация по срезам (для КТ)
- MPR (мультипланарная реконструкция)
- Windowing в реальном времени
- Измерения (расстояния)
- Выбор серий
- Автопроигрывание (для XA)

#### 2.5.5. Управление исследованиями
- 📥 Скачать
- 📤 Отправить на удаленный PACS
- 🗑️ Удалить с устройства
- ❌ Удалить из S3

---

## 3. Проблемы и решения

| Проблема | Решение |
|----------|---------|
| Долгое открытие исследований на мобилке | Полный DICOM-парсинг и предрасчёты выполняются на больничном скрипте до архивации |
| Сложность работы с тысячами DICOM-файлов | Единый архив `zstd` с `manifest.json` и предобработанными данными |
| Риск некорректного порядка срезов | Явная фиксация `dicom_order` в manifest и проверки качества перед публикацией |
| Нестабильная доставка событий на устройства | Kafka + `Notification Gateway` + push/in-app доставка |
| Повторная загрузка в течение недели | On-demand signed URL с коротким TTL и повторным выпуском по запросу |

---

## 4. Архитектура системы

### 4.1. Диаграмма компонентов

``` bash
┌──────────────────────────────────────────────────────────────┐
│                    БОЛЬНИЧНЫЙ КОМПЬЮТЕР                      │
├──────────────────────────────────────────────────────────────┤
│  Python Script                                               │
│  ├── PACS Connector (DICOM Q/R, C-STORE)                     │
│  ├── DICOM Parser (pydicom, полный парсинг)                  │
│  ├── Data Packager (zstd компрессия)                         │
│  ├── Report Generator (отчеты о дежурстве)                   │
│  ├── Plan Manager (планы операций)                           │
│  └── CLI Handler (ручные команды)                            │
└──────────────────────────────────────────────────────────────┘
                              │
                              │ HTTPS API
                              ↓
┌──────────────────────────────────────────────────────────────┐
│                         БЕКЕНД                               │
├──────────────────────────────────────────────────────────────┤
│  API Gateway (Golang)                                        │
│  ├── POST /api/v1/studies (прием метаданных)                 │
│  ├── GET  /api/v1/studies (список для мобилки)               │
│  ├── GET  /api/v1/studies/{id} (детали исследования)          │
│  ├── GET  /api/v1/studies/{id}/download-link (signed URL)    │
│  ├── GET  /api/v1/studies/{id}/download (redirect в S3)       │
│  ├── DELETE /api/v1/studies/{id} (удаление из S3)            │
│  ├── POST /api/v1/reports (прием отчетов)                    │
│  ├── GET  /api/v1/reports (список)                           │
│  ├── POST /api/v1/plans (прием планов)                       │
│  ├── GET  /api/v1/user/settings (настройки)                  │
│  └── PUT  /api/v1/user/settings (обновление настроек)        │
│                                                              │
│  Services                                                    │
│  ├── Metadata Service (работа с БД)                          │
│  ├── Signed URL Service (short-lived URL)                    │
│  ├── PDF Converter (отчеты TXT → PDF)                        │
│  ├── Cleanup Scheduler (очистка S3)                          │
│  └── Message Producer (отправка в брокер)                    │
│                                                              │
│  Database (PostgreSQL)                                       │
│  └── Tables: studies, users, audit_log                       │
│                                                              │
│  Document Store (MongoDB)                                    │
│  └── Collections: reports, surgery_plans                     │
└──────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    ↓                   ↓
        ┌──────────────────┐  ┌──────────────────┐
        │  Yandex S3       │  │ Message Broker   │
        │  └── studies/    │  │ (Kafka)          │
        │                  │  │ ├── studies.new  │
        │                  │  │ ├── reports.new  │
        └──────────────────┘  │ └── plans.new    │
                    ↑         └──────────────────┘
                    │                   │
                    │                   │ Kafka event
                    │                   ↓
                    │         ┌────────────────────────┐
                    │         │ Notification Gateway   │
                    │         │  ├── Kafka Consumer    │
                    │         │  ├── ACL Filter        │
                    │         │  └── Push Dispatcher   │
                    │         └────────────────────────┘
                    │                   │
                    │                   │ FCM/APNS/In-App
                    │                   ↓
┌──────────────────────────────────────────────────────────────┐
│                    МОБИЛЬНОЕ ПРИЛОЖЕНИЕ                      │
├──────────────────────────────────────────────────────────────┤
│  Data Layer                                                  │
│  ├── Notification Client (получение событий)                 │
│  ├── Retrofit (API calls)                                    │
│  ├── Download Manager (загрузка из S3)                       │
│  ├── Zstd Decompressor (распаковка)                          │
│  ├── Local Storage (Room + файловая система)                 │
│  └── Render Engine (OpenGL/Metal)                            │
│                                                              │
│  Domain Layer                                                │
│  ├── AutoDownloadService (фоновый сервис)                    │
│  ├── PreRenderUseCase (пре-рендеринг)                        │
│  ├── SendToPACSUseCase (отправка на PACS)                    │
│  └── NotificationManager (push + in-app)                     │
│                                                              │
│  Presentation Layer                                          │
│  ├── StudiesListFragment (список исследований)               │
│  ├── StudyDetailFragment (просмотр с MPR)                    │
│  ├── ReportsListFragment (список отчетов)                    │
│  ├── PlansListFragment (список планов)                       │
│  └── SettingsFragment (настройки)                            │
└──────────────────────────────────────────────────────────────┘
```

### 4.2. Техническая архитектура

#### 4.2.1. Уровни абстракции

**Уровень 1: Клиентский (Больница)**
- Python скрипт
- PACS Connector
- Локальное хранилище для retry

**Уровень 2: Облачный бэкенд**
- API Gateway
- Services
- Database
- Message Broker
- Notification Gateway

**Уровень 3: Хранилище**
- S3 Object Storage

**Уровень 4: Клиентский (Мобилка)**
- Presentation Layer
- Domain Layer
- Data Layer

---

## 5. Компоненты системы

### 5.1. Больничный скрипт (Python)

#### 5.1.1. Основные классы

```python
class PACSConnector:
    """Подключение к PACS больницы"""
    def find_studies(query_params) -> List[Study]
    def retrieve_study(study_uid) -> bytes
    def send_to_pacs(study_files, remote_config) -> bool

class DICOMParser:
    """Парсинг DICOM файлов"""
    def parse_study(dicom_data) -> ParsedStudy
    def extract_metadata(dicom_dataset) -> Metadata
    def extract_pixel_data(dicom_dataset) -> np.ndarray
    def calculate_histogram(pixel_data) -> dict
    def normalize_pixels(pixel_data, min_val, max_val) -> np.ndarray

class DataPackager:
    """Упаковка и сжатие"""
    def package_study(parsed_study) -> bytes
    def compress(data: bytes) -> bytes  # zstd
    def create_archive_structure(study) -> dict

class ReportGenerator:
    """Генерация отчетов о дежурстве"""
    def generate_shift_report() -> dict
    def load_local_txt() -> str
    def prepare_for_upload() -> dict

class PlanManager:
    """Управление планами операций"""
    def load_local_plan(week_number) -> dict
    def validate_plan(plan_data) -> bool
    def increment_version(plan_data) -> dict

class CLIService:
    """Обработка CLI команд"""
    @click.command()
    def fetch_study(study_uid, patient_name, date_from, date_to)
    @click.command()
    def upload_report(date)
    @click.command()
    def upload_plan(week_number)
```

#### 5.1.2. Конфигурация

```python
# config.py
class Config:
    # PACS Settings
    PACS_HOST = os.getenv("PACS_HOST")
    PACS_PORT = int(os.getenv("PACS_PORT", 104))
    PACS_AET = os.getenv("PACS_AET", "HOSPITAL_AE")
    PACS_CALLING_AET = os.getenv("PACS_CALLING_AET", "SCRIPT_AE")
    
    # Backend API
    BACKEND_URL = os.getenv("BACKEND_URL", "https://api.hospital.com")
    BACKEND_API_KEY = os.getenv("BACKEND_API_KEY")
    
    # S3 Settings
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "https://storage.yandexcloud.net")
    S3_BUCKET = os.getenv("S3_BUCKET", "dicom-studies")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
    
    # Compression
    COMPRESSION_LEVEL = 3  # zstd
    COMPRESSION_FORMAT = "zstd"
    
    # Schedule
    AUTO_FETCH_HOUR = 8  # 8 AM
    AUTO_FETCH_MODALITY = "CT"  # КТ мозга
```

### 5.2. Бекенд (Golang)

#### 5.2.1. Основные модули

```go
// cmd/api/main.go
func main() {
    cfg := config.MustLoad()
    db := postgres.MustConnect(cfg.PostgresDSN)
    s3 := storage.NewS3Client(cfg.S3)
    broker := messaging.NewKafkaProducer(cfg.KafkaBrokers)

    app := httpserver.New(cfg, db, s3, broker)
    app.Run()
}

// internal/http/router.go
func NewRouter(h *Handlers) *gin.Engine {
    r := gin.New()
    api := r.Group("/api/v1")

    studies := api.Group("/studies")
    studies.POST("", h.CreateStudy)
    studies.GET("", h.ListStudies)
    studies.GET("/:id", h.GetStudy)
    studies.GET("/:id/download-link", h.GetStudyDownloadLink) // short-lived signed URL
    studies.GET("/:id/download", h.DownloadStudyArchive) // redirect в signed S3 URL
    studies.DELETE("/:id", h.DeleteStudy)

    reports := api.Group("/reports")
    reports.POST("", h.CreateReport)
    reports.GET("", h.ListReports)

    plans := api.Group("/plans")
    plans.POST("", h.CreatePlan)
    plans.GET("/current", h.GetCurrentPlan)

    user := api.Group("/user")
    user.GET("/settings", h.GetUserSettings)
    user.PUT("/settings", h.UpdateUserSettings)
    user.POST("/push-token", h.RegisterPushToken)

    return r
}

// internal/service/study.go
type StudyService interface {
    CreateStudy(ctx context.Context, in CreateStudyInput) (Study, error)
    GetStudy(ctx context.Context, id uuid.UUID) (StudyDetails, error)
    ListStudies(ctx context.Context, filter StudyFilter) ([]Study, error)
    DeleteStudy(ctx context.Context, id uuid.UUID) error
    GetStableDownloadURL(ctx context.Context, id uuid.UUID) (string, error)
}

// internal/service/messaging.go
type MessageProducer interface {
    PublishNewStudy(ctx context.Context, studyID uuid.UUID, userID uuid.UUID) error
    PublishNewReport(ctx context.Context, reportID uuid.UUID) error
    PublishNewPlan(ctx context.Context, planID uuid.UUID) error
}

// internal/notification/gateway.go
type NotificationGateway interface {
    ConsumeKafka(ctx context.Context) error
    DispatchStudyEvent(ctx context.Context, event StudyEvent) error
    DispatchReportEvent(ctx context.Context, event ReportEvent) error
    DispatchPlanEvent(ctx context.Context, event PlanEvent) error
}

type PushDispatcher interface {
    SendFCM(ctx context.Context, userID uuid.UUID, payload any) error
    SendAPNS(ctx context.Context, userID uuid.UUID, payload any) error
    SendInApp(ctx context.Context, userID uuid.UUID, payload any) error
}
```

#### 5.2.2. Middleware

```go
// internal/http/middleware/
func AuthMiddleware(jwtVerifier JWTVerifier) gin.HandlerFunc
func LoggingMiddleware(logger *zap.Logger) gin.HandlerFunc
func RateLimitMiddleware(rps int, burst int) gin.HandlerFunc
```

### 5.3. Мобильное приложение (Kotlin/Swift)

#### 5.3.1. Android 
#### 5.3.2. IOS Swift

## 6. База данных

### 6.1. Схема PostgreSQL

```sql
-- Таблица исследований
CREATE TABLE studies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    study_uid VARCHAR(64) UNIQUE NOT NULL,
    patient_name VARCHAR(255) NOT NULL,
    patient_id VARCHAR(64) NOT NULL,
    study_date TIMESTAMP NOT NULL,
    modality VARCHAR(16) NOT NULL,
    num_slices INTEGER,
    slice_thickness FLOAT,
    pixel_spacing FLOAT[],
    default_window_center INTEGER,
    default_window_width INTEGER,
    series_data JSONB,
    preprocessed_data JSONB,
    s3_key VARCHAR(512) NOT NULL,
    s3_compressed_size BIGINT,
    compressed_format VARCHAR(10) DEFAULT 'zstd',
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '7 days',
    status VARCHAR(32) DEFAULT 'active'
);

-- Таблица пользователей
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(64) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    specialty VARCHAR(128),
    push_token VARCHAR(255),
    settings JSONB DEFAULT '{
        "downloading_auto": false,
        "max_cache_size_mb": 2048,
        "notifications_enabled": true
    }',
    created_at TIMESTAMP DEFAULT NOW(),
    last_login TIMESTAMP
);

-- Таблица аудита
CREATE TABLE audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    action VARCHAR(64),
    study_id UUID REFERENCES studies(id),
    timestamp TIMESTAMP DEFAULT NOW(),
    details JSONB
);

-- Индексы для производительности
CREATE INDEX CONCURRENTLY idx_studies_patient_date 
    ON studies(patient_id, study_date DESC);

CREATE INDEX CONCURRENTLY idx_studies_date
    ON studies(study_date DESC);

CREATE INDEX CONCURRENTLY idx_studies_modality
    ON studies(modality);

CREATE INDEX CONCURRENTLY idx_studies_status
    ON studies(status);

CREATE INDEX CONCURRENTLY idx_audit_user
    ON audit_log(user_id);

CREATE INDEX CONCURRENTLY idx_audit_timestamp
    ON audit_log(timestamp DESC);
    
CREATE INDEX CONCURRENTLY idx_audit_action_time 
    ON audit_log(action, timestamp);
```

### 6.2. Схема MongoDB (документы)

```javascript
// Коллекция отчетов
db.reports.createIndex({ report_date: -1 })
db.reports.createIndex({ created_at: -1 })

// Документ отчета (пример)
{
  _id: ObjectId("..."),
  report_date: ISODate("2026-02-14"),
  doctor_name: "Иванов И.И.",
  patients_count: 12,
  emergencies: 3,
  content: {
    summary: "...",
    procedures: []
  },
  original_txt: "...",
  created_at: ISODate("2026-02-14T09:00:00Z")
}

// Коллекция планов операций
db.surgery_plans.createIndex({ year: 1, week_number: 1, version: -1 }, { unique: true })
db.surgery_plans.createIndex({ created_at: -1 })

// Документ плана операций (пример)
{
  _id: ObjectId("..."),
  week_number: 8,
  year: 2026,
  version: 1,
  plan_data: {
    monday: [],
    tuesday: []
  },
  created_at: ISODate("2026-02-14T10:00:00Z")
}
```

---

## 7. API Endpoints

### 7.1. Исследования

#### POST /api/v1/studies
**Описание**: Прием метаданных от больничного скрипта

**Request Body**:
```json
{
    "manifest_version": 1,
    "study_uid": "1.2.3.4.5",
    "patient_name": "Иванов И.И.",
    "patient_id": "123456",
    "study_date": "2024-01-15T08:30:00Z",
    "modality": "CT",
    "num_slices": 350,
    "slice_thickness": 1.25,
    "pixel_spacing": [0.5, 0.5],
    "default_window_center": 40,
    "default_window_width": 80,
    "series_data": {
        "series_1": {
            "uid": "1.2.3.4.5.1",
            "modality": "CT",
            "num_slices": 200,
            "slice_range": [0, 199]
        }
    },
    "preprocessed_data": {
        "pixel_arrays_16bit": "base64_encoded_data",
        "min_max_values": [-1000, 2000],
        "histograms": {...}
    },
    "s3_key": "studies/2026/01/15/1.2.3.4.5.zst",
    "s3_compressed_size": 262144000
}
```

**Response**: `201 Created`
```json
{
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "created"
}
```

#### GET /api/v1/studies
**Описание**: Получение списка исследований

**Query Parameters**:
- `modality` (optional: CT, XA)

**Response**: `200 OK`
```json
{
    "studies": [
        {
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "patient_name": "Иванов И.И.",
            "modality": "CT",
            "study_date": "2024-01-15T08:30:00Z",
            "num_slices": 350,
            "is_downloaded_locally": false,
            "size_mb": 250
        }
    ],
    "total": 150,
    "limit": 50,
    "offset": 0
}
```

#### GET /api/v1/studies/{id}
**Описание**: Получение деталей исследования

**Response**: `200 OK`
```json
{
    "metadata": {
        "study_uid": "1.2.3.4.5",
        "patient_name": "Иванов И.И.",
        "patient_id": "123456",
        "study_date": "2024-01-15T08:30:00Z",
        "modality": "CT",
        "num_slices": 350,
        "slice_thickness": 1.25,
        "pixel_spacing": [0.5, 0.5],
        "default_window_center": 40,
        "default_window_width": 80
    },
    "preprocessed_data": {...},
    "can_download": true
}
```

#### GET /api/v1/studies/{id}/download-link
**Описание**: Получение короткоживущего signed URL для скачивания архива

**Response**: `200 OK`
```json
{
    "download_url": "https://storage.yandexcloud.net/...&X-Amz-Expires=300",
    "expires_in_seconds": 300
}
```

#### GET /api/v1/studies/{id}/download
**Описание**: Совместимый endpoint: редирект на короткоживущий signed S3 URL

**Response**: `302 Found` (Redirect to signed S3 URL)

#### DELETE /api/v1/studies/{id}
**Описание**: Удаление исследования из S3

**Response**: `204 No Content`

### 7.2. Отчеты

#### POST /api/v1/reports
**Описание**: Прием отчета о дежурстве

**Request Body**:
```json
{
    "report_date": "2024-01-15",
    "doctor_name": "Иванов И.И.",
    "patients_count": 25,
    "emergencies": [
        {
            "time": "02:30",
            "patient_name": "Петров П.П.",
            "diagnosis": "Острая кишечная непроходимость"
        }
    ],
    "content": {...},
    "original_txt": "Текст отчета в TXT формате...",
    "generate_pdf": true
}
```

**Response**: `201 Created`

#### GET /api/v1/reports
**Описание**: Список отчетов

**Response**: `200 OK`
```json
{
    "reports": [
        {
            "id": "uuid",
            "report_date": "2024-01-15",
            "doctor_name": "Иванов И.И.",
            "patients_count": 25,
            "has_pdf": true
        }
    ]
}
```

#### GET /api/v1/reports/{id}/pdf
**Описание**: Скачать PDF версию отчета

**Response**: `302 Found` (Redirect to signed S3 URL)

### 7.3. Планы операций

#### POST /api/v1/plans
**Описание**: Загрузка плана операций

**Request Body**:
```json
{
    "week_number": 4,
    "year": 2024,
    "plan_data": {
        "surgeries": [
            {
                "date": "2024-01-22",
                "time": "09:00",
                "patient_name": "Сидоров С.С.",
                "surgery_type": "Коронарное шунтирование"
            }
        ]
    },
    "file_format": "json"
}
```

**Response**: `201 Created`

#### GET /api/v1/plans/current
**Описание**: Текущий план операций

**Response**: `200 OK`
```json
{
    "week_number": 4,
    "year": 2024,
    "plan_data": {...},
    "version": 1,
    "created_at": "2024-01-15T10:00:00Z"
}
```

### 7.4. Пользователи и настройки

#### GET /api/v1/user/settings
**Описание**: Получение настроек пользователя

**Response**: `200 OK`
```json
{
    "downloading_auto": true,
    "max_cache_size_mb": 2048,
    "notifications_enabled": true,
    "remote_pacs": {
        "url": "pacs.hospital.com",
        "port": 104,
        "aet": "MOBILE_AE",
        "calling_aet": "BACKEND_AE"
    }
}
```

#### PUT /api/v1/user/settings
**Описание**: Обновление настроек

**Request Body**: Same as GET response

**Response**: `200 OK`

#### POST /api/v1/user/push-token
**Описание**: Регистрация push-токена

**Request Body**:
```json
{
    "token": "fcm_token_or_apns_token",
    "platform": "android"
}
```

**Response**: `200 OK`

---

## 8. Форматы данных

### 8.1. Структура архива исследования (zstd)

```
study_{uid}.zst
├── manifest.json
│   ├── study_uid: string
│   ├── patient_name: string
│   ├── modality: string
│   ├── num_slices: int
│   ├── series_info: array
│   └── compression: "zstd"
├── preprocessed/
│   ├── series_1/
│   │   ├── pixels_16bit.bin  (binary, all slices)
│   │   ├── min_max.json
│   │   ├── histograms.json
│   │   └── tags.json
│   └── series_2/
│       └── ...
└── original_dicom/
    ├── slice_001.dcm
    ├── slice_002.dcm
    └── ...
```

### 8.2. Manifest.json формат

**Примечание**: для метаданных архива используется `JSON` (не `YAML`) для более быстрого и стандартного парсинга на iOS/Android.

```json
{
    "manifest_version": 1,
    "study_uid": "1.2.3.4.5",
    "patient_name": "Иванов И.И.",
    "patient_id": "123456",
    "study_date": "2024-01-15T08:30:00Z",
    "modality": "CT",
    "num_slices": 350,
    "slice_thickness": 1.25,
    "pixel_spacing": [0.5, 0.5],
    "series": [
        {
            "series_uid": "1.2.3.4.5.1",
            "modality": "CT",
            "num_slices": 200,
            "slice_indices": [0, 199],
            "default_window": {
                "center": 40,
                "width": 80
            }
        }
    ],
    "compression": {
        "algorithm": "zstd",
        "level": 3,
        "original_size_bytes": 524288000,
        "compressed_size_bytes": 262144000
    },
    "precomputed": {
        "windowing": true,
        "series_index": true,
        "mpr_geometry": true,
        "checksums": true
    }
}
```

### 8.2.1. JSON Schema (контракт backend ↔ iOS)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://api.hospital.com/schema/manifest.v1.json",
  "title": "Study Manifest",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "manifest_version",
    "study_uid",
    "patient_id",
    "study_date",
    "modality",
    "num_slices",
    "series",
    "compression",
    "precomputed"
  ],
  "properties": {
    "manifest_version": {
      "type": "integer",
      "minimum": 1
    },
    "study_uid": {
      "type": "string",
      "minLength": 1,
      "maxLength": 128
    },
    "patient_name": {
      "type": "string"
    },
    "patient_id": {
      "type": "string",
      "minLength": 1,
      "maxLength": 128
    },
    "study_date": {
      "type": "string",
      "format": "date-time"
    },
    "modality": {
      "type": "string",
      "enum": ["CT", "XA", "MR", "XR", "US", "OTHER"]
    },
    "num_slices": {
      "type": "integer",
      "minimum": 1
    },
    "slice_thickness": {
      "type": "number",
      "minimum": 0
    },
    "pixel_spacing": {
      "type": "array",
      "items": { "type": "number", "exclusiveMinimum": 0 },
      "minItems": 2,
      "maxItems": 2
    },
    "series": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": [
          "series_uid",
          "modality",
          "num_slices",
          "slice_indices",
          "default_window",
          "dicom_order"
        ],
        "properties": {
          "series_uid": { "type": "string", "minLength": 1, "maxLength": 128 },
          "modality": { "type": "string" },
          "num_slices": { "type": "integer", "minimum": 1 },
          "slice_indices": {
            "type": "array",
            "items": { "type": "integer", "minimum": 0 },
            "minItems": 2,
            "maxItems": 2
          },
          "default_window": {
            "type": "object",
            "additionalProperties": false,
            "required": ["center", "width"],
            "properties": {
              "center": { "type": "number" },
              "width": { "type": "number", "exclusiveMinimum": 0 }
            }
          },
          "dicom_order": {
            "type": "array",
            "minItems": 1,
            "items": {
              "type": "object",
              "additionalProperties": false,
              "required": ["index", "sop_instance_uid", "file", "sort_key"],
              "properties": {
                "index": { "type": "integer", "minimum": 0 },
                "sop_instance_uid": { "type": "string" },
                "file": { "type": "string" },
                "sort_key": { "type": "number" }
              }
            }
          }
        }
      }
    },
    "compression": {
      "type": "object",
      "additionalProperties": false,
      "required": ["algorithm", "level", "original_size_bytes", "compressed_size_bytes"],
      "properties": {
        "algorithm": { "type": "string", "enum": ["zstd"] },
        "level": { "type": "integer", "minimum": 1, "maximum": 22 },
        "original_size_bytes": { "type": "integer", "minimum": 1 },
        "compressed_size_bytes": { "type": "integer", "minimum": 1 }
      }
    },
    "precomputed": {
      "type": "object",
      "additionalProperties": false,
      "required": ["windowing", "series_index", "mpr_geometry", "checksums"],
      "properties": {
        "windowing": { "type": "boolean", "const": true },
        "series_index": { "type": "boolean", "const": true },
        "mpr_geometry": { "type": "boolean", "const": true },
        "checksums": { "type": "boolean", "const": true }
      }
    }
  }
}
```

### 8.3. Версионирование архива и backward compatibility

- Поле `manifest_version` обязательно в `manifest.json`
- Клиент обязан поддерживать текущую версию и минимум одну предыдущую
- Для неизвестной версии архив помечается как `unsupported_version`, загрузка не удаляется, показывается понятное сообщение пользователю
- Изменения формата делятся на:
  - **Minor**: добавление необязательных полей (обратная совместимость сохраняется)
  - **Major**: изменение обязательных структур (требует обновления мобильного клиента)
- Бекенд публикует `event_version` для событий и `manifest_version` для архива отдельно

### 8.4. Сообщение в брокере (studies.new)

```json
{
    "event_type": "new_study",
    "event_version": 1,
    "timestamp": "2024-01-15T08:35:00Z",
    "study_id": "550e8400-e29b-41d4-a716-446655440000",
    "user_id": "d8f2d7a2-ec4e-4e1f-b49e-1d40b8509f4b",
    "modality": "CT",
    "study_date": "2024-01-15T08:30:00Z",
    "num_slices": 350,
    "compressed_size_mb": 250,
    "downloading_auto": true,
    "download_url": "/api/v1/studies/550e8400-e29b-41d4-a716-446655440000/download-link",
    "archive_checksum_sha256": "2a9f3c4f..."
}
```

### 8.5. Минимальный обязательный состав предрасчётов в архиве

Для гарантии мгновенного открытия на мобилке, в каждом архиве должны быть:

- `windowing`: предрасчитанные default window/level и гистограммы по сериям
- `series_index`: полный индекс серий, сортировка срезов, диапазоны индексов, ориентирование
- `mpr_geometry`: spacing, origin, orientation, матрица перехода для MPR
- `checksums`: SHA-256 архива и ключевых файлов внутри (`manifest.json`, `pixels_16bit.bin`)
- `render_hints`: подсказки для клиента (например, XA autoplay, preferred series)

Если хотя бы один обязательный блок отсутствует, исследование получает статус `needs_reprocess` и не считается готовым к мгновенному открытию.

### 8.5.1. Обязательный порядок DICOM срезов (критично)

Порядок срезов формируется **на больничном скрипте до архивации** и фиксируется в `manifest.json` в поле `series[].dicom_order`.

Правила сортировки внутри каждой серии:

1. Основной ключ: геометрический порядок по позиции среза (`ImagePositionPatient`) вдоль нормали, рассчитанной из `ImageOrientationPatient`.
2. Fallback #1: `InstanceNumber` (если геометрии недостаточно).
3. Fallback #2: `AcquisitionTime` / `ContentTime`.
4. Fallback #3: лексикографически по `SOPInstanceUID`.

Контроль качества перед упаковкой:

- Проверка, что количество элементов `dicom_order` равно `num_slices`.
- Проверка уникальности `sop_instance_uid` в серии.
- Проверка монотонности итогового `sort_key`.
- При нарушении — серия помечается как `needs_reprocess`, архив не публикуется как готовый к просмотру.

### 8.6. Формат отчета (входящий)

```json
{
    "report_date": "2024-01-15",
    "doctor_name": "Иванов И.И.",
    "patients_count": 25,
    "emergencies": [
        {
            "time": "02:30",
            "patient_name": "Петров П.П.",
            "diagnosis": "Острая кишечная непроходимость",
            "action": "Экстренная операция",
            "outcome": "Успешно"
        },
        {
            "time": "15:45",
            "patient_name": "Сидоров С.С.",
            "diagnosis": "Инфаркт миокарда",
            "action": "Тромболизис",
            "outcome": "Стабильно"
        }
    ],
    "consultations": [
        {
            "time": "10:00",
            "specialty": "Неврология",
            "patient_name": "Кузнецова А.А.",
            "diagnosis": "Острое нарушение мозгового кровообращения"
        }
    ],
    "conclusions": "За время дежурства выполнено 3 экстренные операции. Все пациенты в стабильном состоянии.",
    "original_txt": "Полный текст отчета в TXT формате..."
}
```

---

## 9. Потоки данных

### 9.1. Автоматическая загрузка (Флаг ON)

```
┌──────────┐     ┌──────────┐        ┌──────────┐
│  PACS    │────▶│  Script  │───────▶│   S3     │
└──────────┘     └────┬─────┘        └────┬─────┘
                      │                   │
                      │ POST /studies     │ Archive by key
                      ▼                   ▼
                 ┌──────────┐       [signed URL, short TTL]
                 │ Backend  │
                 └────┬─────┘
                      │
                      ▼
                 [PostgreSQL]
                      │
                      ▼
                [Message Broker]
                      │ Kafka event
                      ▼
             [Notification Gateway]
                      │ Push/In-App event
                      ▼
      ┌──────────────────────────────────────────┐
      │         Mobile App (AutoDownload)        │
      └──────────────────────────────────────────┘
            │
            ▼
       [Download] ───▶ [Decompress] ───▶ [Pre-render]
                                             │
                                             ▼
                                    [Push Notification]
                                             │
                                             ▼
                                      [Instant Open]
```

**Шаги**:
1. Скрипт выгружает исследование из PACS
2. Парсит DICOM, извлекает метаданные
3. Сжимает zstd, загружает в S3
4. Отправляет метаданные на бэкенд
5. Бекенд сохраняет в БД, отправляет в брокер
6. `Notification Gateway` читает `studies.new` из Kafka
7. Gateway доставляет событие на мобильное устройство
8. Мобилка проверяет флаг `downloading_auto`
9. При `downloading_auto=true` запрашивает signed URL и скачивает архив
10. При `downloading_auto=false` только обновляет листинг доступных исследований
11. Распаковывает, сохраняет локально
12. Выполняет пре-рендеринг в фоне
13. Показывает push-уведомление
14. При открытии: мгновенный показ без ожидания анализа

### 9.2. Ручная загрузка (Флаг OFF)

```
Mobile App                    Backend                       S3
    │                            │                          │
    ├─── GET /studies ──────────▶│                          │
    │                            │                          │
    │◀── List of studies ────────┤                          │
    │                            │                          │
    │ (User clicks Download)     │                          │
    │                            │                          │
    ├─── GET /studies/{id} ─────▶│                          │
    │                            │                          │
    │◀─ Metadata + can_download ─┤                          │
    │                            │                          │
    ├── GET /studies/{id}/download-link ───────────────────▶│
    │                            │                          │
    │◀──────────── signed URL ───┤                          │
    │                            │                          │
    ├────────────────────────────┼───────── Download ──────▶│
    │                            │                          │
    │◀───────────────────────────┼────────── Archive ───────┤
    │                            │                          │
    ├─── Decompress ─────────────┤                          │
    │                            │                          │
    ├─── Pre-render ─────────────┤                          │
    │                            │                          │
    └─── Open (instant) ─────────┘                          │
```

### 9.3. Отправка на удаленный PACS

```
Mobile App                        Backend                S3              Remote PACS
    │                                │                   │                    │
    │ (User clicks Send to PACS)     │                   │                    │
    │                                │                   │                    │
    ├── GET /studies/{id}/download ─▶│                   │                    │
    │                                │                   │                    │
    │                                │                   │                    │
    │                                │◀─── Archive ──────┤                    │
    │                                │                   │                    │
    │                                │─── Decompress ────┤                    │
    │                                │                   │                    │
    │                                │─ raw DICOM files──│───────────────────▶│
    │                                │                   │                    │
    │◀── Response Notification───-───┼───────────────────┼────────────────────┤
    │                                │                   │                    │
    │                                │                   │                    │
```

### 9.4. Отчет о дежурстве / план операций

```
Script                    Backend                   MongoDB              Mobile
    │                         │                        │                   │
    ├── POST /reports ───────▶│                        │                   │
    │   (JSON report)         ├── insert report ──────▶│                   │
    │                         │                        │                   │
    ├── POST /plans ─────────▶│                        │                   │
    │   (JSON plan)           ├── insert plan ────────▶│                   │
    │                         │                        │                   │
    │                         ├── publish reports.new ─┼──────────────────▶│
    │                         ├── publish plans.new ───┼──────────────────▶│
    │                         │      (Kafka notify)    │                   │
    │                         │                        │                   │
    │                         │◀──── GET /reports ─────────────────────────┤
    │                         ├── find reports ───────▶│                   │
    │                         │◀── reports documents ──┤                   │
    │                         ├── JSON reports ───────────────────────────▶│
    │                         │                        │                   │
    │                         │◀─ GET /plans/current ──────────────────────┤
    │                         ├── find current plan ──▶│                   │
    │                         │◀── plan document ──────┤                   │
    │                         ├── JSON current plan ──────────────────────▶│
```

**Логика**:
1. Скрипт отправляет отчеты и планы операций в backend (`POST /reports`, `POST /plans`).
2. Backend сохраняет документы в MongoDB (`reports`, `surgery_plans`).
3. Backend публикует `reports.new` и `plans.new` в Kafka для уведомления мобильного клиента.
4. Мобильное приложение получает уведомление и запрашивает данные через backend API.
5. Backend читает MongoDB и возвращает отчеты/планы мобильному приложению.

---

## 10. Технологический стек

### 10.1. Больничный скрипт

| Компонент | Технология | Версия | Обоснование |
|-----------|------------|--------|-------------|
| Язык | Python | 3.9+ | Простота, DICOM библиотеки |
| DICOM парсинг | pydicom | 2.4+ | Стандарт для Python |
| Сжатие | zstandard | 0.22+ | Лучший баланс скорость/размер |
| S3 клиент | boto3 | 1.34+ | Официальный AWS SDK |
| HTTP клиент | requests | 2.31+ | Простота использования |
| Расписание | apscheduler | 3.10+ | Cron-like задачи |
| CLI | click | 8.1+ | Удобный парсер команд |
| Конфигурация | python-dotenv | 1.0+ | 12-factor app |
| PDF генерация | WeasyPrint | 60+ | Качественный HTML→PDF |

**Зависимости**:
```txt
pydicom==2.4.4
zstandard==0.22.0
boto3==1.34.0
requests==2.31.0
APScheduler==3.10.4
click==8.1.7
python-dotenv==1.0.0
weasyprint==60.1
```

### 10.2. Бекенд

| Компонент | Технология | Версия | Обоснование |
|-----------|------------|--------|-------------|
| Язык | Golang | 1.23+ | Высокая производительность, простая эксплуатация |
| HTTP Framework | Gin | 1.10+ | Легковесный роутинг и middleware |
| ORM/SQL | sqlx + squirrel | - | Явный SQL-контроль и простые query-builder паттерны |
| База данных (исследования) | PostgreSQL | 14+ | Надежность, JSONB поддержка |
| База данных (отчеты/планы) | MongoDB | 6.0+ | Гибкая документная модель для JSON-документов |
| Брокер | Apache Kafka | 3.7+ | Высокая пропускная способность и устойчивость |
| Notification Gateway | Go service + FCM/APNS adapters | - | Доставка Kafka событий на мобильные устройства |
| S3 клиент | AWS SDK for Go v2 | 1.30+ | Совместимость с S3 API |
| Аутентификация | JWT | - | Stateless авторизация |
| Миграции | goose | 3.x | Управляемые SQL миграции |


**Зависимости**:
```txt
github.com/gin-gonic/gin v1.10.0
github.com/jmoiron/sqlx v1.4.0
github.com/Masterminds/squirrel v1.5.4
github.com/golang-jwt/jwt/v5 v5.2.1
github.com/aws/aws-sdk-go-v2 v1.30.0
github.com/aws/aws-sdk-go-v2/service/s3 v1.61.0
github.com/segmentio/kafka-go v0.4.47
firebase.google.com/go/v4 v4.15.1
github.com/sideshow/apns2 v0.25.0
go.uber.org/zap v1.27.0
```

### 10.3. Мобильное приложение (Android)

| Компонент | Технология | Версия | Обоснование |
|-----------|------------|--------|-------------|
| Язык | Kotlin | 1.9+ | Современный, корутины |
| Минимум API | 26 (Android 8.0) | - | 95% устройств |
| UI | Jetpack Compose | 1.5+ | Современный UI |
| Архитектура | Clean + MVVM | - | Разделение ответственности |
| БД | Room | 2.6+ | SQLite обертка |
| Network | Retrofit | 2.9+ | Type-safe HTTP |
| Event Client | Notification SDK | - | Получение broker-событий |
| Сжатие | zstd (JNI) | 1.5+ | C++ обертка |
| Рендеринг | OpenGL ES | 3.0+ | Медицинские изображения |
| Фоновый сервис | WorkManager | 2.9+ | Отложенные задачи |
| DI | Dagger Hilt | 2.48+ | Внедрение зависимостей |
| Push | Firebase | 32.5+ | Уведомления |

**Зависимости (build.gradle)**:
```gradle
dependencies {
    implementation 'org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3'
    implementation 'androidx.room:room-runtime:2.6.0'
    implementation 'androidx.room:room-ktx:2.6.0'
    implementation 'com.squareup.retrofit2:retrofit:2.9.0'
    implementation 'com.squareup.retrofit2:converter-gson:2.9.0'
    implementation 'com.squareup.okhttp3:logging-interceptor:4.11.0'
    implementation 'org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5'
    implementation 'androidx.work:work-runtime-ktx:2.9.0'
    implementation 'com.google.dagger:hilt-android:2.48'
    implementation 'com.google.firebase:firebase-messaging:32.5.0'
    implementation 'androidx.compose.ui:ui:1.5.4'
}
```

### 10.4. Мобильное приложение (iOS)

| Компонент | Технология | Версия | Обоснование |
|-----------|------------|--------|-------------|
| Язык | Swift | 5.9+ | Нативный, производительный |
| Минимум iOS | 13.0 | - | Широкая поддержка |
| UI | SwiftUI | - | Современный фреймворк |
| Архитектура | Clean + MVVM | - | Разделение ответственности |
| БД | CoreData | - | Нативная |
| Network | URLSession | - | Встроенная |
| Event Client | Notification SDK | - | Получение broker-событий |
| Сжатие | zstd (FFI) | 1.5+ | C библиотека |
| Рендеринг | Metal | - | Высокая производительность |
| Фоновый сервис | BackgroundTasks | - | Отложенные задачи |
| Push | APNS | - | Нативные уведомления |

### 10.5. Инфраструктура

| Компонент | Технология | 
|-----------|------------|
| Облако | Yandex Cloud |
| Оркестрация | Docker + K8s | 
| CI/CD | GitHub CI |
| Мониторинг | Prometheus + Grafana | 
| Логи | ELK Stack | 
| Трассировка | OpenTelemetry + Jaeger | 

---

## 11. Нефункциональные требования

### 11.1. Производительность

| Параметр | Целевое значение | Допустимое отклонение |
|----------|------------------|----------------------|
| `time_to_available_study` (PACS → доступно в API) | < 5 минут | ±1 минута |
| `time_to_available_report` (script → доступно в API) | < 2 минуты | ±30 секунд |
| `time_to_available_surg_plan` (script → доступно в API) | < 2 минуты | ±30 секунд |
| Время скачивания 100 МБ (4G) | < 30 секунд | ±5 секунд |
| Время распаковки исследования | < 5 секунд | ±1 секунда |
| Время пре-рендеринга КТ (350 срезов) | < 10 секунд | ±2 секунды |
| Время открытия готового исследования | Мгновенно для пользователя | Без видимой задержки |
| Потребление RAM (мобилка) | < 200 МБ | ±50 МБ |
| Потребление диска (мобилка) | Настраиваемый лимит | 1-4 ГБ |
| API latency (p95) | < 200 мс | ±50 мс |
| Задержка доставки событий (`event_delivery_latency`, p95) | < 10 секунд | ±3 секунды |
| API requests per second | 1000 | - |

### 11.2. Надежность

| Параметр | Значение |
|----------|----------|
| Время безотказной работы (SLA) | 99.9% |
| Retry policy | 3 попытки, экспоненциальная задержка |
| Успешная доставка уведомлений | > 99% |
| Успешные загрузки исследований | > 97% |
| Recovery time после сбоя | < 5 минут |
| Целостность данных | Проверка хэшей SHA-256 |
| Офлайн режим | Полная работа с загруженными данными |
| Автоматическое восстановление | Переподключение к брокеру |

### 11.3. Масштабируемость

| Компонент | Горизонтальное масштабирование | Стратегия |
|-----------|-------------------------------|-----------|
| Бекенд API | Да | Stateless, load balancer |
| База данных | Чтение: да / Запись: мастер | Репликация |
| Message broker | Да | Кластеризация |
| S3 | Автоматически | Managed service |

### 11.4. Совместимость

| Компонент | Требование |
|-----------|-------------|
| Android | 8.0 (API 26) и выше |
| iOS | 13.0 и выше |
| DICOM | Стандарт 3.0 |
| Transfer Syntax | Implicit VR LE, Explicit VR LE, JPEG Lossless |
| Pixel Data | 8-bit, 12-bit, 16-bit |
| Сжатие | Zstandard (все платформы) |

---

## 12. Мониторинг и метрики

### 12.1. Бизнес-метрики

| Метрика | Цель | Как измеряется |
|---------|------|----------------|
| `time_to_available_study` | < 5 мин | PACS ingest timestamp → study available via API |
| `time_to_available_report` | < 2 мин | report ingest timestamp → report available via API |
| `time_to_available_surg_plan` | < 2 мин | plan ingest timestamp → current plan available via API |
| Время открытия исследования | Мгновенно для пользователя | Click → first image |
| Успешная доставка уведомлений | > 99% | delivered notifications / total notifications |
| Успешные загрузки исследований | > 97% | successful downloads / started downloads |
| Активных пользователей (DAU) | - | Unique users per day |

### 12.2. Технические метрики (Prometheus)

```yaml
# Бекенд метрики
study_upload_duration_seconds_bucket
study_upload_total{status="success|failed"}
s3_upload_duration_seconds_bucket
pdf_conversion_duration_seconds_bucket
message_broker_publish_total{topic}
notification_delivery_total{channel,status}
notification_delivery_latency_seconds_bucket{channel}
api_requests_total{endpoint, method, status}
api_request_duration_seconds_bucket
database_connection_pool_size
database_query_duration_seconds_bucket
time_to_available_study_seconds_bucket
time_to_available_report_seconds_bucket
time_to_available_surg_plan_seconds_bucket

# Скрипт метрики
pacs_query_duration_seconds_bucket
pacs_studies_found_total
dicom_parse_duration_seconds_bucket
compression_ratio{format="zstd"}

# Мобильные метрики (отправляются на бэкенд)
mobile_download_duration_seconds_bucket
mobile_decompression_duration_seconds_bucket
mobile_prerender_duration_seconds_bucket{modality}
mobile_study_open_duration_seconds_bucket
mobile_download_success_total{status}
mobile_cache_hit_ratio
mobile_memory_usage_mb
mobile_storage_usage_mb
```

### 12.3. End-to-end observability (OpenTelemetry)

- Все сервисы (`script`, `backend`, `notification-gateway`, mobile telemetry backend) используют OpenTelemetry SDK.
- В цепочке передается единый `trace_id` и `correlation_id` (`study_id`/`report_id`/`plan_id`).
- Kafka сообщения содержат trace context в headers.
- Ключевые спаны для исследования: `pacs_fetch` → `dicom_preprocess` → `s3_upload` → `backend_create_study` → `kafka_publish` → `notify_dispatch` → `mobile_download` → `mobile_open`.
- Ключевые спаны для отчетов/планов: `script_submit` → `backend_persist_mongo` → `kafka_publish` → `notify_dispatch` → `mobile_fetch_api`.

### 12.4. Логирование

**Формат лога (JSON)**:
```json
{
    "timestamp": "2024-01-15T08:35:00.123Z",
    "level": "INFO",
    "service": "backend",
    "trace_id": "abc-123-def",
    "user_id": "user-123",
    "study_id": "study-456",
    "event": "study_downloaded",
    "duration_ms": 1234,
    "size_mb": 250,
    "modality": "CT",
    "network_type": "wifi",
    "downloading_auto": true,
    "ip": "192.168.1.1"
}
```

**Уровни логирования**:
- `ERROR`: Критические ошибки (PACS недоступен, S3 ошибка)
- `WARN`: Не критические проблемы (retry, timeout)
- `INFO`: Нормальные операции (загрузка, скачивание)
- `DEBUG`: Детализация для отладки (только dev)

### 12.4. Алерты

| Условие | Уровень | Действие |
|---------|---------|----------|
| API error rate > 5% за 5 минут | CRITICAL | Уведомить DevOps |
| PACS недоступен > 10 минут | HIGH | Уведомить админа больницы |
| S3 upload latency > 10 секунд | MEDIUM | Проверить соединение |
| БД connection pool > 80% | WARNING | Подготовить масштабирование |
| Очередь сообщений > 5 | HIGH | Увеличить consumer'ов |

---

## 13. Обработка ошибок

### 13.1. Сценарии ошибок и их обработка

#### 13.1.1. Скрипт не может подключиться к PACS

```python
# Реализация
retry_count = 0
max_retries = 10
backoff = 60  # seconds

while retry_count < max_retries:
    try:
        studies = pacs_client.find_studies(query)
        break
    except ConnectionError as e:
        logger.error(f"PACS connection failed: {e}")
        time.sleep(backoff * (2 ** retry_count))
        retry_count += 1
else:
    send_alert("PACS недоступен более 10 попыток")
    save_to_local_queue(query)  # Сохраняем для повторной попытки позже
```

**Действия**:
- Логирование ошибки
- Retry с экспоненциальной задержкой
- Уведомление администратора больницы
- Сохранение запроса в локальную очередь

#### 13.1.2. Ошибка загрузки в S3

```python
# Реализация
try:
    s3_client.upload_file(local_path, bucket, key)
except S3UploadError as e:
    logger.error(f"S3 upload failed: {e}")
    save_local_archive(local_path)  # Сохраняем локально
    schedule_retry(key, local_path)  # Повтор через 5 минут
    send_alert(f"S3 upload failed for study {study_uid}")
```

**Действия**:
- Сохранение архива локально
- Планирование повторной попытки
- Уведомление DevOps

#### 13.1.3. Мобилка не получила сообщение из брокера

```kotlin
// Fallback: периодический polling
class FallbackSyncService : Worker() {
    override fun doWork(): Result {
        val lastSync = prefs.getLong("last_sync", 0)
        val now = System.currentTimeMillis()
        
        if (now - lastSync > 30 * 60 * 1000) { // 30 минут
            val newStudies = api.getStudiesSince(lastSync)
            if (newStudies.isNotEmpty()) {
                processNewStudies(newStudies)
            }
            prefs.edit().putLong("last_sync", now).apply()
        }
        return Result.success()
    }
}

// Запускаем WorkManager периодически
WorkManager.getInstance(context).enqueueUniquePeriodicWork(
    "fallback_sync",
    PeriodicWorkRequestBuilder<FallbackSyncService>(30, TimeUnit.MINUTES).build()
)
```

**Действия**:
- Fallback polling каждые 30 минут
- При открытии приложения принудительная синхронизация
- Логирование пропущенных сообщений

#### 13.1.4. Не хватает места на мобилке для авто-загрузки

```kotlin
class StorageManager {
    fun ensureSpaceForStudy(studySizeMB: Int): Boolean {
        val freeSpace = getFreeSpaceMB()
        val cacheSize = getCacheSizeMB()
        
        if (freeSpace < studySizeMB) {
            // Автоматическая очистка старых исследований
            val freed = clearOldStudies(studySizeMB - freeSpace + 100)
            
            if (!freed) {
                // Показываем диалог пользователю
                showStorageFullDialog()
                return false
            }
        }
        return true
    }
    
    private fun clearOldStudies(requiredSpaceMB: Int): Boolean {
        val oldStudies = db.getStudiesOrderedByAccess(oldestFirst = true)
        var freed = 0
        
        for (study in oldStudies) {
            deleteStudy(study.id)
            freed += study.sizeMB
            if (freed >= requiredSpaceMB) break
        }
        
        return freed >= requiredSpaceMB
    }
}
```

**Действия**:
- Автоматическая очистка старых исследований (LRU)
- Уведомление пользователя
- Предложение ручной очистки

#### 13.1.5. Ошибка при отправке на удаленный PACS

```kotlin
class SendToPACSUseCase {
    suspend fun execute(studyId: String, config: PACSConfig): Result<Unit> {
        return try {
            val study = repository.getLocalStudy(studyId)
            val dicomFiles = downloadOriginalDICOM(study.s3Url)
            val client = DICOMClient(config)
            
            client.sendStudies(dicomFiles)
            Result.success(Unit)
        } catch (e: DICOMStoreException) {
            logger.error("PACS send failed: ${e.message}")
            
            // Сохраняем для повторной отправки
            pendingSendRepository.save(studyId, config)
            
            // Показываем пользователю
            showError("Ошибка отправки. Будет повторно отправлено позже.")
            
            // Планируем retry
            scheduleRetry(studyId, config)
            
            Result.failure(e)
        }
    }
}
```

**Действия**:
- Логирование ошибки с деталями
- Сохранение в очередь повторных отправок
- Уведомление пользователя
- Retry через 5, 15, 30 минут

#### 13.1.6. Исследование повреждено (не распаковывается)

```kotlin
class Decompressor {
    fun decompress(compressedData: ByteArray, studyId: String): ByteArray {
        return try {
            zstd.decompress(compressedData)
        } catch (e: ZstdException) {
            logger.error("Decompression failed for study $studyId: ${e.message}")
            
            // Помечаем исследование как поврежденное
            markStudyCorrupted(studyId)
            
            // Удаляем локальную копию
            deleteLocalCopy(studyId)
            
            // Запрашиваем повторную загрузку
            scheduleRedownload(studyId)
            
            throw CorruptedStudyException("Study $studyId is corrupted")
        }
    }
}
```

**Действия**:
- Удаление поврежденной копии
- Отметка в БД (status = 'corrupted')
- Запрос повторной загрузки
- Уведомление администратора

### 13.2. Стратегии Retry

| Компонент | Макс попыток | Начальная задержка | Множитель |
|-----------|--------------|-------------------|-----------|
| Скрипт → PACS | 10 | 60 сек | 2x |
| Скрипт → S3 | 5 | 30 сек | 2x |
| Скрипт → API | 3 | 10 сек | 2x |
| Мобилка → S3 | 3 | 5 сек | 2x |
| Мобилка → PACS | 3 | 60 сек | 2x |
| Бекенд → Брокер | 5 | 1 сек | 2x |

### 13.3. Dead Letter Queue (DLQ)

```python
# Неудачные сообщения отправляются в DLQ
class MessageProcessor:
    async def process_message(self, message):
        try:
            await self.handle_study(message)
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            await self.broker.publish(
                topic="studies.dlq",
                message=message,
                headers={"error": str(e), "retries": message.retries}
            )
            
            if message.retries < 3:
                # Повторная отправка с задержкой
                await self.broker.publish_delayed(
                    topic="studies.new",
                    message=message,
                    delay_seconds=60 * (2 ** message.retries)
                )
```

---

## 14. Безопасность

### 14.1. Аутентификация и авторизация

```go
// JWT payload
type Claims struct {
    UserID   string `json:"sub"`
    Username string `json:"username"`
    Role     string `json:"role"`
    jwt.RegisteredClaims
}

func AuthMiddleware(verifier jwt.Verifier, public map[string]struct{}) gin.HandlerFunc {
    return func(c *gin.Context) {
        if _, ok := public[c.Request.URL.Path]; ok {
            c.Next()
            return
        }

        token := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
        if token == "" {
            c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing token"})
            return
        }

        claims, err := verifier.Verify(token)
        if err != nil {
            c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
            return
        }

        c.Set("user", claims)
        c.Next()
    }
}
```

### 14.2. Доступ к S3 (short-lived signed URL)

```go
// GET /api/v1/studies/:id/download-link
func (h *Handlers) GetStudyDownloadLink(c *gin.Context) {
    user := auth.MustUser(c)
    studyID := uuid.MustParse(c.Param("id"))

    study, err := h.studyService.GetStudy(c.Request.Context(), studyID)
    if err != nil {
        c.JSON(http.StatusNotFound, gin.H{"error": "study not found"})
        return
    }

    if !acl.CanDownload(user, study) {
        c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
        return
    }

    signedURL, ttlSec, err := h.storage.CreateSignedGetURL(c.Request.Context(), study.S3Key, 5*time.Minute)
    if err != nil {
        c.JSON(http.StatusBadGateway, gin.H{"error": "storage unavailable"})
        return
    }

    c.JSON(http.StatusOK, gin.H{
        "download_url": signedURL,
        "expires_in_seconds": ttlSec,
    })
}
```

**Политика signed URL**:
- URL короткоживущий (например, 5 минут)
- Доступ разрешен только авторизованным пользователям
- После истечения TTL клиент запрашивает новый signed URL
- Каждый доступ логируется в `audit_log`

### 14.3. Шифрование данных

```python
# Шифрование в S3 (SSE-S3)
s3_client.upload_file(
    Filename=local_file,
    Bucket=bucket,
    Key=key,
    ExtraArgs={
        'ServerSideEncryption': 'AES256',
        'Metadata': {
            'encrypted': 'true',
            'created_by': 'dicom_processor',
            'study_uid': study_uid
        }
    }
)

# TLS для всех соединений
# Настройка HTTPS
https = True
ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
ssl_context.load_cert_chain('cert.pem', 'key.pem')
```

### 14.4. Аудит доступа

```sql
-- Логирование доступа к исследованиям
CREATE TABLE audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    action VARCHAR(64), -- 'view_study', 'download_study', 'send_to_pacs'
    study_id UUID REFERENCES studies(id),
    timestamp TIMESTAMP DEFAULT NOW(),
    ip_address INET,
    user_agent TEXT,
    details JSONB
);

-- Пример записи
INSERT INTO audit_log (user_id, action, study_id, ip_address, details) VALUES (
    'user-123',
    'view_study',
    'study-456',
    '192.168.1.100',
    '{"modality": "CT", "num_slices": 350}'
);
```

### 14.5. Безопасность на мобилке

```kotlin
// Хранение JWT токена в Secure Storage
class TokenManager(context: Context) {
    private val securePrefs = context.getSharedPreferences(
        "secure_prefs",
        Context.MODE_PRIVATE
    )
    
    fun saveToken(token: String) {
        securePrefs.edit().putString("jwt_token", encrypt(token)).apply()
    }
    
    fun getToken(): String? {
        val encrypted = securePrefs.getString("jwt_token", null)
        return encrypted?.let { decrypt(it) }
    }
    
    private fun encrypt(data: String): String {
        // Используем Android Keystore
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        // ... encryption logic
    }
}

// Проверка целостности приложения
class SecurityCheck {
    fun isAppTampered(): Boolean {
        return isDebuggable() || 
               isEmulator() || 
               hasRootAccess() ||
               isHookDetected()
    }
}
```

### 14.6. Очистка данных

```python
# Автоматическая очистка S3 через 7 дней
async def cleanup_expired_studies():
    expired = await db.fetch_all(
        "SELECT * FROM studies WHERE expires_at < NOW() AND status = 'active'"
    )
    
    for study in expired:
        # Удаляем из S3
        await s3_client.delete_object(
            Bucket=study.s3_bucket,
            Key=study.s3_key
        )
        
        # Обновляем статус в БД
        await db.execute(
            "UPDATE studies SET status = 'deleted', deleted_at = NOW() WHERE id = ?",
            study.id
        )
        
        # Логируем удаление
        logger.info(f"Study {study.id} deleted from S3 (expired)")
```

---

## 15. Roadmap

### Фаза 1: MVP

**Цель**: Базовая рабочая система

- Настройка инфраструктуры (S3, PostgreSQL, MongoDB, Kafka)
- Больничный скрипт: выгрузка КТ мозга
- Сжатие `zstd`, загрузка архивов в S3
- Бекенд: прием метаданных и сохранение в БД
- Бекенд: API списка исследований и деталей
- Мобилка: список исследований и ручное скачивание
- Мобилка: базовый просмотр КТ
- Аутентификация JWT

**Результат**: Врач может вручную скачать и посмотреть КТ

---

### Фаза 2: Асинхронная доставка

**Цель**: Автоматическая доставка

- Настройка Kafka и топиков событий
- Бекенд: producer событий
- Реализация `Notification Gateway` (Kafka consumer + dispatch)
- Доставка push/in-app событий на iOS/Android
- Мобилка: `AutoDownloadService`
- Настройки: флаг `downloading_auto`

**Результат**: Исследования автоматически загружаются, push-уведомления

---

### Фаза 3: Полная функциональность

**Цель**: Все типы исследований и MPR

- Поддержка XA (ангиография)
- Поддержка XR (рентген)
- MPR для КТ
- Windowing в реальном времени
- Измерения (расстояния, углы)
- Отправка на удаленный PACS
- Пре-рендеринг в фоне

**Результат**: Полноценный DICOM-вьюер

---

### Фаза 4: Документы и отчеты

**Цель**: Отчеты и планы операций

- Скрипт: отправка отчетов
- Бекенд: конвертация TXT → PDF
- Мобилка: раздел отчетов
- Скрипт: загрузка планов операций
- Мобилка: раздел планов
- Уведомления о новых документах

**Результат**: Единая система для изображений и документов

---

### Фаза 5: Оптимизация и Production

**Цель**: Стабильность и производительность

- Cleanup scheduler
- Оптимизация памяти и диска
- Мониторинг (Prometheus + Grafana)
- Логи (ELK Stack)
- Тестирование в реальной больнице
- Исправление багов
- Документация и обучение

**Результат**: Production-ready система

---

## 16. Риски и митигация

### 16.1. Технические риски

| Проблема | Решение |
|----------|---------|
| **PACS больницы недоступен** | Retry с экспоненциальной задержкой, локальная очередь, уведомление администратора |
| **S3 недоступен** | Retry, отложенная повторная загрузка, хранение локальной копии до успешной отправки |
| **Kafka или Notification Gateway недоступны** | Репликация Kafka, DLQ, повторная доставка через gateway, fallback синхронизация списка исследований через API |
| **Мобилка теряет соединение** | Локальное кэширование, синхронизация при восстановлении сети, ручной refresh |
| **Недостаточно места на мобилке** | Авто-очистка LRU, предупреждение пользователя, настройка лимита кэша |
| **Медленная распаковка zstd** | Уровень сжатия 3, фоновая распаковка, индикация прогресса |
| **Удаленный PACS не принимает отправку** | Очередь повторной отправки, диагностика конфигурации AET/host/port, журнал ошибок |
| **Поврежденные DICOM/архив** | Проверка checksum, повторная загрузка, статус `needs_reprocess` |
| **Нарушен порядок срезов** | Валидация `dicom_order` на скрипте, блок публикации невалидного архива |

### 16.2. Организационные риски

| Проблема | Решение |
|----------|---------|
| **Конфиденциальность данных** | TLS, шифрование в S3, аудит доступа, минимум PHI в событиях брокера |
| **Бюджетные ограничения** | Очистка S3 через 7 дней, мониторинг объема и стоимости хранения |

### 16.3. Риски производительности

| Проблема | Решение |
|----------|---------|
| **Медленная загрузка КТ большого объема** | `zstd`, фоновая загрузка, докачка после обрыва, прогресс-бар |
| **Высокая нагрузка на бэкенд API** | Горизонтальное масштабирование, профилирование узких мест, кэширование read-path |
| **Большой объем данных в S3** | Авто-очистка через 7 дней, алерты на рост storage |
| **Медленный парсинг на скрипте** | Параллельная обработка серий, профилирование CPU/IO, оптимизация pydicom pipeline |

---

## 17. Ключевые решения

### 17.1. Принятые архитектурные решения

| Решение | Обоснование | Возможные улучшения |
|---------|-------------|---------------------|
| **Парсинг DICOM на скрипте** | Снимает тяжелую обработку с мобилки, ускоряет открытие | Добавить GPU/многопоточную предобработку на больничном ПК |
| **Явная фиксация `dicom_order`** | Гарантирует корректный порядок срезов на iOS/Android | Добавить автоматический QA-отчет по качеству сортировки |
| **Отказ от стриминга** | Проще архитектура и стабильный офлайн-путь | Добавить опциональный lightweight preview для плохой сети |
| **Полная загрузка на мобилку** | Мгновенный доступ к исследованию после подготовки | Добавить умное управление кэшем по приоритету пациента/типа исследований |
| **Kafka + Notification Gateway** | Надежная событийная доставка без прямого доступа мобилки к Kafka | Добавить отдельный сервис подтверждения доставки (delivery receipts) |
| **`zstd` сжатие** | Лучший баланс скорости и размера для больших исследований | Динамически выбирать уровень сжатия по типу модальности |
| **Единый архив в S3** | Упрощает хранение и повторную загрузку | Добавить optional split-архивы по сериям для очень больших кейсов |
| **Short-lived signed URL** | Повышает безопасность доступа к S3 | Добавить фоновый refresh URL при длинных скачиваниях |
| **`manifest_version` + JSON Schema контракт** | Предсказуемая совместимость backend/iOS/Android | Автоматическая валидация схемы в CI перед публикацией |
| **Пре-рендеринг на мобилке после распаковки** | Пользователь открывает исследование без ожидания анализа | Добавить warm-up кэша GPU/Metal/OpenGL при зарядке и Wi‑Fi |
| **Хранение в S3 7 дней** | Баланс доступности и стоимости | Политики хранения по классам исследований (горячее/холодное) |


---

## 18. Приложения

### 18.1. Глоссарий

| Термин | Определение |
|--------|-------------|
| **DICOM** | Digital Imaging and Communications in Medicine - стандарт медицинских изображений |
| **PACS** | Picture Archiving and Communication System - система хранения медицинских изображений |
| **CT** | Computed Tomography - компьютерная томография |
| **XA** | X-ray Angiography - рентгеновская ангиография |
| **MPR** | Multi-Planar Reconstruction - мультипланарная реконструкция |
| **Windowing** | Настройка окна/уровня для визуализации |
| **AET** | Application Entity Title - идентификатор DICOM приложения |
| **C-STORE** | DICOM команда для отправки изображений |
| **C-FIND** | DICOM команда для поиска исследований |
| **C-MOVE** | DICOM команда для перемещения исследований |
| **Q/R** | Query/Retrieve - запрос и получение исследований |
| **Signed URL** | Короткоживущий подписанный URL для безопасного скачивания архива из S3 |
| **Message Broker** | Промежуточное звено для асинхронной доставки сообщений |
| **Kafka** | Distributed event streaming platform для асинхронной доставки событий |
