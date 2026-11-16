# HomeCloudBackend — COMMANDS

**Protocol:** WebSocket / JSON v1  
**Endpoint:** `ws://127.0.0.1:8080/ws`  
**Авторизация:** отсутствует  
**Идентификация:** `hello` при каждом новом подключении

Все сообщения отправляются по одному активному WebSocket-соединению. Сначала выполняется `hello`, затем можно отправлять команды.

---

## 01. Подключение

### HELLO

Представление устройства серверу. Первое сообщение после соединения, в течение 10 секунд.

```json
{
  "v": 1,
  "id": "hello-001",
  "type": "hello",
  "device_id": "my-desktop",
  "client_type": "desktop"
}
```

**Ожидаемый ответ:** `identified`

---

## 02. SYSTEM — Система

### SYSTEM.STATUS

Подробная диагностика компьютера, на котором работает backend.

```json
{
  "v": 1,
  "id": "status-001",
  "type": "command",
  "action": "system.status"
}
```

**Ответ:** `result`

### SYSTEM.HEALTH — Подписка

При включённом `message_bus.require_subscription` сначала необходимо отправить:

```json
{
  "v": 1,
  "id": "sub-001",
  "type": "subscribe",
  "topic": "system.health"
}
```

**Ответ:** `subscribed`

### SYSTEM.HEALTH — Запрос

Получение состояния сервера, времени и количества соединений.

```json
{
  "v": 1,
  "id": "health-001",
  "type": "command",
  "action": "system.health"
}
```

**Ответ:** `result`

---

## 03. CLIENTS — Подключённые устройства

### CLIENTS.LIST

Получение списка клиентов, подключённых к HomeCloudBackend.

```json
{
  "v": 1,
  "id": "clients-001",
  "type": "command",
  "action": "clients.list"
}
```

**Ответ:** `result`

---

## 04. WEBSOCKET — Служебные сообщения

### PING

Проверка работы соединения на уровне JSON-протокола.

```json
{
  "v": 1,
  "id": "ping-001",
  "type": "ping"
}
```

**Ответ:** `pong`

### DISCONNECT

Корректное завершение WebSocket-соединения.

```json
{
  "v": 1,
  "id": "disconnect-001",
  "type": "disconnect"
}
```

**Ответ:** `disconnected`, после чего соединение закрывается.

---

## Правила протокола

| Поле | Значение |
|---|---|
| `v` | Версия JSON-протокола |
| `id` | Идентификатор сообщения для сопоставления запросов и ответов |
| `type` | Тип сообщения |
| `action` | Имя выполняемой команды |
| `topic` | Тема подписки |
| `device_id` | Идентификатор подключаемого устройства |
| `client_type` | Тип клиентского приложения |

Для новых запросов рекомендуется генерировать новые значения `id`.

**Всего реализовано:** 3 команды (`system.status`, `system.health`, `clients.list`) и 4 служебных типа (`hello`, `subscribe`, `ping`, `disconnect`).

**Статус документа:** HomeCloudBackend Command API v1.