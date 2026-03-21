# Documentación Técnica - IB Core

## 1. Visión General
**IB Core** es el backend central del sistema de trading. Actúa como gateway inteligente entre la API de Interactive Brokers (IBAPI) y el resto del ecosistema (GUI, Estrategias, Base de Datos).

### Responsabilidades Code
- **Conectividad**: Mantiene la sesión con TWS/IB Gateway y gestiona desconexiones.
- **Normalización**: Convierte los datos crudos de IB en estructuras propias (`models.py`).
- **Persistencia**: Guarda histórico de ejecuciones, precios y snapshots de cuenta en InfluxDB.
- **Tiempo Real**: Sirve datos de mercado y estado de órdenes vía WebSockets.
- **Integridad**: Asegura que no se pierdan ejecuciones ni se dupliquen datos.

---

## 2. Arquitectura de Módulos

### 2.1. `ib_connector.py` (Cerebro)
Es el controlador principal (`IBConnector`).
- Inicia y monitorea la conexión (`_monitor_connection`).
- Orquesta la suscripción a datos de mercado (`subscribe_contract`).
- Maneja el ciclo de vida de las órdenes (colocación, modificación, cancelación).
- **Ejecución de Reconciliación**: Al inicio, lanza un proceso para asegurar la integridad de los datos (ver sección 3).

### 2.2. `portfolio_manager.py` (Gestor de Posiciones)
Transforma las posiciones crudas de IB en estrategias con sentido financiero.
- **Raw Positions**: Recibe actualizaciones átomicas de IB (`position`).
- **Reconcile**: Agrupa patas individuales ("legs") en estrategias complejas (Spreads, Butterflies) basándose en la metadata de suscripción.
- **Cierre de Posiciones**: Detecta cuando una posición llega a 0 y actualiza el estado interno.

### 2.3. `db_client.py` (Persistencia)
Cliente de **InfluxDB v2**.
- **Buffering**: Acumula ticks de mercado en memoria y los escribe en batch cada 5 segundos para reducir I/O.
- **Lazy Loading**: Carga el estado previo de órdenes para deduplicación.
- **Measurements**:
  - `precios`: Ticks de mercado (BID, ASK, LAST).
  - `orders`: Cambios de estado de órdenes.
  - `account`: Snapshots del balance (NetLiquidation, etc.).
  - `executions`: Trades confirmados.

### 2.4. `connection_manager.py` (WebSockets)
Gestor de conexiones WebSocket.
- Mantiene registro de clientes conectados y sus suscripciones a tópicos.
- `broadcast(topic, data)`: Envía mensajes JSON a todos los suscriptores de un tópico.

---

## 3. Integridad de Datos (Executions)

Para garantizar que **todas** las ejecuciones (trades) se guardan en InfluxDB sin duplicados y sin huecos, el sistema sigue un protocolo estricto al arrancar (`reconcile_executions` en `ib_connector.py`):

1.  **Carga de Contexto (InfluxDB -> Memoria)**:
    - Recupera las ejecuciones de las últimas 48h desde InfluxDB (`get_recent_executions_context`).
    - Almacena `ExecId`, `Timestamp` y `Tags` en caché.
    - *Objetivo*: Tener contexto para identificar si una ejecución que envía IB es nueva o ya conocida, especialmente útil para correcciones de comisiones tardías.

2.  **Identificación del Último Sync**:
    - Consulta a InfluxDB la fecha/hora de la última ejecución guardada (`get_last_execution_time`).

3.  **Solicitud a IB (`reqExecutions`)**:
    - Solicita explícitamente a IB todas las ejecuciones del día actual (`ExecutionFilter` vacío).
    - Al recibir una ejecución (`execDetails`):
        - Verifica si el `ExecId` ya existe en la caché cargada en el paso 1.
        - **Si existe**: Se ignora (evita duplicados).
        - **Si no existe**: Se procesa, se guarda en InfluxDB y se añade a la caché.

Este mecanismo permite reiniciar el Core tantas veces como sea necesario sin corromper el historial de trades.

### 3.1. Watchdog de Órdenes (Pending Confirmations)
Interactive Brokers ocasionalmente retrasa u omite el envío de los callbacks `openOrder` y `orderStatus` al crear órdenes complejas (como Brackets o agrupaciones OCA) hasta que se solicita un refresco explícito. 
Para mitigar este punto ciego sin saturar el servidor con peticiones innecesarias, IB Core implementa un **Order Watchdog** (`_monitor_pending_orders_loop`):
- **Registro**: Toda orden enviada registra su `orderId` y un *timestamp*.
- **Confirmación Rápida**: Si IB responde naturalmente a través de callbacks en menos de 2 segundos, la orden se elimina del registro ("ACK").
- **Salvaguarda**: Si una orden pendiente supera los 2 segundos, el guardián de fondo lanza de forma asíncrona un `reqAllOpenOrders()` para forzar a IB a devolver las órdenes "atascadas". Existe un *cooldown* de 5 segundos entre peticiones forzadas para prevención de *Rate Limiting*.
- **Timeout**: Si la orden supera los 60 segundos sin respuesta, se considera huérfana (rechazo silencioso o fallo de red) y se elimina de caché para prevenir fugas de memoria.

---

## 4. WebSockets y Tópicos

El sistema expone un endpoint WebSocket en `/restAPI/ws`. Los clientes pueden suscribirse a los siguientes tópicos:

### 4.1. Tópicos de Sistema
- **`orders`**:
  - **Payload**: Diccionario completo de órdenes activas (`{oid: {status: ..., filled: ...}}`) o actualizaciones delta.
  - **Uso**: Tablas de órdenes en tiempo real.
- **`account`**:
  - **Payload**: Delta de cambios en métricas de cuenta (`{'U12345': {'NetLiquidation': 50000}}`).
  - **Uso**: Header de la GUI, P&L total.
- **`portfolio`**:
  - **Payload**: Lista completa de posiciones reconciliadas y Bags.
  - **Uso**: Pestaña "Portfolio".

### 4.2. Tópicos de Mercado (`market:{gConId}`)
- **Formato**: `market:ES_202309_FUT`
- **Payload**:
  ```json
  {
    "type": "update",
    "topic": "market:...",
    "data": {
      "gConId": "...",
      "tickType": "LAST",
      "price": 4500.50,
      "timestamp": 1690000000000
    }
  }
  ```
- **Throttling**: Las actualizaciones de mercado se agrupan y emiten cada X milisegundos para no saturar el cliente visual.

---

## 5. API REST (Endpoints Clave)

Aunque el WebSocket maneja el flujo de datos en tiempo real, la API REST se usa para acciones transaccionales:

- **Trading**:
  - `POST /restAPI/Orders/PlaceOrder`: Orden simple.
  - `POST /restAPI/Orders/PlaceOCA`: Grupo One-Cancels-All.
  - `POST /restAPI/Orders/PlaceBracket`: Orden con Stop Loss y Take Profit.
  - `POST /restAPI/Orders/{id}/Update`: Modificar precio/cantidad.
  - `DELETE /restAPI/Orders/{id}`: Cancelar.

- **Account & System**:
  - `GET /restAPI/Account/{id}`: Detalles de cuenta.
  - `GET /restAPI/Config`: Lectura de parámetros del archivo `.env`.
  - `POST /restAPI/Config`: Modificación en caliente de parámetros `.env`.

---

## 6. Configuración

Variables de entorno críticas definidas en el archivo `.env`:
- `APP_MODE`: `LAB` (Paper) o `PROD` (Live). Define buckets y comportamiento base.
- `IB_HOST`: IP del servidor/Gateway de Interactive Brokers (ej. `192.168.2.130`).
- `IB_CLIENT_ID`: ID de cliente para aislar eventos websocket en IB (ej. `1`).
- `INFLUXDB_*`: Credenciales y base de datos ts (`_URL`, `_TOKEN`, `_ORG`, `_BUCKET_PRICES`, `_BUCKET_PROD`, `_BUCKET_LAB`).
- `LOG_LEVEL`: Nivel de detalle de bitácora (`INFO`, `DEBUG`, etc.).
- `WATCHLIST_FILE`: Archivo JSON local que persiste los símbolos observados (ej. `watchlist.json`).
- `API_PORT`: Puerto donde expone su interfaz el servidor interno FastAPI (ej. `8000`).

---

*Última Actualización: 2026-02-21*
