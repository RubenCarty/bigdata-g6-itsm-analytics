# Datos del Proyecto — ITSM Analytics Big Data

## Cómo generar el dataset

```bash
# Genera 100,000 tickets de TI en español
python src/generador_tickets.py
# Output: data/raw/tickets_historicos.csv (~50MB)
```

## Estructura de campos

| Campo | Tipo | Descripción |
|-------|------|-------------|
| ticket_id | string | ID único (TKT-YYYY-NNNNNN) |
| fecha_creacion | datetime | Cuándo se abrió el ticket |
| fecha_cierre | datetime | Cuándo se cerró (null si abierto) |
| tipo | string | incidencia / requerimiento / problema / cambio |
| prioridad | string | critica / alta / media / baja |
| area_solicitante | string | Área de negocio que reporta |
| area_ti_asignada | string | Equipo TI que atiende |
| categoria | string | Categoría ITIL |
| titulo | string | Resumen breve del ticket |
| descripcion | string | Texto libre en español |
| resolucion | string | Texto libre de la solución |
| tiempo_resolucion_min | int | Minutos desde apertura a cierre |
| sla_cumplido | bool | True si resuelto dentro del objetivo |
| es_recurrente | bool | True si misma causa en últimos 30 días |
| satisfaccion_usuario | int | Calificación 1-5 |

## Dataset de muestra
`data/sample/tickets_muestra_100.csv` — 100 tickets de ejemplo (en GitHub)

## Fuentes externas (complemento)
- UCI IT Incident Dataset: https://archive.ics.uci.edu/dataset/498
- Kaggle IT Incidents: https://www.kaggle.com/datasets/faisalahmed/it-incident-management
