# Predictive IT Incident Management and Service Gap Detection Using Big Data Analytics, NLP, and Time Series Forecasting: A Business Intelligence Framework for Executive Decision-Making

> **Título en español:** Gestión Predictiva de Incidencias TI y Detección de Brechas de Servicio mediante Big Data, Procesamiento de Lenguaje Natural y Pronóstico de Series Temporales: Un Marco de Inteligencia de Negocios para la Alta Dirección  
> **Curso:** Big Data DD283 | Universidad Autónoma del Perú | 2026-1  
> **Grupo:** 6 | **Sector:** Tecnologías de la Información / ITSM / Gobierno de TI

---

## Equipo

| Nombre | GitHub | Rol |
|--------|--------|-----|
| [Apellido Nombre 1] | [@usuario1](https://github.com/usuario1) | Líder + Arquitectura de Datos |
| [Apellido Nombre 2] | [@usuario2](https://github.com/usuario2) | Pipeline PySpark + ETL |
| [Apellido Nombre 3] | [@usuario3](https://github.com/usuario3) | NLP + ML Predictivo |
| [Apellido Nombre 4] | [@usuario4](https://github.com/usuario4) | Dashboard Ejecutivo + Gobierno TI |

---

## 1. Caso de Negocio

### Contexto
Las organizaciones medianas y grandes en Perú gestionan entre **500 y 50,000 tickets de TI mensuales** (incidencias, requerimientos, cambios, problemas) en herramientas como ServiceNow, Jira Service Management, Zendesk, GLPI o sistemas propios. La **alta dirección (CIO, CTO, Gerente TI)** necesita responder preguntas críticas que hoy no pueden responder con agilidad:

- ¿En qué áreas de la empresa se concentran más los problemas de TI?
- ¿Qué incidencias se repiten mes a mes sin ser resueltas de raíz?
- ¿Hacia dónde va el volumen de tickets? ¿Está aumentando o disminuyendo?
- ¿Qué porcentaje del equipo de TI trabaja en "apagar incendios" vs proyectos estratégicos?
- ¿Cuáles son las brechas más críticas entre la capacidad del área TI y la demanda del negocio?

**Situación actual (punto de dolor):**
- Reportes manuales en Excel que toman 2-3 días en prepararse
- Sin análisis de texto de los tickets: solo categorías predefinidas que no capturan el problema real
- Incidencias recurrentes que nunca se convierten en "problemas" resueltos estructuralmente
- La alta dirección no tiene visibilidad en tiempo real del estado de TI
- Sin predicción: se descubre el pico de demanda cuando ya colapsó el equipo

**Impacto organizacional:**
- Cada hora de sistema caído = pérdida de S/ 8,000-50,000 (según sector)
- 60-70% del tiempo del equipo TI se consume en incidencias repetitivas (ITIL Research 2023)
- Costo de no resolver la causa raíz: el mismo problema genera 3-8 tickets antes de escalar

### El Problema
> Construir una plataforma Big Data que **ingeste, procese y analice** el histórico completo de tickets de TI (incidencias + requerimientos) para:
> 1. Detectar **brechas y patrones ocultos** en la gestión de TI mediante clustering y NLP
> 2. **Predecir** el volumen de tickets por área, tipo y mes con 4 semanas de anticipación
> 3. Generar un **dashboard ejecutivo** que permita a la Alta Dirección tomar decisiones basadas en datos, no en intuición

**¿Por qué Big Data?**
| V | Justificación |
|---|--------------|
| **Volumen** | 3 años de histórico × 2,000 tickets/mes = 72,000+ registros con texto libre |
| **Velocidad** | Tickets nuevos cada minuto en horas pico (9-11am, post-reuniones) |
| **Variedad** | Datos estructurados (estado, área, prioridad), semi-estructurados (JSON API), no estructurados (descripción texto libre en español) |
| **Veracidad** | Tickets mal categorizados, duplicados, descripciones incompletas, SLAs incumplidos sin registro |
| **Valor** | Reducir 30% incidencias repetitivas = liberar capacidad TI para proyectos estratégicos |

---

## 2. Objetivo del Proyecto

**Objetivo general:**
Desarrollar un sistema Big Data de análisis predictivo de tickets de TI que integre procesamiento de lenguaje natural (NLP en español), series temporales y clustering para detectar brechas de servicio y predecir la demanda futura, generando inteligencia accionable para la alta dirección.

**Objetivos específicos:**
1. Generar un dataset sintético realista de **100,000 tickets TI** con texto en español y metadatos
2. Construir pipeline ETL con PySpark: limpieza, normalización y enriquecimiento de tickets
3. Aplicar **NLP en español** (spaCy/NLTK) para clasificación automática y extracción de temas
4. Implementar **clustering K-Means** para identificar grupos de incidencias similares (brechas ocultas)
5. Entrenar modelo de **series temporales (Prophet/ARIMA)** para predecir volumen mensual por área
6. Almacenar en MongoDB Atlas con esquema optimizado para consultas ejecutivas
7. Construir **dashboard ejecutivo** con KPIs tipo ITIL: disponibilidad, MTTR, MTBF, SLA compliance
8. Evaluar el impacto potencial: ¿cuántos tickets se podrían evitar con gestión proactiva?

---

## 3. Arquitectura del Sistema

```
┌──────────────────────────────────────────────────────────────────────┐
│           ARQUITECTURA ITSM BIG DATA ANALYTICS                       │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  FUENTES DE DATOS ITSM                                               │
│  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐            │
│  │ Histórico      │ │ Tickets activos │ │ CMDB           │            │
│  │ 3 años         │ │ en tiempo real  │ │ (activos TI:   │            │
│  │ (CSV/JSON)     │ │ (API mock)      │ │ servidores,    │            │
│  │ 100K tickets   │ │                 │ │ sistemas)      │            │
│  └───────┬────────┘ └───────┬─────────┘ └───────┬────────┘            │
│          └─────────────────┴───────────────────┘                    │
│                              │                                       │
│  ┌───────────────────────────▼────────────────────────────────────┐  │
│  │              PYSPARK ETL + MEDALLION                           │  │
│  │  Bronze: tickets raw (sin transformar)                         │  │
│  │  Silver: limpieza + estandarización + enriquecimiento          │  │
│  │          (área normalizada, SLA calculado, duplicados removidos)│  │
│  │  Gold:   KPIs ejecutivos + predicciones + clusters             │  │
│  └───────────────────────────┬────────────────────────────────────┘  │
│                              │                                       │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │              CAPA DE ANALÍTICA                               │    │
│  │                                                              │    │
│  │  ┌─────────────────┐  ┌────────────────┐  ┌──────────────┐  │    │
│  │  │ NLP EN ESPAÑOL  │  │ CLUSTERING     │  │ FORECASTING  │  │    │
│  │  │ spaCy es_core   │  │ K-Means        │  │ Prophet      │  │    │
│  │  │ TF-IDF          │  │ Identificar    │  │ ARIMA        │  │    │
│  │  │ LDA (topics)    │  │ brechas ocultas│  │ 4 semanas    │  │    │
│  │  │ Clasificación   │  │ por similitud  │  │ hacia adelante│  │   │
│  │  │ automática      │  │ de texto       │  │              │  │    │
│  │  └─────────────────┘  └────────────────┘  └──────────────┘  │    │
│  └──────────────────────────────────────────────────────────────┘    │
│                              │                                       │
│  ┌───────────────────────────▼────────────────────────────────────┐  │
│  │    MongoDB Atlas (almacenamiento) + CockroachDB (KPIs ACID)    │  │
│  └───────────────────────────┬────────────────────────────────────┘  │
│                              │                                       │
│  ┌───────────────────────────▼────────────────────────────────────┐  │
│  │                  DASHBOARD EJECUTIVO                           │  │
│  │  Streamlit / Power BI / Plotly Dash                           │  │
│  │                                                                │  │
│  │  Vistas para la Alta Dirección:                               │  │
│  │  • Mapa de calor: tickets por área × mes                      │  │
│  │  • Ranking de brechas críticas (top 10 problemas recurrentes) │  │
│  │  • Predicción próximo mes: volumen esperado por área          │  │
│  │  • SLA Compliance: % cumplimiento por categoría               │  │
│  │  • MTTR (Mean Time To Resolve) por tipo de incidencia         │  │
│  │  • Alertas: áreas con tendencia creciente de tickets          │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 4. Modelo de Datos — Ticket de TI

```python
# Estructura de cada ticket (100,000 registros sintéticos)
{
  "ticket_id": "TKT-2024-001234",
  "fecha_creacion": "2024-03-15T09:23:00",
  "fecha_cierre": "2024-03-15T14:45:00",
  "tipo": "incidencia",              # incidencia | requerimiento | problema | cambio
  "prioridad": "alta",               # critica | alta | media | baja
  "area_solicitante": "Contabilidad",# área del negocio que reporta
  "area_ti_asignada": "Infraestructura",
  "categoria": "Red/Conectividad",
  "subcategoria": "VPN",
  "titulo": "No puedo conectarme a VPN desde casa",
  "descripcion": "Desde las 8am no puedo...",  # texto libre en español
  "resolucion": "Se reinició el servidor VPN...", # texto libre
  "tecnico_asignado": "TEC-045",
  "tiempo_resolucion_min": 322,
  "sla_cumplido": True,              # True/False
  "sla_objetivo_min": 480,
  "es_recurrente": False,            # mismo problema en últimos 30 días?
  "satisfaccion_usuario": 4,         # 1-5
  "escalado": False,
  "costo_estimado_parada_soles": 0,  # 0 si no generó parada
  "tag_nlp": [],                     # calculado por el pipeline
  "cluster_id": None                 # calculado por K-Means
}
```

---

## 5. Stack Tecnológico (100% Gratuito)

| Capa | Tecnología | Plataforma | Para qué |
|------|-----------|------------|---------|
| Generación datos | Faker + Python | Local/Colab | 100K tickets realistas en español |
| ETL Medallion | PySpark 3.5 | Databricks Community | Pipeline Bronze→Silver→Gold |
| NLP español | spaCy (`es_core_news_sm`) | pip install | Tokenización, extracción entidades |
| Topic Modeling | LDA (sklearn/gensim) | Google Colab | Descubrir temas latentes en tickets |
| Clustering | K-Means PySpark MLlib | Databricks Community | Agrupar tickets similares |
| Forecasting | Prophet (Meta) + ARIMA | Google Colab | Predecir volumen por mes/área |
| Almacenamiento NoSQL | MongoDB Atlas M0 | Atlas Free | Tickets procesados, KPIs |
| NewSQL | CockroachDB Serverless | Free tier | SLA compliance, métricas ACID |
| Calidad datos | Great Expectations | pip install | Validar tickets malformados |
| Dashboard | Streamlit | Streamlit Cloud Free | Portal ejecutivo CIO/CTO |
| Visualización | Plotly + Seaborn | pip install | Mapas de calor, series temporales |

---

## 6. Metodología

### 6.1 Análisis Exploratorio — Preguntas que debe responder el EDA

```
¿Cuáles son las 10 categorías con mayor volumen de tickets? (histórico 3 años)
¿Cuál es la tendencia mensual por área de negocio? ¿Hay estacionalidad?
¿Qué porcentaje de tickets son incidencias vs requerimientos?
¿Cuál es el MTTR (tiempo promedio de resolución) por categoría?
¿Qué % de tickets cumplen el SLA por prioridad?
¿Cuántos tickets "recurrentes" existen (mismo problema, misma área, mismo mes)?
¿Cuáles son las horas y días de mayor demanda?
¿Qué áreas de negocio generan más tickets? ¿Ha cambiado en 3 años?
```

### 6.2 NLP en Español — Pipeline de Procesamiento de Texto

```python
# Pipeline NLP para cada ticket
import spacy
nlp = spacy.load("es_core_news_sm")

# Paso 1: Preprocesamiento
# - Eliminar stopwords en español ("el", "la", "que", "no")
# - Lematización ("conectando" → "conectar")
# - Normalización de términos TI ("VPN", "servidor", "SAP", "red")

# Paso 2: TF-IDF
# - Extraer términos más representativos por categoría
# - Detectar nuevas categorías emergentes no en el catálogo original

# Paso 3: LDA Topic Modeling
# - Descubrir N temas latentes en los textos de tickets
# - Ejemplo temas esperados:
#   Tema 1: "VPN, acceso, remoto, conexión, home office"
#   Tema 2: "SAP, transacción, módulo, contabilidad, error"
#   Tema 3: "impresora, papel, atasco, driver, soporte"
#   Tema 4: "correo, Outlook, Teams, calendario, adjunto"

# Paso 4: Clasificación automática
# - Modelo TF-IDF + Random Forest para clasificar tickets mal etiquetados
# - Detectar tickets en categoría "General/Otros" que deberían estar en otra
```

### 6.3 Clustering K-Means — Detección de Brechas Ocultas

```python
# Features para K-Means (vectores de tickets)
# - TF-IDF vector del texto (descripción + resolución)
# - área_solicitante (encoded)
# - hora_del_día, día_semana, mes
# - prioridad (encoded)
# - tiempo_resolucion_min

# Resultado esperado (ejemplo con K=8 clusters):
# Cluster 0: "Incidencias de red en horario de cierre mensual contabilidad" ← BRECHA
# Cluster 1: "Requerimientos SAP módulo RRHH — nuevos usuarios" (crecimiento)
# Cluster 2: "Fallos de impresoras en sedes regionales" (problema infraestructura)
# Cluster 3: "Soporte correo electrónico — usuarios nuevos" (onboarding TI)
```

### 6.4 Forecasting — Predicción de Volumen con Prophet

```python
from prophet import Prophet

# Modelos de predicción:
# 1. Modelo global: total tickets próximo mes
# 2. Por área: predicción individual por área de negocio
# 3. Por categoría: ¿cuántos tickets de "Red" se esperan en julio?
# 4. Detección de anomalías: ¿este mes tuvo más tickets de lo esperado?

# Variables externas (regressors):
# - Inicio de mes (cierre contable = pico de incidencias)
# - Incorporación de personal (aumenta requerimientos)
# - Actualizaciones de sistemas (genera incidencias post-cambio)
```

### 6.5 KPIs Ejecutivos (Marco ITIL)

| KPI | Fórmula | Meta |
|-----|---------|------|
| **MTTR** | Σ(tiempo_resolucion) / N tickets | < 4h (alta prioridad) |
| **MTBF** | Tiempo entre incidencias del mismo tipo | > 30 días |
| **SLA Compliance** | Tickets resueltos en tiempo / Total × 100 | > 95% |
| **First Contact Resolution** | Tickets resueltos sin escalar / Total | > 70% |
| **Ticket Backlog** | Tickets abiertos > SLA objetivo | < 5% |
| **Brecha Index** | Tickets recurrentes / Total × 100 | < 10% |
| **Carga por Técnico** | Tickets asignados / N técnicos activos | < 15/semana |

---

## 7. Plan de Entregables por Semana

| Semana | Entregable | Notebook | Criterio de éxito |
|--------|-----------|----------|-------------------|
| **S1** | Dataset 100K tickets + EDA ejecutivo | `01_EDA_tickets_ti.ipynb` | Análisis completo: por área, mes, tipo, prioridad; 8 preguntas respondidas con gráficos |
| **S2** | Pipeline PySpark Medallion | `02_pipeline_medallion.ipynb` | Bronze→Silver→Gold; tickets limpios, SLA calculado, duplicados removidos |
| **S3** | MongoDB Atlas + queries ejecutivas | `03_mongodb_kpis.ipynb` | 3 colecciones, MTTR/SLA por área, top 10 incidencias recurrentes |
| **S4** | **EP: Arquitectura + EDA + Pipeline + NLP base** | `presentacion_EP_grupo6.pdf` | 60% proyecto: problema, datos, pipeline y primeros insights NLP |
| **S5** | NLP español + Topic Modeling LDA | `04_nlp_topics.ipynb` | spaCy pipeline, 6+ temas descubiertos, clusters de texto identificados |
| **S6** | K-Means clusters + Prophet forecasting | `05_clustering_forecasting.ipynb` | 8 brechas identificadas, predicción 4 semanas con MAPE < 15% |
| **S7** | Scraping + Great Expectations calidad | `06_calidad_scraping.ipynb` | Validación de datos, reporte de calidad, integración fuentes externas |
| **S8** | **EF: Dashboard ejecutivo + resultados** | `presentacion_EF_grupo6.pdf` | Demo dashboard en vivo para "CIO", resultados cuantificados |

### Semana 4 — Evaluación Parcial EP (60% del proyecto)

El grupo debe presentar en 20 minutos ante la "Alta Dirección" (simular que son el CIO/CTO):

1. **El problema de negocio** — ¿Por qué el gerente TI necesita Big Data para gestionar tickets?
2. **Dataset y EDA** — Resumen de los 100K tickets: distribución, áreas críticas, tendencias
3. **Arquitectura Medallion** — Diagrama Bronze→Silver→Gold explicado con datos reales
4. **Pipeline PySpark funcionando** — Demo en vivo: ingresar ticket sucio → salida limpia en Gold
5. **MongoDB Atlas** — 3 queries que el CIO haría: "¿cuál es mi área con más tickets este año?"
6. **Primeros insights** — Al menos 3 hallazgos accionables para la dirección

### Semana 8 — Evaluación Final EF (proyecto completo)

Presentación de 25 minutos + demo interactiva del dashboard:

1. **Sistema completo end-to-end** — Pipeline desde ticket raw hasta dashboard ejecutivo
2. **Hallazgos de NLP** — "Descubrimos 6 temas latentes que el sistema de tickets no capturaba"
3. **Brechas detectadas** — "Las 3 brechas más críticas de TI según nuestro análisis Big Data"
4. **Predicciones** — "El mes de cierre trimestral genera 35% más tickets en el área de Finanzas"
5. **Impacto cuantificado** — ¿Cuántos tickets se podrían evitar con intervención proactiva?
6. **Recomendaciones ejecutivas** — Plan de acción basado en datos para el CIO
7. **Draft del paper Scopus** — Abstract + introducción + metodología (500 palabras)

---

## 8. Estructura del Repositorio

```
grupo6-itsm-analytics-bd/
│
├── README.md                              ← este archivo (guía completa)
│
├── notebooks/
│   ├── 01_EDA_tickets_ti.ipynb            ← Semana 1: análisis exploratorio
│   ├── 02_pipeline_medallion.ipynb        ← Semana 2: Bronze→Silver→Gold
│   ├── 03_mongodb_kpis.ipynb              ← Semana 3: NoSQL + KPIs ejecutivos
│   ├── 04_nlp_topics.ipynb               ← Semana 5: NLP español + LDA
│   ├── 05_clustering_forecasting.ipynb    ← Semana 6: K-Means + Prophet
│   ├── 06_calidad_scraping.ipynb          ← Semana 7: Great Expectations
│   └── 07_dashboard_ejecutivo.ipynb       ← Semana 8: Dashboard CIO
│
├── src/
│   ├── generador_tickets.py               ← Faker: 100K tickets en español
│   ├── pipeline_medallion.py             ← PySpark ETL Bronze→Silver→Gold
│   ├── nlp_pipeline.py                   ← spaCy + TF-IDF + LDA
│   ├── modelo_clustering.py              ← K-Means + análisis de brechas
│   ├── forecasting_prophet.py            ← Prophet + ARIMA por área
│   └── kpis_itil.py                      ← cálculo MTTR, SLA, MTBF
│
├── data/
│   ├── README_datos.md                   ← descripción de datos y cómo generarlos
│   ├── raw/                              ← tickets crudos (NO en GitHub, generar local)
│   ├── processed/                        ← datos Gold procesados (NO en GitHub)
│   └── sample/
│       ├── tickets_muestra_100.csv       ← 100 tickets de ejemplo (SÍ en GitHub)
│       └── categorias_ti.json            ← catálogo de categorías ITIL
│
├── docs/
│   ├── arquitectura_itsm_bigdata.png     ← diagrama de arquitectura
│   ├── diccionario_datos.md              ← definición de cada campo
│   ├── kpis_ejecutivos.md               ← definición de KPIs ITIL usados
│   ├── presentacion_EP_semana4.pdf       ← sustentación parcial
│   └── presentacion_EF_semana8.pdf       ← sustentación final
│
├── .gitignore
└── requirements.txt
```

---

## 9. Cómo Usar Este Repositorio

### Configuración inicial (una sola vez por integrante)

```bash
# Paso 1: Fork de este repo en GitHub
# → Ir a github.com/RubenCarty/grupo6-itsm-analytics-bd
# → Clic "Fork" → "Create fork"

# Paso 2: Clonar TU fork
git clone https://github.com/TU-USUARIO/grupo6-itsm-analytics-bd.git
cd grupo6-itsm-analytics-bd

# Paso 3: Conectar con el repo del líder del grupo
git remote add upstream https://github.com/LIDER-GRUPO/grupo6-itsm-analytics-bd.git

# Verificar configuración
git remote -v
# origin   → tu fork personal (donde subes tu trabajo)
# upstream → repo del líder del grupo (de donde sincronizas)

# Paso 4: Instalar dependencias
pip install -r requirements.txt
python -m spacy download es_core_news_sm

# Paso 5: Generar el dataset (primera vez)
python src/generador_tickets.py
# → Crea data/raw/tickets_historicos.csv (100K registros)
```

### Flujo semanal de trabajo

```bash
# Al inicio de cada semana: sincronizar con el grupo
git checkout main
git pull upstream main           # traer cambios del líder/compañeros
git push origin main             # actualizar tu fork

# Crear tu rama de trabajo
git checkout -b feature/nlp-pipeline-tuapellido
# Formato: feature/descripcion-tuapellido

# Trabajar en el notebook de la semana...
# jupyter notebook notebooks/04_nlp_topics.ipynb

# Guardar tu progreso
git add notebooks/04_nlp_topics.ipynb src/nlp_pipeline.py
git commit -m "feat: NLP pipeline spaCy + LDA 6 topics - TuApellido"
git push origin feature/nlp-pipeline-tuapellido

# En GitHub: crear Pull Request → el líder del grupo revisa → merge a main
```

### Actualizar tu local cuando el líder hace merge de un PR

```bash
git checkout main
git pull upstream main   # descarga los últimos cambios aprobados
git push origin main     # sincroniza tu fork también
```

### Comandos útiles para este proyecto

```bash
# Ver el historial de commits del grupo
git log --oneline --graph --all

# Ver en qué rama estás
git branch

# Ver qué cambió antes de hacer commit
git diff notebooks/04_nlp_topics.ipynb

# Si te equivocaste en un commit: deshacerlo SIN perder el trabajo
git reset HEAD~1 --soft   # deshace el commit pero guarda los cambios
```

---

## 10. Recursos y Referencias

### Datos reales disponibles (públicos)
- [Stack Overflow Annual Developer Survey](https://insights.stackoverflow.com/survey) — tendencias en soporte técnico
- [IT Incident Data — Kaggle](https://www.kaggle.com/datasets/faisalahmed/it-incident-management)
- [ServiceNow Sample Dataset](https://developer.servicenow.com/dev.do)
- [GLPI Open Source — Demo con datos](https://glpi-project.org/)
- [UCI Machine Learning — IT Incident Management](https://archive.ics.uci.edu/dataset/498/incident+management+process+enriched+event+log)

### Repositorios GitHub similares
- [IT Incident Classification NLP](https://github.com/laugustyniak/awesome-nlp-polish) — NLP en otros idiomas
- [Prophet Time Series Forecasting](https://github.com/facebook/prophet)
- [spaCy Spanish NLP](https://github.com/explosion/spacy-models)
- [K-Means con PySpark MLlib](https://github.com/apache/spark/tree/master/examples/src/main/python/ml)
- [ITIL KPIs Dashboard Streamlit](https://github.com/streamlit/streamlit)
- [Great Expectations Data Quality](https://github.com/great-expectations/great_expectations)
- [Gensim LDA Topic Modeling](https://github.com/RaRe-Technologies/gensim)
- [scikit-learn TF-IDF + Clustering](https://github.com/scikit-learn/scikit-learn)

### Papers para artículo Scopus Q1
- Marrone & Kolbe (2011) — "Uncovering ITIL Claims: IT Executives' Perception on Benefits and Barriers" — European Journal of Information Systems (Q1 AJG 4)
- Yusop et al. (2023) — "Prediction of IT Service Desk Ticket Volume Using Machine Learning" — IEEE Access (Q1)
- Diao & Bhatt (2004) — "An Approach Toward Automatic Classification of Tool Alerts" — ISAS — base para NLP en incidencias
- Zolotukhin et al. (2016) — "Semi-Supervised Anomaly Detection in IT Ticket Data" — IEEE INFOCOM
- **Referencia clave:** Cha et al. (2020) — "Automatic Incident Triage and Assignment in Cloud Systems Using NLP and ML" — ACM (muy citado)

### Herramientas gratuitas para el proyecto
| Herramienta | Link | Para qué |
|-------------|------|---------|
| Google Colab | colab.research.google.com | Notebooks + GPU para NLP |
| Databricks Community | community.cloud.databricks.com | PySpark Medallion |
| MongoDB Atlas M0 | mongodb.com/atlas | KPIs ejecutivos |
| Streamlit Cloud | streamlit.io/cloud | Dashboard CIO |
| Prophet (Meta) | facebook.github.io/prophet | Forecasting tickets |
| spaCy es | spacy.io/models/es | NLP en español |
| draw.io | app.diagrams.net | Diagrama arquitectura |

---

## 11. Diccionario de KPIs Ejecutivos

```
MTTR (Mean Time To Resolve)
  = Promedio del tiempo desde creación hasta cierre del ticket
  = Suma(tiempo_resolucion_min) / Cantidad_tickets
  Meta: < 240 min para alta, < 480 min para media

MTBF (Mean Time Between Failures)
  = Tiempo promedio entre incidencias del mismo tipo en la misma área
  Meta: > 30 días (incidencia recurrente si < 30 días)

SLA Compliance
  = (Tickets resueltos antes del SLA objetivo / Total tickets) × 100
  Meta global: > 95%

First Contact Resolution (FCR)
  = (Tickets resueltos sin escalar / Total) × 100
  Meta: > 70%

Brecha Index (nuevo — creado por este proyecto)
  = (Tickets recurrentes por misma causa raíz / Total) × 100
  Interpretación: < 10% bien gestionado; 10-25% mejora necesaria; > 25% brecha crítica

Ticket Abandonment Rate
  = Tickets cerrados sin solución (usuario desistió) / Total × 100
  Meta: < 3%
```

---

## 12. Tracker de Progreso del Grupo

| Semana | Entregable | Responsable | Estado | Link PR |
|--------|-----------|-------------|--------|---------|
| S1 | Dataset 100K + EDA completo | [Nombre] | ⬜ Pendiente | — |
| S2 | Pipeline Medallion PySpark | [Nombre] | ⬜ Pendiente | — |
| S3 | MongoDB Atlas + KPIs | [Nombre] | ⬜ Pendiente | — |
| S4 | **Sustentación EP (20 min)** | Todos | ⬜ Pendiente | — |
| S5 | NLP spaCy + LDA Topics | [Nombre] | ⬜ Pendiente | — |
| S6 | K-Means Brechas + Prophet | [Nombre] | ⬜ Pendiente | — |
| S7 | Great Expectations + Scraping | [Nombre] | ⬜ Pendiente | — |
| S8 | **Sustentación EF (25 min)** | Todos | ⬜ Pendiente | — |

---

## 13. Checklist de Entrega Final (Semana 8)

**Repositorio GitHub:**
- [ ] README.md completamente actualizado con resultados reales
- [ ] Los 7 notebooks ejecutados con outputs visibles (no vacíos)
- [ ] `src/` con código limpio y comentado
- [ ] `docs/arquitectura_itsm_bigdata.png` — diagrama final
- [ ] `docs/presentacion_EF_semana8.pdf` — slides finales
- [ ] `docs/diccionario_datos.md` — actualizado

**Resultados mínimos requeridos:**
- [ ] Dataset de 100K tickets generado y documentado
- [ ] Pipeline Medallion funcionando (Bronze → Silver → Gold)
- [ ] Al menos 6 temas LDA identificados con interpretación de negocio
- [ ] Al menos 5 brechas de TI detectadas mediante K-Means
- [ ] Predicción de volumen próximo mes con MAPE < 20%
- [ ] Dashboard ejecutivo con mínimo 6 KPIs visualizados
- [ ] Abstract del paper Scopus redactado (200 palabras en inglés)

---

*Big Data DD283 | Universidad Autónoma del Perú | 2026-1*  
*Dudas del proyecto: abrir un Issue en este repositorio*
