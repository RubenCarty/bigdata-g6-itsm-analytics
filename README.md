# 📊 Gestión Predictiva de Incidencias TI (Enfoque Lean Big Data)

> **Curso:** Big Data DD283  
> **Universidad:** Universidad Autónoma del Perú  
> **Periodo:** 2026-1  
> **Grupo:** 6  
> **Sector:** Tecnologías de la Información / ITSM / Gobierno de TI  

## 👥 Integrantes

- Armando Carbajal
- Jamphier Paipay

---

# 1. 📌 Caso de Negocio

## Contexto

Las organizaciones medianas y grandes en Perú gestionan miles de tickets de TI mensuales. La alta dirección necesita responder preguntas críticas, pero actualmente se enfrenta a un ecosistema de reportes fragmentado.

Las incidencias ya no solo llegan por sistemas estructurados como **ServiceNow** o **GLPI**, sino que también existe un entorno de **Shadow IT**, donde los reportes urgentes ingresan mediante bots de WhatsApp, correos electrónicos o transcripciones de llamadas de emergencia.

## Situación Actual (Puntos de Dolor)

- Reportes manuales aislados que toman días en consolidarse.
- Información fragmentada en múltiples formatos: Excel, JSON y TXT.
- Categorizaciones erróneas o descripciones libres que dificultan detectar problemas repetitivos.
- Ausencia de una visión centralizada para la alta dirección.
- Reacción tardía ante picos de demanda operativa.
- Sobrecarga y desgaste del personal de soporte técnico.

## El Problema

Construir un **Pipeline de Big Data** capaz de ingerir, procesar y unificar información histórica de incidencias estructuradas y semi-estructuradas para:

- Detectar patrones mediante extracción de palabras clave.
- Agrupar incidencias similares.
- Proyectar tendencias futuras.
- Predecir la carga operativa.
- Generar un dashboard ejecutivo centralizado (**Single Source of Truth**).
- Pasar de una gestión reactiva a una gestión proactiva basada en datos.

---

# 2. 🎯 Objetivos del Proyecto

## Objetivo General

Desarrollar un sistema de **Big Data e Inteligencia de Negocios** utilizando una **Arquitectura Medallion**, integrando incidencias de TI provenientes de múltiples fuentes, permitiendo:

- Detección de brechas de servicio.
- Análisis de garantías.
- Visualización predictiva de tendencias.
- Soporte a la toma de decisiones estratégicas.

## Objetivos Específicos

- Configurar un **Data Lake** para centralizar datos heterogéneos.
- Construir un pipeline ETL distribuido utilizando **PySpark** en Databricks.
- Implementar las capas **Bronze, Silver y Gold**.
- Aplicar técnicas de procesamiento de texto mediante:
  - Regex
  - String Functions
- Almacenar información consolidada en **MongoDB Atlas**.
- Desarrollar un dashboard ejecutivo con **Streamlit + Plotly**.
- Implementar un modelo predictivo con **Facebook Prophet**.

---

# 3. 🚀 Justificación Big Data (Las 5 V)

| Característica | Aplicación en el Proyecto |
|---------------|---------------------------|
| **Volumen** | Procesamiento de históricos masivos de tickets de soporte. |
| **Velocidad** | Actualización continua del Data Lake y dashboard. |
| **Variedad** | Integración de Excel, JSON y TXT. |
| **Veracidad** | Limpieza y validación de datos en Silver Layer. |
| **Valor** | Conversión de datos en recomendaciones gerenciales. |

---

# 4. 🏗️ Arquitectura del Proyecto

El sistema implementa una **Arquitectura Medallion** basada en tecnologías Cloud y Open Source.

```text
Fuentes de Datos
(Excel / JSON / TXT)
          │
          ▼
┌─────────────────┐
│  Data Lake      │
│   (Bronze)      │
└─────────────────┘
          │
          ▼
┌─────────────────┐
│ PySpark ETL     │
│   (Silver)      │
└─────────────────┘
          │
          ▼
┌─────────────────┐
│ Datos Agregados │
│     (Gold)      │
└─────────────────┘
          │
          ▼
┌─────────────────┐
│ MongoDB Atlas   │
└─────────────────┘
          │
          ▼
┌─────────────────┐
│ Streamlit       │
│ + Plotly        │
└─────────────────┘
          │
          ▼
┌─────────────────┐
│ Prophet ML      │
│ Predicciones    │
└─────────────────┘
```

---

# 🛠️ Stack Tecnológico

| Categoría | Herramienta |
|------------|------------|
| Data Lake | Archivos Excel, JSON, TXT |
| Procesamiento ETL | Databricks + PySpark |
| Arquitectura | Medallion (Bronze / Silver / Gold) |
| Base de Datos | MongoDB Atlas |
| Visualización | Streamlit |
| Gráficos | Plotly |
| Machine Learning | Prophet |
| Lenguaje | Python |

---

# 5. 📊 Dashboard Gerencial

La aplicación fue desarrollada en **Streamlit** e incluye filtros globales:

- Año
- Mes
- Hardware
- Garantía

## 📈 Visión General

- Evolución temporal de tickets.
- Tendencias mensuales.
- Top 10 incidencias más frecuentes.

### Visualizaciones

- Gráfico de líneas
- Barras horizontales

---

## 🛡️ Análisis de Brechas y Hardware

- Fallas por garantía.
- Fallas fuera de cobertura.
- Impacto operativo.

### Visualizaciones

- Barras agrupadas
- Bubble Chart (CSAT vs Minutos Perdidos)

---

## 🔎 Drill Down por Equipos

Análisis detallado por componente:

- CPU
- RAM
- Disco
- Impresoras
- Monitores

### Indicadores

- Costos ocultos.
- Fallas sin garantía.

### Visualización

- Donut Chart

---

## 🔮 Predicción de Demanda

Modelo de Machine Learning utilizando **Facebook Prophet**.

### Capacidades

- Predicción de 1 a 12 meses.
- Pronóstico por:
  - Hardware
  - Tipo de problema
- Generación automática de recomendaciones.

### Ejemplos de recomendaciones

- Automatización de procesos.
- Creación de manuales de autoservicio.
- Mantenimientos preventivos.
- Renovación tecnológica.

---

# 6. 📂 Estructura del Proyecto

```text
Proyecto-BigData-TI/
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── notebooks/
│   ├── etl_bronze.ipynb
│   ├── etl_silver.ipynb
│   └── etl_gold.ipynb
│
├── app/
│   ├── app.py
│   ├── dashboard/
│   └── utils/
│
├── models/
│   └── prophet_model.pkl
│
├── .streamlit/
│   └── secrets.toml
│
├── requirements.txt
├── README.md
└── LICENSE
```

---

# 7. ⚙️ Instalación y Ejecución

## 1️⃣ Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/tu-repositorio.git
cd tu-repositorio
```

## 2️⃣ Crear entorno virtual

```bash
python -m venv venv
```

### Windows

```bash
venv\Scripts\activate
```

### Linux / Mac

```bash
source venv/bin/activate
```

---

## 3️⃣ Instalar dependencias

```bash
pip install -r requirements.txt
```

---

## 4️⃣ Configurar MongoDB Atlas

Crear el archivo:

```text
.streamlit/secrets.toml
```

Contenido:

```toml
MONGO_URI = "mongodb+srv://usuario:password@cluster.mongodb.net/?retryWrites=true&w=majority"
```

---

## 5️⃣ Ejecutar Streamlit

```bash
streamlit run app.py
```

---

# 📌 Resultados Esperados

✅ Centralización de datos de TI.  
✅ Reducción de tiempos de análisis.  
✅ Identificación de brechas operativas.  
✅ Predicción de demanda futura.  
✅ Soporte a decisiones estratégicas.  
✅ Transformación de datos en valor de negocio.

---

# 📄 Licencia

Proyecto académico desarrollado para el curso **Big Data DD283** de la **Universidad Autónoma del Perú**.

---

## ⭐ Si este proyecto te resulta útil, no olvides darle una estrella al repositorio.