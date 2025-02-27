# 🚀 Apache Spark Delta Lake - Ejemplos de Uso

Este repositorio contiene dos ejemplos completos de uso de **Delta Lake** con **Apache Spark 3.5.5 y Hadoop**.  
Incluyen operaciones básicas como creación, escritura, lectura, merge, historial de cambios, eliminación del historial y streaming.

---

## 📌 Requisitos

- **Apache Spark 3.5.5** con Hadoop
- **Delta Lake 3.3.0**
- Entorno con soporte para `spark-shell`

---

## 🔧 Configuración del Entorno

Ejecuta **Spark Shell** con el paquete de **Delta Lake**:

```bash
spark-shell --packages io.delta:delta-spark_2.12:3.3.0
