import streamlit as st
import pandas as pd
import json
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession

# 🔹 Iniciar sesión de Spark
spark = SparkSession.builder.appName("CybersecurityAnalysis").getOrCreate()

# 🔹 Cargar el dataset en Spark
file_path = "Global_Cybersecurity_Threats_2015-2024.csv"  # Cambia esto al nombre correcto
df_spark = spark.read.csv(file_path, header=True, inferSchema=True)

# 🔹 Renombrar columnas para evitar espacios en nombres
df_spark = df_spark.withColumnRenamed("Attack Type", "Attack_Type") \
                   .withColumnRenamed("Target Industry", "Target_Industry") \
                   .withColumnRenamed("Financial Loss (in million $)", "Financial_Loss") \
                   .withColumnRenamed("Number of Affected Users", "Affected_Users") \
                   .withColumnRenamed("Attack Source", "Attack_Source") \
                   .withColumnRenamed("Security Vulnerability Type", "Vulnerability_Type") \
                   .withColumnRenamed("Defense Mechanism Used", "Defense_Mechanism") \
                   .withColumnRenamed("Incident Resolution Time (in Hours)", "Resolution_Time")

df_spark.createOrReplaceTempView("cyber_attacks")

# 🔹 Convertir a Pandas para visualización
df = df_spark.toPandas()

# 🔹 Título del dashboard
st.title("📊 Análisis de Amenazas de Ciberseguridad (2015-2024)")

# 🔹 Vista previa de datos
st.subheader("🔍 Datos de Amenazas")
st.dataframe(df.head())

# 🔹 Consulta SQL: Top 5 países con más ataques
query = """SELECT Country, COUNT(*) AS num_attacks FROM cyber_attacks GROUP BY Country ORDER BY num_attacks DESC LIMIT 5"""
df_top_countries = spark.sql(query).toPandas()

st.subheader("🌍 Top 5 Países con Más Ataques")
st.bar_chart(df_top_countries.set_index("Country"))

# 🔹 Análisis de pérdidas financieras por ataque
st.subheader("💰 Pérdidas Financieras por Tipo de Ataque")
query = """SELECT Attack_Type, SUM(Financial_Loss) AS Total_Loss FROM cyber_attacks GROUP BY Attack_Type ORDER BY Total_Loss DESC"""
df_loss = spark.sql(query).toPandas()

fig, ax = plt.subplots(figsize=(10, 5))
sns.barplot(data=df_loss, x="Total_Loss", y="Attack_Type", ax=ax)
st.pyplot(fig)

# 🔹 Filtrado de datos por país
st.subheader("🔎 Filtrar Ataques por País")
selected_country = st.selectbox("Selecciona un país:", df["Country"].unique())
df_filtered = df[df["Country"] == selected_country]
st.dataframe(df_filtered)

# 🔹 Guardar datos en JSON
results = df_filtered.to_json(orient="records")
with open('filtered_data.json', 'w') as file:
    json.dump(results, file)

st.success("📂 Datos guardados en `filtered_data.json`")

# 🔹 Finalizar Spark
spark.stop()
