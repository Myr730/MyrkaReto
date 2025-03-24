from pyspark.sql import SparkSession
import streamlit as st
import json

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

# 🔹 Título del dashboard
st.title("📊 Análisis de Amenazas de Ciberseguridad (2015-2024)")

# 🔹 Vista previa de datos (sin pandas)
st.subheader("🔍 Datos de Amenazas")
st.dataframe(df_spark.limit(10).toPandas())

# 🔹 Consulta SQL: Top 5 países con más ataques
query = """SELECT Country, COUNT(*) AS num_attacks FROM cyber_attacks GROUP BY Country ORDER BY num_attacks DESC LIMIT 5"""
df_top_countries = spark.sql(query)

st.subheader("🌍 Top 5 Países con Más Ataques")
# Convertir a Pandas solo para el gráfico
df_top_countries_pandas = df_top_countries.toPandas()
st.bar_chart(df_top_countries_pandas.set_index("Country"))

# 🔹 Análisis de pérdidas financieras por ataque
st.subheader("💰 Pérdidas Financieras por Tipo de Ataque")
query = """SELECT Attack_Type, SUM(Financial_Loss) AS Total_Loss FROM cyber_attacks GROUP BY Attack_Type ORDER BY Total_Loss DESC"""
df_loss = spark.sql(query)

# Convertir a Pandas solo para el gráfico
df_loss_pandas = df_loss.toPandas()
st.bar_chart(df_loss_pandas.set_index("Attack_Type")["Total_Loss"])

# 🔹 Filtrado de datos por país
st.subheader("🔎 Filtrar Ataques por País")
country_list = [row.Country for row in df_spark.select("Country").distinct().collect()]
selected_country = st.selectbox("Selecciona un país:", country_list)

# Filtrar los datos de PySpark según el país seleccionado
df_filtered = df_spark.filter(df_spark.Country == selected_country)
st.dataframe(df_filtered.limit(10).toPandas())

# 🔹 Guardar datos en JSON
results = df_filtered.toJSON().collect()
with open('filtered_data.json', 'w') as file:
    json.dump(results, file)

st.success("📂 Datos guardados en `filtered_data.json`")
