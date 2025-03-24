from pyspark.sql import SparkSession
import json
import streamlit as st

if __name__ == "__main__":
    # 🔹 Iniciar sesión de Spark
    spark = SparkSession.builder.appName("CybersecurityAnalysis").getOrCreate()

    st.title("📊 Análisis de Amenazas de Ciberseguridad (2015-2024)")

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

    #  Consulta SQL: Top 5 países con más ataques
    query = """SELECT Country, COUNT(*) AS num_attacks FROM cyber_attacks GROUP BY Country ORDER BY num_attacks DESC LIMIT 5"""
    df_top_countries = spark.sql(query)
    df_top_countries.show()

    # Análisis de pérdidas financieras por ataque
    st.subheader("Pérdidas Financieras por Tipo de Ataque")
    query = """SELECT Attack_Type, SUM(Financial_Loss) AS Total_Loss FROM cyber_attacks GROUP BY Attack_Type ORDER BY Total_Loss DESC"""
    df_loss = spark.sql(query)
    df_loss.show()

    # Filtrado de datos por país
    st.subheader("Filtrar Ataques por País")
    country_list = [row.Country for row in df_spark.select("Country").distinct().collect()]
    selected_country = st.selectbox("Selecciona un país:", country_list)

    # Filtrar los datos según el país seleccionado
    query_filtered = f"""SELECT * FROM cyber_attacks WHERE Country = "{selected_country}" """
    df_filtered = spark.sql(query_filtered)

    # Guardar datos en JSON
    results = df_filtered.toJSON().collect()
    with open('filtered_data.json', 'w') as file:
        json.dump(results, file)

    st.success("📂 Datos guardados en `filtered_data.json`")
    spark.stop()

