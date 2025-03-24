from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CybersecurityAnalysis").getOrCreate()
    
    print("Leyendo el archivo CSV...")
    file_path = "Global_Cybersecurity_Threats_2015-2024.csv"

    try:
        df_spark = spark.read.option("maxColumns", 20480).csv(file_path, header=True, inferSchema=True)
        df_spark.show(5)  # Verifica las primeras filas
        df_spark.printSchema()  # Muestra la estructura de las columnass
    except Exception as e:
        print(f"Error al leer el CSV: {e}")
        spark.stop()
        exit()

    # Renombrar columnas
    df_spark = df_spark.withColumnRenamed("Attack Type", "Attack_Type") \
                       .withColumnRenamed("Target Industry", "Target_Industry") \
                       .withColumnRenamed("Financial Loss (in million $)", "Financial_Loss") \
                       .withColumnRenamed("Number of Affected Users", "Affected_Users") \
                       .withColumnRenamed("Attack Source", "Attack_Source") \
                       .withColumnRenamed("Security Vulnerability Type", "Vulnerability_Type") \
                       .withColumnRenamed("Defense Mechanism Used", "Defense_Mechanism") \
                       .withColumnRenamed("Incident Resolution Time (in Hours)", "Resolution_Time")

    # Registrar DataFrame como tabla en Spark
    df_spark.createOrReplaceTempView("cyber_attacks")

    # Top 5 países con más ataques
    query = """SELECT Country, COUNT(*) AS num_attacks FROM cyber_attacks GROUP BY Country ORDER BY num_attacks DESC LIMIT 5"""
    spark.sql(query).show()

    # Análisis de pérdidas financieras por tipo de ataque
    query = """SELECT Attack_Type, SUM(Financial_Loss) AS Total_Loss FROM cyber_attacks GROUP BY Attack_Type ORDER BY Total_Loss DESC"""
    spark.sql(query).show()

    # Filtrado de datos por país
    selected_country = "USA"
    query_filtered = f"""SELECT * FROM cyber_attacks WHERE Country = '{selected_country}'"""
    df_filtered = spark.sql(query_filtered)

    results = df_filtered.toJSON().collect()
    with open('filtered_data.json', 'w') as file:
        json.dump(results, file)

    print("Datos guardados en `filtered_data.json`")
    spark.stop()
