from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    # Iniciar sesión de Spark
    spark = SparkSession.builder.appName("CybersecurityAnalysis").getOrCreate()

    # Cargar el dataset en Spark
    file_path = "Global_Cybersecurity_Threats_2015-2024.csv"  
    df_spark = spark.read.csv(file_path, header=True, inferSchema=True)

    # Renombrar columnas para evitar espacios en nombres
    df_spark = df_spark.withColumnRenamed("Attack Type", "Attack_Type") \
                       .withColumnRenamed("Target Industry", "Target_Industry") \
                       .withColumnRenamed("Financial Loss (in million $)", "Financial_Loss") \
                       .withColumnRenamed("Number of Affected Users", "Affected_Users") \
                       .withColumnRenamed("Attack Source", "Attack_Source") \
                       .withColumnRenamed("Security Vulnerability Type", "Vulnerability_Type") \
                       .withColumnRenamed("Defense Mechanism Used", "Defense_Mechanism") \
                       .withColumnRenamed("Incident Resolution Time (in Hours)", "Resolution_Time")

    df_spark.createOrReplaceTempView("cyber_attacks")

    # Consulta SQL: Top 5 países con más ataques
    query = """SELECT Country, COUNT(*) AS num_attacks FROM cyber_attacks GROUP BY Country ORDER BY num_attacks DESC LIMIT 5"""
    df_top_countries = spark.sql(query)
    df_top_countries.show()

    # Análisis de pérdidas financieras por ataque
    query = """SELECT Attack_Type, SUM(Financial_Loss) AS Total_Loss FROM cyber_attacks GROUP BY Attack_Type ORDER BY Total_Loss DESC"""
    df_loss = spark.sql(query)
    df_loss.show()

    # Filtrado de datos por país (puedes elegir un país manualmente o hacerlo de otra forma)
    selected_country = "USA"  # Puedes poner el país manualmente para probar
    query_filtered = f"""SELECT * FROM cyber_attacks WHERE Country = "{selected_country}" """
    df_filtered = spark.sql(query_filtered)
    
    results = df_filtered.toJSON().collect()
    with open('filtered_data.json', 'w') as file:
        json.dump(results, file)

    print("📂 Datos guardados en `filtered_data.json`")

    spark.stop()
