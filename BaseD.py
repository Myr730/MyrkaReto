from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("CybersecurityAnalysis")\
        .config("spark.sql.files.maxPartitionBytes", "128MB") \  # Ajusta el tamaño máximo de las particiones
        .config("spark.sql.files.maxColumns", "20000") \  # Ajusta el límite de columnas procesadas
        .getOrCreate()

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

    # 🔹 Describir la tabla de ataques cibernéticos
    query = "DESCRIBE cyber_attacks"
    spark.sql(query).show(20)

    # 🔹 Consulta SQL: Top 5 países con más ataques
    query = """SELECT Country, COUNT(*) AS num_attacks FROM cyber_attacks GROUP BY Country ORDER BY num_attacks DESC LIMIT 5"""
    spark.sql(query).show(20)

    # 🔹 Análisis de pérdidas financieras por tipo de ataque
    query = """SELECT Attack_Type, SUM(Financial_Loss) AS Total_Loss FROM cyber_attacks GROUP BY Attack_Type ORDER BY Total_Loss DESC"""
    spark.sql(query).show(20)

    # 🔹 Filtrado de datos por país
    selected_country = "USA"  
    query_filtered = f"""SELECT * FROM cyber_attacks WHERE Country = '{selected_country}' """
    df_filtered = spark.sql(query_filtered)
    
    results = df_filtered.toJSON().collect() 
    with open('filtered_data.json', 'w') as file:
        json.dump(results, file)

    print("Datos guardados en `filtered_data.json`")

    spark.stop()
