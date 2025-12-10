import traceback
from datetime import datetime
from utils.config_manager import ConfigManager
from pyspark.sql.functions import lit, current_timestamp

class MeteoritesIngestor:
    def __init__(self):
        self.config_manager = ConfigManager()

        # Pegando configurações - CORRIGIDO os paths
        self.csv_file = self.config_manager.get('volumes', 'bronze', 'meteorites', 'csv_file')
        self.volume_path = self.config_manager.get('volumes', 'bronze', 'meteorites', 'path')
        self.csv_full_path = f"{self.volume_path}/{self.csv_file}"
        self.delta_path = self.config_manager.get('volumes', 'bronze', 'meteorites', 'delta_path')

        self.catalog : str = self.config_manager.get('catalog', 'name')
        self.schema : str = self.config_manager.get('catalog' , 'schemas', 'bronze')
        self.table : str = self.config_manager.get('table', 'bronze', 'meteorites')
    
    def read_data(self):
        try:
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("escape", '"') \
                .option("multiLine", "true") \
                .csv(self.csv_full_path)
            count = df.count()
            print(f"{count:,} registros carregados")
            print(f"Colunas: {len(df.columns)}")
            
            return df
        except Exception as e:
            print(f"Erro ao carregar dados: {e}")
            traceback.print_exc()
            raise
    
    def normalize_columns(self, df):
        for col in df.columns:
            df = df.withColumnRenamed(col, col.replace(" ", "_").replace("(", "").replace(")", "").lower())
        return df
    
    def add_metadata(self, df):
        return df \
            .withColumn("timestamp_ingestao", current_timestamp()) \
            .withColumn("source", lit("nasa_meteorite_landings")) \
            .withColumn("source_type", lit("volume_csv")) \
            .withColumn("source_path", lit(self.volume_path))
    
    def write_data(self, df):
        df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{self.catalog}.{self.schema}.{self.table}")
    
    def verify_data(self):
        """Verifica os dados salvos"""
        try:
            print(f"\nVerificando dados...")

            # Contar registros
            count = spark.sql(
                f"SELECT COUNT(*) as cnt FROM {self.catalog}.{self.schema}.{self.table}"
            ).collect()[0]['cnt']
            print(f"{count:,} registros na tabela")

            # Mostrar amostra
            print("\nAmostra (5 registros):")
            df_sample = spark.sql(
                f"SELECT * FROM {self.catalog}.{self.schema}.{self.table} LIMIT 5"
            )
            display(df_sample)

            # Estatísticas básicas
            print("\nEstatísticas:")
            df_stats = spark.sql(
                f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT name) as unique_names,
                    COUNT(DISTINCT recclass) as unique_types,
                    AVG(mass_g) as average_mass,
                    MIN(year) as earliest_year,
                    MAX(year) as latest_year
                FROM {self.catalog}.{self.schema}.{self.table}
                """
            )
            display(df_stats)

        except Exception as e:
            print(f"Erro na verificação: {e}")
            traceback.print_exc()
    
    def ingest(self):
        print("=" * 60)
        print("COSMIC INSIGHTS - INGESTÃO METEORITOS")
        print("=" * 60)
        print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        print("Carregando os dados...")
        df = self.read_data()
        print("Adicionando metadados...")
        df = self.add_metadata(df)
        print("Normalizando colunas...")
        df = self.normalize_columns(df)
        print("Salvando os dados...")
        self.write_data(df)
        print("Verificando os dados...")
        self.verify_data()

        print("\n" + "=" * 60)
        print("INGESTÃO CONCLUÍDA COM SUCESSO!")
        print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("Tabela salva no caminho ")
        print("=" * 60)
            
        return True

 # ===============================================================================================================


ingestor = MeteoritesIngestor()
ingestor.ingest()