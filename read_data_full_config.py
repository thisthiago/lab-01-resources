#%pyspark

# 1. Configuração ESSENCIAL para MinIO
from pyspark.sql import SparkSession

# Encerrar qualquer sessão existente (importante!)
spark.stop()

# Criar nova sessão com todas as configurações necessárias
spark = SparkSession.builder \
    .appName("MinIO-Access-Fix") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "senhasegura") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

# 2. Configuração adicional para garantir autenticação
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "admin")
hadoop_conf.set("fs.s3a.secret.key", "senhasegura")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# 3. Verificação EXTRA das configurações
print("\nVerificação EXTRA das configurações:")
print("Endpoint:", hadoop_conf.get("fs.s3a.endpoint"))
print("Access Key:", hadoop_conf.get("fs.s3a.access.key"))
print("Secret Key:", hadoop_conf.get("fs.s3a.secret.key"))
print("Credentials Provider:", hadoop_conf.get("fs.s3a.aws.credentials.provider"))

# 4. Teste DIRETO via API Hadoop
try:
    print("\nTestando conexão via API Hadoop...")
    URI = spark.sparkContext._jvm.java.net.URI
    Path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path
    FileSystem = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem
    
    fs = FileSystem.get(URI.create("s3a://raw-bucket"), hadoop_conf)
    status = fs.listStatus(Path("/"))
    print(f"\nConexão bem-sucedida! Encontrados {len(status)} itens no bucket raiz")
    
    # Leitura dos dados
    print("\nLendo dados específicos...")
    df = spark.read.parquet("s3a://raw-bucket/teste/")
    print("Leitura concluída com sucesso!")
    df.printSchema()
    df.show()
    
except Exception as e:
    print("\nFalha na conexão:")
    print("Tipo:", type(e))
    print("Mensagem:", str(e))
    
    # Diagnóstico avançado
    print("\nDIAGNÓSTICO AVANÇADO:")
    print("1. Execute no container Zeppelin: aws --endpoint-url http://minio:9000 s3 ls s3://raw-bucket/")
    print("2. Verifique os logs do MinIO: docker logs minio")
    print("3. Confira se o bucket existe: docker exec minio mc ls local/raw-bucket")
    print("4. Verifique as políticas: docker exec minio mc policy list local/raw-bucket")
    
    raise e