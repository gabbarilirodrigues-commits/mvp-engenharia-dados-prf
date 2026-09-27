# Databricks notebook source
# MAGIC %md
# MAGIC # MVP - Engenharia de Dados
# MAGIC
# MAGIC ## Pipeline de Dados para Análise de Sinistros em Rodovias Federais Brasileiras
# MAGIC
# MAGIC ### Contexto e objetivo
# MAGIC
# MAGIC Os sinistros de trânsito nas rodovias federais brasileiras geram impactos para a sociedade, principalmente devido à quantidade de ocorrências, pessoas feridas e mortes registradas todos os anos.
# MAGIC
# MAGIC A Polícia Rodoviária Federal (PRF) disponibiliza dados abertos sobre os sinistros ocorridos nas rodovias federais. Esses dados apresentam informações sobre as ocorrências, como localização, causa, tipo de sinistro, condições da via e quantidade de vítimas.
# MAGIC
# MAGIC O objetivo deste MVP é construir um pipeline de dados em ambiente de nuvem utilizando dados públicos da PRF referentes aos anos de 2023, 2024 e 2025. Os dados serão carregados, tratados e organizados no Databricks, seguindo as etapas de dados brutos, dados tratados e dados preparados para análise.
# MAGIC
# MAGIC Ao final, os dados serão utilizados para responder perguntas relacionadas às características dos sinistros registrados nas rodovias federais brasileiras.
# MAGIC
# MAGIC ### Perguntas de negócio
# MAGIC
# MAGIC 1. Como evoluiu a quantidade de sinistros nas rodovias federais entre 2023 e 2025?
# MAGIC 2. Quais estados registraram as maiores quantidades de sinistros no período analisado?
# MAGIC 3. Quais foram as principais causas dos sinistros registrados?
# MAGIC 4. Em quais dias da semana ocorreu a maior quantidade de sinistros?
# MAGIC 5. Quais tipos de sinistros apresentaram maior quantidade de mortos e feridos?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Coleta e armazenamento dos dados
# MAGIC
# MAGIC Os dados utilizados neste projeto foram obtidos no Portal de Dados Abertos da Polícia Rodoviária Federal (PRF). Foram utilizados os arquivos de acidentes agrupados por ocorrência referentes aos anos de 2023, 2024 e 2025.
# MAGIC
# MAGIC Os arquivos foram disponibilizados em formato CSV e carregados no Databricks. Durante a importação foi utilizado o ponto e vírgula (;) como delimitador, conforme a estrutura original dos arquivos.
# MAGIC
# MAGIC Para representar a camada Bronze, foram criadas três tabelas com os dados originais de cada ano:
# MAGIC
# MAGIC - bronze_prf_2023
# MAGIC - bronze_prf_2024
# MAGIC - bronze_prf_2025
# MAGIC
# MAGIC Nesta etapa os dados foram mantidos próximos à sua estrutura original. Os tratamentos e padronizações serão realizados posteriormente na camada Silver.
# MAGIC

# COMMAND ----------

# Leitura das tabelas da camada Bronze
df_2023 = spark.table("workspace.default.bronze_prf_2023")
df_2024 = spark.table("workspace.default.bronze_prf_2024")
df_2025 = spark.table("workspace.default.bronze_prf_2025")

# Quantidade de registros por ano
print("Quantidade de registros em 2023:", df_2023.count())
print("Quantidade de registros em 2024:", df_2024.count())
print("Quantidade de registros em 2025:", df_2025.count())

# COMMAND ----------

# Verificação da estrutura das tabelas Bronze

print("=== ESTRUTURA 2023 ===")
df_2023.printSchema()

print("=== ESTRUTURA 2024 ===")
df_2024.printSchema()

print("=== ESTRUTURA 2025 ===")
df_2025.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tratamento e padronização dos dados - Camada Silver
# MAGIC
# MAGIC Após a carga dos dados na camada Bronze, foi realizada uma verificação da estrutura das tabelas dos três anos.
# MAGIC
# MAGIC Foi identificada uma diferença no tipo da coluna `id`: nos dados de 2023 e 2025 a coluna foi reconhecida como `long`, enquanto em 2024 foi reconhecida como `double`.
# MAGIC
# MAGIC Na camada Silver serão realizadas a união dos três anos e a padronização dos tipos de dados, além de tratamentos necessários para preparar a base para as análises.

# COMMAND ----------

# Comparação dos tipos de latitude e longitude nos três anos

print("2023:")
print("latitude =", df_2023.schema["latitude"].dataType)
print("longitude =", df_2023.schema["longitude"].dataType)

print("\n2024:")
print("latitude =", df_2024.schema["latitude"].dataType)
print("longitude =", df_2024.schema["longitude"].dataType)

print("\n2025:")
print("latitude =", df_2025.schema["latitude"].dataType)
print("longitude =", df_2025.schema["longitude"].dataType)

# COMMAND ----------

# Leitura das tabelas Bronze
df_2023 = spark.table("workspace.default.bronze_prf_2023")
df_2024 = spark.table("workspace.default.bronze_prf_2024")
df_2025 = spark.table("workspace.default.bronze_prf_2025")

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, year

# Função para padronizar os dados antes da união
def preparar_dados(df):
    return (
        df
        .withColumn("id", col("id").cast("long"))
        .withColumn(
            "latitude",
            regexp_replace(col("latitude").cast("string"), ",", ".").cast("double")
        )
        .withColumn(
            "longitude",
            regexp_replace(col("longitude").cast("string"), ",", ".").cast("double")
        )
    )

# Aplicação da padronização nos três anos
df_2023_pad = preparar_dados(df_2023)
df_2024_pad = preparar_dados(df_2024)
df_2025_pad = preparar_dados(df_2025)

# União das três bases
df_silver = (
    df_2023_pad
    .unionByName(df_2024_pad)
    .unionByName(df_2025_pad)
)

# Criação da coluna ano
df_silver = df_silver.withColumn("ano", year(col("data_inversa")))

# Validação
print("Total de registros:", df_silver.count())
print("Tipo latitude:", df_silver.schema["latitude"].dataType)
print("Tipo longitude:", df_silver.schema["longitude"].dataType)

df_silver.groupBy("ano").count().orderBy("ano").show()

# COMMAND ----------

# Verificação de IDs duplicados

ids_duplicados = (
    df_silver
    .groupBy("id")
    .count()
    .filter(col("count") > 1)
)

print("Quantidade de IDs duplicados:", ids_duplicados.count())

display(ids_duplicados)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col

# Verificação de valores nulos por coluna
nulos = df_silver.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_silver.columns
])

display(nulos)

# COMMAND ----------

from pyspark.sql.functions import col

# Verificação de valores negativos nas principais colunas numéricas
colunas_numericas = [
    "pessoas",
    "mortos",
    "feridos_leves",
    "feridos_graves",
    "feridos",
    "veiculos"
]

for coluna in colunas_numericas:
    quantidade = df_silver.filter(col(coluna) < 0).count()
    print(f"{coluna}: {quantidade} valores negativos")

# COMMAND ----------

# Gravação da camada Silver no Databricks

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_prf")

print("Tabela Silver criada com sucesso!")
print("Total de registros:", spark.table("workspace.default.silver_prf").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resultado e qualidade dos dados - Camada Silver
# MAGIC
# MAGIC Após o carregamento dos dados na camada Bronze, foi realizada a união das bases de 2023, 2024 e 2025 para a criação de uma base única.
# MAGIC
# MAGIC Durante a análise da estrutura dos dados, foi identificada uma diferença nos tipos das colunas de latitude e longitude entre os anos. Para permitir a união correta das bases, essas colunas foram padronizadas para o tipo numérico `double`. A coluna `id` também foi padronizada para o tipo `long`.
# MAGIC
# MAGIC Também foi criada a coluna `ano` a partir da data da ocorrência, facilitando as análises por período.
# MAGIC
# MAGIC Após a padronização, a base consolidada apresentou 213.451 registros, distribuídos da seguinte forma:
# MAGIC
# MAGIC - 2023: 67.766 registros
# MAGIC - 2024: 73.156 registros
# MAGIC - 2025: 72.529 registros
# MAGIC
# MAGIC Como parte da verificação da qualidade dos dados, foram analisados valores nulos, IDs duplicados e valores negativos nas principais colunas numéricas. Não foram encontrados IDs duplicados, valores nulos ou valores negativos nas verificações realizadas.
# MAGIC
# MAGIC Ao final do processo, os dados tratados foram armazenados na tabela `silver_prf`, que será utilizada como base para a criação da camada Gold e para as análises do projeto.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Camada Gold
# MAGIC
# MAGIC A partir dos dados tratados na camada Silver, foram criadas tabelas agregadas na camada Gold para facilitar as análises do projeto. As tabelas foram organizadas de acordo com as perguntas de negócio, considerando informações por ano, estado, causa do sinistro, dia da semana e tipo de sinistro.

# COMMAND ----------

from pyspark.sql.functions import count, sum

# Gold 1 - Resumo dos sinistros por ano
gold_sinistros_ano = (
    df_silver
    .groupBy("ano")
    .agg(
        count("*").alias("quantidade_sinistros"),
        sum("mortos").alias("total_mortos"),
        sum("feridos").alias("total_feridos")
    )
    .orderBy("ano")
)

# Gravação da tabela Gold no catálogo
gold_sinistros_ano.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_sinistros_ano")

display(gold_sinistros_ano)

# COMMAND ----------

from pyspark.sql.functions import count, sum

# Gold 2 - Resumo dos sinistros por estado
gold_sinistros_uf = (
    df_silver
    .groupBy("uf")
    .agg(
        count("*").alias("quantidade_sinistros"),
        sum("mortos").alias("total_mortos"),
        sum("feridos").alias("total_feridos")
    )
    .orderBy(col("quantidade_sinistros").desc())
)

# Gravação da tabela Gold no catálogo
gold_sinistros_uf.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_sinistros_uf")

display(gold_sinistros_uf)

# COMMAND ----------

from pyspark.sql.functions import count, col

# Gold 3 - Principais causas dos sinistros
gold_causas = (
    df_silver
    .groupBy("causa_acidente")
    .agg(
        count("*").alias("quantidade_sinistros")
    )
    .orderBy(col("quantidade_sinistros").desc())
)

# Gravação da tabela Gold no catálogo
gold_causas.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_causas")

display(gold_causas)

# COMMAND ----------

from pyspark.sql.functions import count

gold_dia_semana = (
    df_silver
    .groupBy("dia_semana")
    .agg(
        count("*").alias("quantidade_sinistros")
    )
    .orderBy("quantidade_sinistros", ascending=False)
)

display(gold_dia_semana)

# COMMAND ----------

gold_dia_semana.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("workspace.default.gold_dia_semana")

print("Tabela gold_dia_semana criada com sucesso!")

# COMMAND ----------

from pyspark.sql.functions import count, sum

gold_tipo_vitimas = (
    df_silver
    .groupBy("tipo_acidente")
    .agg(
        count("*").alias("quantidade_sinistros"),
        sum("mortos").alias("total_mortos"),
        sum("feridos").alias("total_feridos")
    )
    .orderBy("total_mortos", ascending=False)
)

display(gold_tipo_vitimas)

# COMMAND ----------

gold_tipo_vitimas.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("workspace.default.gold_tipo_vitimas")

print("Tabela gold_tipo_vitimas criada com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Análise dos dados
# MAGIC
# MAGIC Nesta etapa foram realizadas consultas sobre as tabelas da camada Gold para responder às perguntas de negócio definidas no início do projeto. Os resultados foram apresentados por meio de tabelas e visualizações para facilitar a interpretação dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pergunta 1 - Como evoluiu a quantidade de sinistros nas rodovias federais entre 2023 e 2025?
# MAGIC

# COMMAND ----------

# Pergunta 1
# Como evoluiu a quantidade de sinistros entre 2023 e 2025?

resultado_ano = spark.sql("""
    SELECT
        ano,
        quantidade_sinistros,
        total_mortos,
        total_feridos
    FROM workspace.default.gold_sinistros_ano
    ORDER BY ano
""")

display(resultado_ano)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resposta - Pergunta 1
# MAGIC
# MAGIC Entre 2023 e 2024 houve um aumento na quantidade de sinistros registrados nas rodovias federais, passando de 67.766 para 73.156 ocorrências. Em 2025 foram registrados 72.529 sinistros, apresentando uma pequena redução em relação ao ano anterior.
# MAGIC
# MAGIC Dessa forma, observa-se que 2024 apresentou a maior quantidade de sinistros entre os três anos analisados. Apesar da redução observada em 2025, a quantidade de ocorrências permaneceu acima do valor registrado em 2023.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pergunta 2 - Quais estados registraram as maiores quantidades de sinistros no período analisado?
# MAGIC

# COMMAND ----------

# Pergunta 2
# Quais estados registraram as maiores quantidades de sinistros?

resultado_uf = spark.sql("""
    SELECT
        uf,
        quantidade_sinistros,
        total_mortos,
        total_feridos
    FROM workspace.default.gold_sinistros_uf
    ORDER BY quantidade_sinistros DESC
    LIMIT 10
""")

display(resultado_uf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resposta - Pergunta 2
# MAGIC
# MAGIC No período analisado, Minas Gerais apresentou a maior quantidade de sinistros, com 27.873 ocorrências. Em seguida aparecem Santa Catarina, com 24.366, e Paraná, com 22.316 ocorrências.
# MAGIC
# MAGIC Os resultados mostram uma concentração maior de sinistros nesses três estados quando comparados aos demais estados presentes entre os dez maiores registros.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pergunta 3 - Quais foram as principais causas dos sinistros registrados?

# COMMAND ----------

# Pergunta 3 - Principais causas dos sinistros

resultado_causas = spark.sql("""
    SELECT
        causa_acidente,
        quantidade_sinistros
    FROM workspace.default.gold_causas
    ORDER BY quantidade_sinistros DESC
    LIMIT 10
""")

display(resultado_causas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resposta - Pergunta 3
# MAGIC
# MAGIC As principais causas de sinistros no período analisado foram reação tardia ou ineficiente do condutor, com 31.574 ocorrências, e ausência de reação do condutor, com 31.459 ocorrências. Em seguida, aparece a causa relacionada ao acesso à via sem observar a presença dos outros veículos, com 20.380 registros.
# MAGIC
# MAGIC Os resultados indicam que fatores relacionados ao comportamento e à atenção dos condutores aparecem entre as causas mais frequentes dos sinistros registrados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pergunta 4 - Em quais dias da semana ocorreu a maior quantidade de sinistros?

# COMMAND ----------

# Pergunta 4 - Quantidade de sinistros por dia da semana

resultado_dia_semana = spark.sql("""
    SELECT
        dia_semana,
        quantidade_sinistros
    FROM workspace.default.gold_dia_semana
    ORDER BY quantidade_sinistros DESC
""")

display(resultado_dia_semana)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resposta - Pergunta 4
# MAGIC
# MAGIC O domingo apresentou a maior quantidade de sinistros no período analisado, com 34.476 ocorrências. Em seguida aparecem o sábado, com 34.298, e a sexta-feira, com 32.882 ocorrências.
# MAGIC
# MAGIC Os resultados mostram que os maiores volumes de sinistros ocorreram entre sexta-feira e domingo, com destaque para o final de semana.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pergunta 5 - Quais tipos de sinistros apresentaram maior quantidade de mortos e feridos?

# COMMAND ----------

# Pergunta 5 - Tipos de sinistros com maior quantidade de mortos e feridos

resultado_tipo_vitimas = spark.sql("""
    SELECT
        tipo_acidente,
        quantidade_sinistros,
        total_mortos,
        total_feridos
    FROM workspace.default.gold_tipo_vitimas
    ORDER BY total_mortos DESC
    LIMIT 10
""")

display(resultado_tipo_vitimas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resposta - Pergunta 5
# MAGIC
# MAGIC Os resultados mostram diferenças importantes entre os tipos de sinistros. A colisão frontal apresentou a maior quantidade de mortes, com 5.655 registros, enquanto a colisão traseira apresentou a maior quantidade de feridos, com 47.349 registros. Dessa forma, observa-se que os tipos de sinistros com maior número de ocorrências não são necessariamente os mesmos que apresentam maior quantidade de mortes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão
# MAGIC
# MAGIC Neste MVP foi desenvolvido um pipeline de dados utilizando informações públicas de sinistros em rodovias federais disponibilizadas pela Polícia Rodoviária Federal (PRF), considerando os anos de 2023, 2024 e 2025.
# MAGIC
# MAGIC Os dados foram armazenados e processados no Databricks, passando pelas etapas Bronze, Silver e Gold. Durante o processo foram realizadas etapas de carregamento, tratamento, verificação da qualidade dos dados e criação de tabelas agregadas para análise.
# MAGIC
# MAGIC A análise permitiu observar a evolução dos sinistros ao longo dos anos, os estados com maior quantidade de ocorrências, as principais causas registradas, a distribuição dos sinistros pelos dias da semana e os tipos de sinistros com maior quantidade de mortos e feridos.
# MAGIC
# MAGIC O desenvolvimento do projeto permitiu aplicar na prática conceitos de engenharia de dados, principalmente relacionados à construção de pipelines, organização dos dados em camadas e preparação dos dados para análise.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Autoavaliação
# MAGIC
# MAGIC O desenvolvimento deste MVP foi importante para colocar em prática os conceitos estudados na disciplina de Engenharia de Dados.
# MAGIC
# MAGIC A principal dificuldade encontrada foi organizar as diferentes etapas do pipeline e realizar o tratamento dos dados para que pudessem ser utilizados nas análises. Também foi necessário verificar a qualidade dos dados e trabalhar com algumas limitações presentes na base original, como problemas de codificação de caracteres em alguns campos de texto.
# MAGIC
# MAGIC Como resultado, foi possível construir um pipeline completo no Databricks, desde o armazenamento dos arquivos originais até a criação das camadas Bronze, Silver e Gold e a realização das análises finais.
# MAGIC
# MAGIC Como possibilidade de evolução do projeto, poderiam ser incluídos novos anos da base da PRF, novas análises e outros indicadores relacionados aos sinistros nas rodovias federais.