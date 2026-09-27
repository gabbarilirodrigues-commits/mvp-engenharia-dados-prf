# MVP - Engenharia de Dados

## Pipeline de Dados para Análise de Sinistros em Rodovias Federais Brasileiras

## 1. Contexto e objetivo

Este projeto foi desenvolvido como MVP da Sprint de Engenharia de Dados da Pós-Graduação em Ciência de Dados e Analytics da PUC-Rio.

O objetivo do trabalho é construir um pipeline de dados em ambiente de nuvem utilizando dados públicos de sinistros em rodovias federais disponibilizados pela Polícia Rodoviária Federal (PRF), considerando os anos de 2023, 2024 e 2025.

Os dados foram carregados, tratados e organizados no Databricks seguindo a arquitetura de camadas Bronze, Silver e Gold.

### Perguntas de negócio

1. Como evoluiu a quantidade de sinistros nas rodovias federais entre 2023 e 2025?
2. Quais estados registraram as maiores quantidades de sinistros no período analisado?
3. Quais foram as principais causas dos sinistros registrados?
4. Em quais dias da semana ocorreu a maior quantidade de sinistros?
5. Quais tipos de sinistros apresentaram maior quantidade de mortos e feridos?

## 2. Fonte e coleta dos dados

Os dados utilizados foram obtidos no Portal de Dados Abertos da Polícia Rodoviária Federal (PRF).

Foram utilizados os arquivos de acidentes agrupados por ocorrência referentes aos anos de:

- 2023
- 2024
- 2025

Os arquivos foram disponibilizados em formato CSV e carregados no Databricks utilizando ponto e vírgula (`;`) como delimitador. 

**Licença e uso dos dados:** Os dados utilizados são disponibilizados publicamente pela Polícia Rodoviária Federal (PRF) em seu Portal de Dados Abertos, para acesso e reutilização das informações públicas.

Fonte: [Portal de Dados Abertos da Polícia Rodoviária Federal (PRF)](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf)

## 3. Arquitetura e modelagem dos dados

O pipeline foi organizado seguindo a arquitetura de camadas Bronze, Silver e Gold.

### Camada Bronze

A camada Bronze contém os dados próximos à sua estrutura original. Foram utilizadas três tabelas:

- `bronze_prf_2023`
- `bronze_prf_2024`
- `bronze_prf_2025`

### Camada Silver

Na camada Silver foi realizada a consolidação e padronização dos dados.

As principais transformações foram:

- união das bases de 2023, 2024 e 2025;
- padronização da coluna `id` para o tipo `long`;
- padronização das colunas `latitude` e `longitude` para o tipo `double`;
- criação da coluna `ano` a partir da data da ocorrência;
- verificação da qualidade dos dados.

A tabela resultante foi armazenada como:

`silver_prf`

A base consolidada possui **213.451 registros**:

- 2023: 67.766 registros
- 2024: 73.156 registros
- 2025: 72.529 registros

### Camada Gold

A camada Gold foi criada a partir da tabela Silver e contém dados agregados para facilitar as análises.

Foram criadas as seguintes tabelas:

- `gold_sinistros_ano` - resumo dos sinistros por ano;
- `gold_sinistros_uf` - resumo dos sinistros por estado;
- `gold_causas` - quantidade de sinistros por causa;
- `gold_dia_semana` - quantidade de sinistros por dia da semana;
- `gold_tipo_vitimas` - quantidade de sinistros, mortos e feridos por tipo de acidente.

### Fluxo do pipeline

PRF (arquivos CSV) → Databricks → Bronze → Silver → Gold → Análises

## 4. Catálogo de dados

Alguns dos principais campos utilizados no projeto foram:

| Campo | Descrição |
|---|---|
| `id` | Identificador da ocorrência |
| `data_inversa` | Data da ocorrência |
| `ano` | Ano da ocorrência |
| `dia_semana` | Dia da semana |
| `uf` | Unidade Federativa |
| `municipio` | Município da ocorrência |
| `causa_acidente` | Causa registrada para o sinistro |
| `tipo_acidente` | Tipo do sinistro |
| `mortos` | Quantidade de mortos |
| `feridos_leves` | Quantidade de feridos leves |
| `feridos_graves` | Quantidade de feridos graves |
| `feridos` | Quantidade total de feridos |
| `veiculos` | Quantidade de veículos envolvidos |
| `latitude` | Latitude da ocorrência |
| `longitude` | Longitude da ocorrência |

## 5. Qualidade dos dados

Durante a preparação da camada Silver foram realizadas verificações de qualidade dos dados.

Foram analisados:

- valores nulos;
- IDs duplicados;
- valores negativos nas principais colunas numéricas;
- diferenças de tipos de dados entre os arquivos dos diferentes anos.

Nas verificações realizadas não foram encontrados IDs duplicados, valores nulos ou valores negativos nas principais colunas numéricas analisadas.

Também foram identificadas diferenças nos tipos das colunas de latitude e longitude entre os anos, que foram corrigidas durante a padronização da camada Silver.

Foi observada ainda uma limitação relacionada à codificação de caracteres em alguns campos de texto da base original.

## 6. Tecnologias utilizadas

- Databricks Free Edition
- Apache Spark / PySpark
- Spark SQL
- Delta Tables
- Python
- GitHub

## 7. Análise dos dados

### Pergunta 1 - Evolução dos sinistros

Entre 2023 e 2024 houve aumento na quantidade de sinistros, passando de 67.766 para 73.156 ocorrências. Em 2025 foram registrados 72.529 sinistros, apresentando uma pequena redução em relação a 2024.

### Pergunta 2 - Estados com maior quantidade de sinistros

Minas Gerais apresentou a maior quantidade de sinistros no período analisado, com 27.873 ocorrências. Em seguida aparecem Santa Catarina, com 24.366, e Paraná, com 22.316 ocorrências.

### Pergunta 3 - Principais causas

As principais causas registradas foram reação tardia ou ineficiente do condutor, com 31.574 ocorrências, e ausência de reação do condutor, com 31.459 ocorrências.

### Pergunta 4 - Dias da semana

O domingo apresentou a maior quantidade de sinistros, com 34.476 ocorrências, seguido pelo sábado, com 34.298, e pela sexta-feira, com 32.882 ocorrências.

### Pergunta 5 - Mortos e feridos por tipo de sinistro

A colisão frontal apresentou a maior quantidade de mortes, com 5.655 registros, enquanto a colisão traseira apresentou a maior quantidade de feridos, com 47.349 registros.

## 8. Conclusão

O desenvolvimento do projeto permitiu construir um pipeline de dados utilizando informações públicas da PRF, passando pelas etapas de armazenamento, tratamento, verificação da qualidade e criação de tabelas agregadas.

A análise permitiu observar a evolução dos sinistros ao longo dos anos, os estados com maior quantidade de ocorrências, as principais causas, a distribuição pelos dias da semana e os tipos de sinistros com maior quantidade de mortos e feridos.

## 9. Autoavaliação

O desenvolvimento deste MVP foi importante para colocar em prática os conceitos estudados na disciplina de Engenharia de Dados.

## 10. Evidências do projeto

Foram incluídas neste repositório evidências das principais etapas desenvolvidas no Databricks, incluindo:

- armazenamento dos dados no ambiente de nuvem;
- criação das tabelas da camada Bronze;
- consolidação e padronização da camada Silver;
- verificações de qualidade dos dados;
- criação das tabelas da camada Gold;
- visualizações utilizadas para responder às perguntas de negócio.

Os arquivos de evidência estão disponíveis neste repositório em formato PNG, numerados de 01 a 18.

A principal dificuldade encontrada foi organizar as diferentes etapas do pipeline e realizar o tratamento dos dados para utilização nas análises. Também foi necessário trabalhar com diferenças nos tipos de dados entre os arquivos e com limitações de codificação de caracteres presentes em alguns campos de texto.

Como possibilidade de evolução do projeto, poderiam ser incluídos novos anos da base da PRF, novas análises e outros indicadores relacionados aos sinistros nas rodovias federais.
