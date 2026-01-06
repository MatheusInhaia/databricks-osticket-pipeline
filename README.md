# OSTicket Data Pipeline – Databricks


Este projeto implementa um pipeline de dados, utilizando o Databricks Free Edition,
seguindo a arquitetura Bronze, Silver e Gold.

## 🎯 Objetivo

Implementar um pipeline de engenharia de dados utilizando dados dos
chamados do TechOps, garantindo o processamento e métricas prontas para consumo analítico.

#### 🔐 Observação sobre os Dados
_Os dados utilizados neste projeto são reais, porém para as visualizações foram anonimizados._

## 🏗️ Arquitetura de Dados
O pipeline segue o padrão:

Raw → Bronze → Silver → Gold

- **Raw**: Armazenamento dos dados brutos (rastreamento e reprocessamento)
- **Bronze**: ingestão dos dados brutos
- **Silver**: tratamento, limpeza e transformação
- **Gold**: agregações e métricas para consumo analítico

A execução é controlada por um notebook orquestrador, que garante a ordem correta entre as camadas.


## 🧰 Tecnologias Utilizadas
- Databricks Free Edition
- Apache Spark (PySpark / Spark SQL)
- Delta Lake
- GitHub (versionamento)

## 📁 Estrutura do Projeto

```
|── pipelines/
    ├── bronze/
    ├── silver/
    ├── gold/
```

## ▶️ Como Executar o Pipeline
1. Acesse o Databricks Free Edition
2. Abra o notebook setup e o execute na opção **Run ALL**
3. Depois certifique-se de estar com os dados brutos na pasta Raw*
4. Execute o notebook utilizando a opção **Run All**

_*os dados usado do OSTicket neste projeto são: ost_thread_entry.csv, ost_ticket.csv, ost_staff.csv, ost_ticket_cdata.csv, ost_ticket_status.csv_

O pipeline será executado na ordem:
Bronze → Silver → Gold

## 📊 Camada Gold e Métricas Analíticas

As métricas da camada Gold foram definidas com foco em análise operacional e acompanhamento de SLA.

➡️ [Clique aqui para ver o detalhamento das métricas e dashboards no Power BI](docs/gold_metrics.md)



## 👤 Autor
Matheus Inhaia

